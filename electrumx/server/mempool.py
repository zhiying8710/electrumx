# Copyright (c) 2016-2018, Neil Booth
#
# All rights reserved.
#
# See the file "LICENCE" for information about the copyright
# and warranty status of this software.

'''Mempool handling.'''
import asyncio
import itertools
import signal
import struct
import time
from abc import ABC, abstractmethod
from asyncio import Lock
from collections import defaultdict
from typing import Sequence, Tuple, TYPE_CHECKING, Type, Dict
import math

import attr
import zmq
import zmq.asyncio
from aiorpcx import run_in_thread, sleep

from electrumx.lib.hash import hash_to_hex_str, hex_str_to_hash
from electrumx.lib.tx import SkipTxDeserialize
from electrumx.lib.util import class_logger, chunks, OldTaskGroup, Queue
from electrumx.server.db import UTXO

if TYPE_CHECKING:
    from electrumx.lib.coins import Coin


@attr.s(slots=True)
class MemPoolTx:
    prevouts = attr.ib()  # type: Sequence[Tuple[bytes, int]]
    # A pair is a (hashX, value) tuple
    in_pairs = attr.ib()
    out_pairs = attr.ib()
    fee = attr.ib()
    size = attr.ib()


@attr.s(slots=True)
class MemPoolTxSummary:
    hash = attr.ib()
    fee = attr.ib()
    has_unconfirmed_inputs = attr.ib()


class DBSyncError(Exception):
    pass


class MemPoolAPI(ABC):
    '''A concrete instance of this class is passed to the MemPool object
    and used by it to query DB and blockchain state.'''

    @abstractmethod
    async def height(self):
        '''Query bitcoind for its height.'''

    @abstractmethod
    def cached_height(self):
        '''Return the height of bitcoind the last time it was queried,
        for any reason, without actually querying it.
        '''

    @abstractmethod
    def db_height(self):
        '''Return the height flushed to the on-disk DB.'''

    @abstractmethod
    async def mempool_hashes(self):
        '''Query bitcoind for the hashes of all transactions in its
        mempool, returned as a list.'''

    @abstractmethod
    async def raw_transactions(self, hex_hashes):
        '''Query bitcoind for the serialized raw transactions with the given
        hashes.  Missing transactions are returned as None.

        hex_hashes is an iterable of hexadecimal hash strings.'''

    @abstractmethod
    async def lookup_utxos(self, prevouts):
        '''Return a list of (hashX, value) pairs each prevout if unspent,
        otherwise return None if spent or not found.

        prevouts - an iterable of (hash, index) pairs
        '''

    @abstractmethod
    async def on_mempool(self, touched, height):
        '''Called each time the mempool is synchronized.  touched is a set of
        hashXs touched since the previous call.  height is the
        daemon's height at the time the mempool was obtained.'''

    @abstractmethod
    async def getblock(self, hex_hash, verbosity=1):
        '''Query bitcoind for the block info'''


class MemPool:
    '''Representation of the daemon's mempool.

        coin - a coin class from coins.py
        api - an object implementing MemPoolAPI

    Updated regularly in caught-up state.  Goal is to enable efficient
    response to the calls in the external interface.  To that end we
    maintain the following maps:

       tx:     tx_hash -> MemPoolTx
       hashXs: hashX   -> set of all hashes of txs touching the hashX
    '''

    def __init__(self, env, api: MemPoolAPI, refresh_secs=5.0, log_status_secs=60.0):
        assert isinstance(api, MemPoolAPI)
        self.env = env
        self.coin = env.coin
        self.api = api
        self.logger = class_logger(__name__, self.__class__.__name__)
        self.txs = {}
        self.hashXs = defaultdict(set)  # None can be a key
        self.cached_compact_histogram = []
        self.refresh_secs = refresh_secs
        self.log_status_secs = log_status_secs
        # Prevents mempool refreshes during fee histogram calculation
        self.lock = Lock()

        # subscribe event
        self.subscribe_mempool_tx_hashXs = defaultdict(set)
        self.subscribe_mempool_txs = {}
        self.subscribe_mempool_hashXs = defaultdict(set)
        self.subscribe_event_queue = Queue()

    async def _logging(self, synchronized_event):
        '''Print regular logs of mempool stats.'''
        self.logger.info('beginning processing of daemon mempool.  '
                         'This can take some time...')
        start = time.monotonic()
        await synchronized_event.wait()
        elapsed = time.monotonic() - start
        self.logger.info(f'synced in {elapsed:.2f}s')
        while True:
            mempool_size = sum(tx.size for tx in self.txs.values()) / 1_000_000
            self.logger.info(f'{len(self.txs):,d} txs {mempool_size:.2f} MB '
                             f'touching {len(self.hashXs):,d} addresses')
            await sleep(self.log_status_secs)
            await synchronized_event.wait()

    async def _refresh_histogram(self, synchronized_event):
        while True:
            await synchronized_event.wait()
            async with self.lock:
                # Threaded as can be expensive
                bin_size = self.coin.MEMPOOL_COMPACT_HISTOGRAM_BINSIZE
                await run_in_thread(self._update_histogram, bin_size)
            await sleep(self.coin.MEMPOOL_HISTOGRAM_REFRESH_SECS)

    def _update_histogram(self, bin_size):
        # Build a histogram by fee rate
        histogram = defaultdict(int)
        for tx in self.txs.values():
            fee_rate = tx.fee / tx.size
            # use 0.1 sat/byte resolution
            # note: rounding *down* is intentional. This ensures txs
            #       with a given fee rate will end up counted in the expected
            #       bucket/interval of the compact histogram.
            fee_rate = math.floor(10 * fee_rate) / 10
            histogram[fee_rate] += tx.size

        compact = self._compress_histogram(histogram, bin_size=bin_size)
        self.logger.info(f'compact fee histogram: {compact}')
        self.cached_compact_histogram = compact

    @classmethod
    def _compress_histogram(
            cls, histogram: Dict[float, int], *, bin_size: int
    ) -> Sequence[Tuple[float, int]]:
        '''Calculate and return a compact fee histogram as needed for
        "mempool.get_fee_histogram" protocol request.

        histogram: feerate (sat/vbyte) -> total size in bytes of txs that pay approx feerate
        bin_size: ~minimum vsize of a bucket of txs in the result (e.g. 100 kb)
        '''
        # Now compact it.  For efficiency, get_fees returns a
        # compact histogram with variable bin size.  The compact
        # histogram is an array of (fee_rate, vsize) values.
        # vsize_n is the cumulative virtual size of mempool
        # transactions with a fee rate in the interval
        # [rate_(n-1), rate_n)], and rate_(n-1) > rate_n.
        # Intervals are chosen to create tranches containing at
        # least a certain cumulative size (bin_size) of transactions.
        assert bin_size > 0
        compact = []
        cum_size = 0
        prev_fee_rate = None
        for fee_rate, size in sorted(histogram.items(), reverse=True):
            # if there is a big lump of txns at this specific size,
            # consider adding the previous item now (if not added already)
            if size > 2 * bin_size and prev_fee_rate is not None and cum_size > 0:
                compact.append((prev_fee_rate, cum_size))
                cum_size = 0
                bin_size *= 1.1
            # now consider adding this item
            cum_size += size
            if cum_size > bin_size:
                compact.append((fee_rate, cum_size))
                cum_size = 0
                bin_size *= 1.1
            prev_fee_rate = fee_rate
        return compact

    def _accept_transactions(self, tx_map, utxo_map, touched):
        '''Accept transactions in tx_map to the mempool if all their inputs
        can be found in the existing mempool or a utxo_map from the
        DB.

        Returns an (unprocessed tx_map, unspent utxo_map) pair.
        '''
        hashXs = self.hashXs
        txs = self.txs

        deferred = {}
        unspent = set(utxo_map)
        # Try to find all prevouts so we can accept the TX
        for hash, tx in tx_map.items():
            in_pairs = []
            try:
                for prevout in tx.prevouts:
                    utxo = utxo_map.get(prevout)
                    if not utxo:
                        prev_hash, prev_index = prevout
                        # Raises KeyError if prev_hash is not in txs
                        utxo = txs[prev_hash].out_pairs[prev_index]
                    in_pairs.append(utxo)
            except KeyError:
                deferred[hash] = tx
                continue

            # Spend the prevouts
            unspent.difference_update(tx.prevouts)

            # Save the in_pairs, compute the fee and accept the TX
            tx.in_pairs = tuple(in_pairs)
            # Avoid negative fees if dealing with generation-like transactions
            # because some in_parts would be missing
            tx.fee = max(0, (sum(v for _, v in tx.in_pairs) -
                             sum(v for _, v in tx.out_pairs)))
            txs[hash] = tx

            for hashX, _value in itertools.chain(tx.in_pairs, tx.out_pairs):
                touched.add(hashX)
                hashXs[hashX].add(hash)

        return deferred, {prevout: utxo_map[prevout] for prevout in unspent}

    async def _refresh_hashes(self, synchronized_event):
        '''Refresh our view of the daemon's mempool.'''
        # Touched accumulates between calls to on_mempool and each
        # call transfers ownership
        touched = set()
        subscribed = False
        while True:
            height = self.api.cached_height()
            hex_hashes = await self.api.mempool_hashes()
            if height != await self.api.height():
                continue
            hashes = {hex_str_to_hash(hh) for hh in hex_hashes}
            try:
                async with self.lock:
                    await self._process_mempool(hashes, touched, height)
            except DBSyncError:
                # The UTXO DB is not at the same height as the
                # mempool; wait and try again
                self.logger.debug('waiting for DB to sync')
            else:
                synchronized_event.set()
                synchronized_event.clear()
                await self.api.on_mempool(touched, height)
                touched = set()
            await sleep(self.refresh_secs)

    async def _process_mempool(self, all_hashes, touched, mempool_height):
        # Re-sync with the new set of hashes
        txs = self.txs
        hashXs = self.hashXs

        if mempool_height != self.api.db_height():
            raise DBSyncError

        # First handle txs that have disappeared
        for tx_hash in (set(txs) - all_hashes):
            tx = txs.pop(tx_hash)
            tx_hashXs = {hashX for hashX, value in tx.in_pairs}
            tx_hashXs.update(hashX for hashX, value in tx.out_pairs)
            for hashX in tx_hashXs:
                hashXs[hashX].remove(tx_hash)
                if not hashXs[hashX]:
                    del hashXs[hashX]
            touched |= tx_hashXs

        # Process new transactions
        new_hashes = list(all_hashes.difference(txs))
        if new_hashes:
            group = OldTaskGroup()
            for hashes in chunks(new_hashes, 200):
                coro = self._fetch_and_accept(hashes, all_hashes, touched)
                await group.spawn(coro)
            if mempool_height != self.api.db_height():
                raise DBSyncError

            tx_map = {}
            utxo_map = {}
            async for task in group:
                deferred, unspent = task.result()
                tx_map.update(deferred)
                utxo_map.update(unspent)

            prior_count = 0
            # FIXME: this is not particularly efficient
            while tx_map and len(tx_map) != prior_count:
                prior_count = len(tx_map)
                tx_map, utxo_map = self._accept_transactions(tx_map, utxo_map,
                                                             touched)
            if tx_map:
                self.logger.error(f'{len(tx_map)} txs dropped')

        return touched

    async def _fetch_and_accept(self, hashes, all_hashes, touched):
        '''Fetch a list of mempool transactions.'''
        hex_hashes_iter = (hash_to_hex_str(hash) for hash in hashes)
        raw_txs = await self.api.raw_transactions(hex_hashes_iter)

        def deserialize_txs():    # This function is pure
            to_hashX = self.coin.hashX_from_script
            deserializer = self.coin.DESERIALIZER

            txs = {}
            for hash, raw_tx in zip(hashes, raw_txs):
                # The daemon may have evicted the tx from its
                # mempool or it may have gotten in a block
                if not raw_tx:
                    continue
                try:
                    tx, tx_size = deserializer(raw_tx).read_tx_and_vsize()
                except SkipTxDeserialize as ex:
                    self.logger.debug(f'skipping tx {hash_to_hex_str(hash)}: {ex}')
                    continue
                # Convert the inputs and outputs into (hashX, value) pairs
                # Drop generation-like inputs from MemPoolTx.prevouts
                txin_pairs = tuple((txin.prev_hash, txin.prev_idx)
                                   for txin in tx.inputs
                                   if not txin.is_generation())
                txout_pairs = tuple((to_hashX(txout.pk_script), txout.value)
                                    for txout in tx.outputs)
                txs[hash] = MemPoolTx(txin_pairs, None, txout_pairs,
                                      0, tx_size)
            return txs

        # Thread this potentially slow operation so as not to block
        tx_map = await run_in_thread(deserialize_txs)

        # Determine all prevouts not in the mempool, and fetch the
        # UTXO information from the database.  Failed prevout lookups
        # return None - concurrent database updates happen - which is
        # relied upon by _accept_transactions. Ignore prevouts that are
        # generation-like.
        prevouts = tuple(prevout for tx in tx_map.values()
                         for prevout in tx.prevouts
                         if prevout[0] not in all_hashes)
        utxos = await self.api.lookup_utxos(prevouts)
        utxo_map = {prevout: utxo for prevout, utxo in zip(prevouts, utxos)}

        return self._accept_transactions(tx_map, utxo_map, touched)

    async def _subscribe_sequence_event(self):
        if not self.env.zmq_pub_address:
            return
        zmqContext = zmq.asyncio.Context()

        zmqSubSocket = zmqContext.socket(zmq.SUB)
        zmqSubSocket.setsockopt(zmq.RCVHWM, 0)
        zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "hashblock")
        zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "hashtx")
        zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "rawblock")
        zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "rawtx")
        zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "sequence")
        zmqSubSocket.connect(self.env.zmq_pub_address)
        self.logger.info(f"Connected to zmq {self.env.zmq_pub_address}")
        mempool_hashs = None

        def stop():
            zmqContext.destroy()

        def deserialize_tx(txid, raw_tx):    # This function is pure
            if not raw_tx:
                return
            try:
                tx, tx_size = self.coin.DESERIALIZER(raw_tx).read_tx_and_vsize()
            except SkipTxDeserialize as ex:
                self.logger.debug(f'skipping tx {txid}: {ex}')
                return

            txin_pairs = tuple((txin.prev_hash, txin.prev_idx)
                               for txin in tx.inputs
                               if not txin.is_generation())
            txout_pairs = tuple((self.coin.hashX_from_script(txout.pk_script), txout.value)
                                for txout in tx.outputs)
            return MemPoolTx(txin_pairs, None, txout_pairs, 0, tx_size)

        async def handle():
            topic, body, seq = await zmqSubSocket.recv_multipart()
            nonlocal mempool_hashs
            if not mempool_hashs:
                mempool_hashs = {hex_str_to_hash(hh) for hh in await self.api.mempool_hashes()}
            if topic == b"sequence":
                _hash = body[:32].hex()
                label = chr(body[32])
                event = (_hash, label)
                if label == 'A' or label == 'R' or label == 'C':
                    self.logger.info(f"Queue event {event}")
                    self.subscribe_event_queue.put_nowait(event)
            # schedule ourselves to receive the next message
            asyncio.ensure_future(handle())

        def drop_txs(tx_hex_hashs):
            for _hash in tx_hex_hashs:
                hh = hex_str_to_hash(_hash)
                if hh in mempool_hashs:
                    mempool_hashs.remove(hh)
                for hashX in self.subscribe_mempool_tx_hashXs.get(hh, []):
                    hhs = self.subscribe_mempool_hashXs.get(hashX)
                    if hhs and hh in hhs:
                        hhs.remove(hh)
                if hh in self.subscribe_mempool_tx_hashXs:
                    self.subscribe_mempool_tx_hashXs.pop(hh)
                if hh in self.subscribe_mempool_txs:
                    self.subscribe_mempool_txs.pop(hh)
                self.logger.info(f"Drop tx {_hash}")

        async def treat_event():
            async for _hash, label in self.subscribe_event_queue:
                try:
                    if label == "A":
                        hh = hex_str_to_hash(_hash)
                        mempool_hashs.add(hh)
                        _txs = await self.api.raw_transactions([_hash])
                        tx = await asyncio.to_thread(deserialize_tx, _hash, _txs[0])
                        if not tx:
                            continue
                        prevouts = tuple(prevout for prevout in tx.prevouts if prevout[0] not in mempool_hashs) # 过滤掉内存池的utxo
                        utxos = await self.api.lookup_utxos(prevouts)
                        utxo_map = {prevout: utxo for prevout, utxo in zip(prevouts, utxos)}

                        in_pairs = []
                        for prevout in tx.prevouts:
                            utxo = utxo_map.get(prevout)
                            if not utxo:
                                prev_hash, prev_index = prevout
                                # Raises KeyError if prev_hash is not in txs
                                try:
                                    utxo = self.subscribe_mempool_txs[prev_hash].out_pairs[prev_index]
                                except KeyError:
                                    try:
                                        utxo = self.txs[prev_hash].out_pairs[prev_index]
                                    except KeyError:
                                        continue
                            in_pairs.append(utxo)
                        if not in_pairs:
                            continue

                        tx.in_pairs = tuple(in_pairs)

                        tx.fee = max(0, (sum(v for _, v in tx.in_pairs) -
                                         sum(v for _, v in tx.out_pairs)))

                        self.subscribe_mempool_txs[hh] = tx
                        self.subscribe_mempool_tx_hashXs[hh] = set()
                        for hashX, _value in itertools.chain(tx.in_pairs, tx.out_pairs):
                            self.subscribe_mempool_hashXs[hashX].add(hh)
                            self.subscribe_mempool_tx_hashXs[hh].add(hashX)
                        self.logger.info(f"Add new tx {_hash}")
                    elif label == "R":
                        drop_txs([_hash])
                    elif label == "C":
                        block = await self.api.getblock(_hash)
                        while block is None:
                            await sleep(.1)
                        drop_txs(block['tx'])
                        self.logger.info(f"Add new block {_hash}")
                except Exception as e:
                    self.logger.error("Treat tx from zmq error", exc_info=e)
                finally:
                    self.subscribe_event_queue.task_done()

        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, stop)
        loop.create_task(handle())
        loop.create_task(treat_event())

    #
    # External interface
    #

    async def keep_synchronized(self, synchronized_event):
        if self.env.unsync_mempool:
            synchronized_event.set()
            synchronized_event.clear()
            # await self._subscribe_sequence_event()
            return
        '''Keep the mempool synchronized with the daemon.'''
        async with OldTaskGroup() as group:
            await group.spawn(self._refresh_hashes(synchronized_event))
            await group.spawn(self._refresh_histogram(synchronized_event))
            await group.spawn(self._logging(synchronized_event))

    async def balance_delta(self, hashX):
        '''Return the unconfirmed amount in the mempool for hashX.

        Can be positive or negative.
        '''
        _hashXs_cache = self.hashXs.copy()
        _txs_cache = self.txs.copy()
        value = 0
        if hashX in _hashXs_cache:
            for hash in _hashXs_cache[hashX]:
                tx = _txs_cache[hash]
                value -= sum(v for h168, v in tx.in_pairs if h168 == hashX)
                value += sum(v for h168, v in tx.out_pairs if h168 == hashX)
        return value

    async def compact_fee_histogram(self):
        '''Return a compact fee histogram of the current mempool.'''
        return self.cached_compact_histogram

    async def potential_spends(self, hashX):
        '''Return a set of (prev_hash, prev_idx) pairs from mempool
        transactions that touch hashX.

        None, some or all of these may be spends of the hashX, but all
        actual spends of it (in the DB or mempool) will be included.
        '''
        _hashXs_cache = self.hashXs.copy()
        _txs_cache = self.txs.copy()
        result = set()
        for tx_hash in _hashXs_cache.get(hashX, ()):
            tx = _txs_cache[tx_hash]
            result.update(tx.prevouts)
        return result

    async def transaction_summaries(self, hashX):
        '''Return a list of MemPoolTxSummary objects for the hashX.'''
        _hashXs_cache = self.hashXs.copy()
        _txs_cache = self.txs.copy()
        result = []
        for tx_hash in _hashXs_cache.get(hashX, ()):
            tx = _txs_cache[tx_hash]
            has_ui = any(hash in _txs_cache for hash, idx in tx.prevouts)
            result.append(MemPoolTxSummary(tx_hash, tx.fee, has_ui))
        return result

    async def unordered_UTXOs(self, hashX):
        '''Return an unordered list of UTXO named tuples from mempool
        transactions that pay to hashX.

        This does not consider if any other mempool transactions spend
        the outputs.
        '''
        _hashXs_cache = self.hashXs.copy()
        _txs_cache = self.txs.copy()
        utxos = []
        for tx_hash in _hashXs_cache.get(hashX, ()):
            tx = _txs_cache.get(tx_hash)
            for pos, (hX, value) in enumerate(tx.out_pairs):
                if hX == hashX:
                    utxos.append(UTXO(-1, pos, tx_hash, 0, value))
        return utxos

    async def unordered_UTXOs2(self, hashX):
        utxos = []
        for tx_hash in self.subscribe_mempool_hashXs.get(hashX, ()):
            tx = self.subscribe_mempool_txs.get(tx_hash)
            for pos, (hX, value) in enumerate(tx.out_pairs):
                if hX == hashX:
                    utxos.append(UTXO(-1, pos, tx_hash, 0, value))
        return utxos

    async def potential_spends2(self, hashX):
        result = set()
        for tx_hash in self.subscribe_mempool_hashXs.get(hashX, ()):
            tx = self.subscribe_mempool_txs[tx_hash]
            result.update(tx.prevouts)
        return result


