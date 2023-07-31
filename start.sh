#!/bin/bash
export LD_LIBRARY_PATH=/usr/local/lib
export COIN=Bitcoin
export DB_DIRECTORY=/root/.electrumx/data
export DB_ENGINE=rocksdb
export DAEMON_URL=http://idclub:Bitcoin2088@127.0.0.1:2088
export ELECTRUMX=/root/.electrumx/electrumx_server
export USERNAME=ubuntu
export SERVICES=tcp://0.0.0.0:50001,rpc://0.0.0.0:8000,ws://0.0.0.0:50003
export MAX_SESSIONS=5000
export MAX_SEND=10000000
export LOG_LEVEL=info
export LOG_FORMAT="%(asctime)s-%(name)s-%(threadName)s-%(levelname)s ~%(message)s ~[%(filename)s:%(lineno)d]"
export BANDWIDTH_UNIT_COST=50
export COST_SOFT_LIMIT=0
export COST_HARD_LIMIT=0
export INITIAL_CONCURRENT=10000
export REQUEST_SLEEP=100
export UNSYNC_MEMPOOL=0

nohup python3 electrumx_server > out.log 2>&1 &