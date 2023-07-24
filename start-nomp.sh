#!/bin/bash

export COIN=Bitcoin
export DB_DIRECTORY=/home/ubuntu/.electrumx/data-nomp
export DAEMON_URL=http://idclub:Bitcoin2088@172.31.26.43:2078/
export ELECTRUMX=/home/ubuntu/.electrumx/electrumx_server
export USERNAME=ubuntu
export SERVICES=tcp://0.0.0.0:51001,rpc://0.0.0.0:8100,ws://0.0.0.0:51003
export MAX_SESSIONS=5000
export MAX_SEND=10000000
export LOG_LEVEL=info
export LOG_FORMAT="%(asctime)s-%(name)s-%(threadName)s-%(levelname)s ~%(message)s ~[%(filename)s:%(lineno)d]"
export BANDWIDTH_UNIT_COST=50
export COST_SOFT_LIMIT=100000
export COST_HARD_LIMIT=100000
export INITIAL_CONCURRENT=10000
export REQUEST_SLEEP=100
export UNSYNC_MEMPOOL=1

nohup python3 electrumx_server nomp > out-nomp.log 2>&1 &