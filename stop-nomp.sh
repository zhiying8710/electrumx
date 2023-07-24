#!/bin/bash

python3 electrumx_rpc -p 8100 stop

while :
do
        pid=`ps aux | grep -v grep | grep 'electrumx_server' | grep 'nomp' | awk '{print $2}'`
        if [ "$pid"x != ""x ]; then
                echo "electrumx_server is stopping!"
                sleep 3s
        else
                echo "electrumx_server has been stopped!"
                break
        fi
done