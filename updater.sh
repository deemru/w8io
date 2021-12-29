#!/bin/bash

cd $(dirname $0)
unlink var/db/scams.txt
unlink var/db/weights.txt
unlink var/db/blockchain.sqlite3-wal
unlink var/db/blockchain.sqlite3-shm
while :; do echo '' && date "+%Y.%m.%d %H:%M:%S" && echo "-------------------"; php w8_updater.php; echo "-------------------"; date "+%Y.%m.%d %H:%M:%S"; sleep 17; done
