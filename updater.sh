#!/bin/bash

cd $(dirname $0)
unlink var/db/scams.txt
unlink var/db/weights.txt
while :; do
unlink var/db/blockchain.sqlite3-wal.last;
unlink var/db/blockchain.sqlite3-shm.last;
mv var/db/blockchain.sqlite3-wal var/db/blockchain.sqlite3-wal.last;
mv var/db/blockchain.sqlite3-shm var/db/blockchain.sqlite3-shm.last;
cp var/db/blockchain.sqlite3-wal.last var/db/blockchain.sqlite3-wal;
cp var/db/blockchain.sqlite3-shm.last var/db/blockchain.sqlite3-shm;
echo '' && date "+%Y.%m.%d %H:%M:%S" && echo "-------------------"; php w8_updater.php; echo "-------------------"; date "+%Y.%m.%d %H:%M:%S"; sleep 17; done
