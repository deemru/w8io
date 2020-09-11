#!/bin/bash

cd $(dirname $0)
unlink var/db/scams.txt
unlink var/db/weights.txt
while :; do echo '' && date "+%Y.%m.%d %H:%M:%S" && echo "-------------------"; php w8_updater.php; echo "-------------------"; date "+%Y.%m.%d %H:%M:%S"; sleep 1; done
