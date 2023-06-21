#!/bin/bash

cd $(dirname $0)
while :; do
echo '' && date "+%Y.%m.%d %H:%M:%S" && echo "-------------------"; php w8_updater_headers.php; echo "-------------------"; date "+%Y.%m.%d %H:%M:%S"; sleep 17; done
