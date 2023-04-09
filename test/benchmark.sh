#!/bin/bash

for i in 64B 1K 4K 16K 64K 128K 256K 512K 1M 4M 16M 128M 1G;
do

# run web_server
cd ../bin/
./web_server &
pid=$!
sleep 5
cd -

# benchmark
#wrk -t4 -c1000 -d60s http://127.0.0.1:8080/${i}.txt > ${i}.benchmark
wrk -t4 -c1000 -d60s http://127.0.0.1:8081/${i}.txt > ${i}.benchmark

sleep 20

# kill
kill -9 $pid
done