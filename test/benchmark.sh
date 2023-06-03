#!/bin/bash
for i in 64B 1K 4K 16K 64K 128K 256K 512K 1M;
do

# run web_server
cd ../bin/
./web_server &
pid=$!
sleep 5
cd -

# run nginx
openresty -s stop
openresty -c `pwd`/benchmark.nginx.conf

# benchmark
echo $i
wrk -t2 -c1000 -d60s http://127.0.0.1:8080/${i}.txt >> catplusplus.benchmark
wrk -t2 -c1000 -d60s http://127.0.0.1:8086/${i}.txt >> nginx.benchmark
sleep 10

# kill
kill -9 $pid
done