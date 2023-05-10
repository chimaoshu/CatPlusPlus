#!/bin/bash

c=1000

# for i in 64B 1K 4K 16K 64K 128K 256K 512K 1M 4M;
#for i in 64 1024 4096 16384 65536 131072 262144 524288 1048576 4194304;
for i in 64 1024 4096 16384 65536 131072 262144 524288 1048576 4194304;
do

# # io_test_server
cd ../bin/
echo "start io_test_server ${i} test"
./io_test_server $i &
cd -
pid=$(ps -ef | grep "./io_test_server ${i}" | grep -v grep | awk '{print $2}')
sleep 5
wrk -t1 -c1000 -d30s http://127.0.0.1:8080/${i} >> 1000c.1worker.cpp.io_test.benchmark
sleep 5
kill $pid
echo "kill io_test_server"

# # nginx with io_test
# echo "start nginx test"
# wrk -t1 -c1000 -d30s http://127.0.0.1:8086/io_test?num=${i} >> 1000c.1worker.nginx.io_test.benchmark
# echo "end nginx test"

# alibaba/photonlibos server_perf
# cd ../bin/
# echo "start server_perf ${i} test"
# ./server_perf -body_size $i -port 8085 &
# cd -
# pid=$(ps -ef | grep "./server_perf" | grep -v grep | awk '{print $2}')
# sleep 10
# wrk -t1 -c1000 -d30s http://127.0.0.1:8085/${i} >> 1000c.1worker.server_perf.io_test.benchmark
# sleep 5
# kill $pid
# echo "kill server_perf"

done
