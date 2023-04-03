#!/bin/bash

time=10

# 获取web server的进程ID
pid=$(ps -ef | grep './web_server' | grep -v grep | awk '{print $2}')

# 启动perf record并收集数据
perf record -F 99 -p $pid -g -- sleep $time &

# benchmark
wrk -t1 -c400 "-d${time}s" http://127.0.0.1:8080/404.html

# 等待perf record和web server退出
wait

# 生成火焰图
perf script | stackcollapse-perf.pl | flamegraph.pl > perf.svg
