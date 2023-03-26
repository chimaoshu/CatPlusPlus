#!/bin/bash

url="http://127.0.0.1:8080/api"
num_requests=1000
concurrency=100

# 发送请求并记录时间
time for ((i=1;i<=$num_requests;i++)); do
  curl -s -o /dev/null -w "%{http_code}\n" -X GET "$url" &
  if (( $i % $concurrency == 0 )); then
    wait
  fi
done
wait
