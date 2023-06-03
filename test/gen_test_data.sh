#!/bin/bash

for i in 64 1K 4K 16K 64K 128K 256K 512K 1M;
do
fallocate -l $i /var/www/html/$i.txt;
done