# CatPlusPlus
CatPlusPlus is a high-performance c++ web framework using io_uring.

See [examples](https://github.com/chimaoshu/CatPlusPlus/tree/main/examples) for a HTTP echo server and a web server.

for Ubuntu users:

+ compiler(as long as it supports c++ coroutine): `apt install gcc-10`

+ boost: `apt install libboost-all-dev`

+ liburing: https://github.com/axboe/liburing

+ cmake >= 3.20: https://github.com/Kitware/CMake/releases

+ Linux Kernel >= 6.1

```config
#==================
# server conf
#==================

# ip address of server
SERVER_ADDRESS = 127.0.0.1

# listen port of server, please ensure that 1023 < port < 65535
LISTEN_PORT = 8080

# number of workers running in thread
WORKER_NUM = 1

# enable TCP_NODELAY, 0 or 1
TCP_NODELAY = 1

# use sendfile file size threshold (bytes)
# When the static file size >= the threshold, the web server
# will use `sendfile` instead of `read && zero-copy-write`.
# If SENDFILE_THRESHOLD is set to -1 then splice will not be used.
SENDFILE_THRESHOLD = 0

# ==================
# worker conf
# ==================

# max connection per worker
WORKER_MAX_CONN_NUM = 16384

# max read/write buffer num per worker
WORKER_MAX_BUF_NUM = 4096

# init buffer num per worker, must be <= WORKER_MAX_BUF_NUM
WORKER_INIT_BUF_NUM = 1024 

# enable the work stealing algorithm, 0 or 1
ENABLE_WORK_STEALING = 0

# capacity of work-stealing-queue(wsq) per worker. If the wsq reached
# the upper limit, then those processing works will be push to a
# global wsq with no capacity limit, in which those works can be
# got by all workers to process.
WORKER_WSQ_CAPACITY = 128

# io_uring sqe slot num
# a simple splice send will use a lot of sqe
# must be <= 2^15(32768)
WORKER_IO_URING_ENTRIES = 32768

# For web static file that are larger than SENDFILE_THRESHOLD,
# workers use splice with pipe to implement `sendfile` on io_uring
# and this field will be set as the size of that pipe.
# system's max pipe size can be found at /proc/sys/fs/pipe-max-size
# must be <= /proc/sys/fs/pipe-max-size
SENDFILE_CHUNK_SIZE = 1048576

# ==================
# log conf
# ==================

# directory of default logger
LOG_DIR = ./logs/

# initial num of fixed buffer, which can be registered to linux kernel
LOGGER_INIT_FIXED_BUF_NUM = 512

# max num of fixed buffer, due to internal design of io_uring, the num
# of fixed buffer must have a upper limit.
LOGGER_MAX_FIXED_BUF_NUM = 1024

# If there are no enough buffer, logger will extend fixed buffer first.
# When the fixed buf has reached the upper limit, logger will extend some
# buffer that are not registered into linux kernel, and this field is
# the max number of buffer created by logger. 
# it must >= LOGGER_MAX_FIXED_BUF_NUM and <= UIO_MAXIOV(1024)
# also must be the power of 2, since we use it as the num of io_uring_entries.
LOGGER_MAX_BUF_NUM = 1024

# clean history log while starting, 0 or 1
CLEAN_LOG_ON_START = 1
```
