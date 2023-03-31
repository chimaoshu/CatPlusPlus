# CatPlusPlus
CatPlusPlus is a high-performance c++ web framework using io_uring.

See [examples](https://github.com/chimaoshu/CatPlusPlus/tree/main/examples) for a HTTP echo server and a web server.

for Ubuntu users:

+ compiler(as long as it supports c++ coroutine): `apt install gcc-10`

+ boost: `apt install libboost-all-dev`

+ liburing: https://github.com/axboe/liburing

+ cmake >= 3.20: https://github.com/Kitware/CMake/releases

+ Linux Kernel >= 6.1