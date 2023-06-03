cmake_minimum_required(VERSION 3.20)
project(CatPlusPlus CXX)

set(CMAKE_C_COMPILER gcc-10)
set(CMAKE_CXX_COMPILER g++-10)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src)
link_directories(src)

# compile options
if (CMAKE_BUILD_TYPE AND (CMAKE_BUILD_TYPE STREQUAL "Release"))
    set(COMMON_COMPILE_OPTIONS -fcoroutines -O3 -DPRODUCTION)
else()
    set(COMMON_COMPILE_OPTIONS -fcoroutines -O0 -g)
endif()

# common link lib
set(COMMON_LIBRARIES uring pthread)

# log_test
add_executable(log_test src/log.cpp test/log_test.cpp)
target_link_libraries(log_test ${COMMON_LIBRARIES})

# echo_server
add_executable(echo_server src/log.cpp src/worker.cpp src/http.cpp src/awaitable.cpp src/io_uring_wrapper.cpp examples/http_echo_server.cpp)
target_link_libraries(echo_server ${COMMON_LIBRARIES})
target_compile_options(echo_server PRIVATE ${COMMON_COMPILE_OPTIONS})

# web_server
add_executable(web_server src/log.cpp src/worker.cpp src/http.cpp src/awaitable.cpp src/io_uring_wrapper.cpp examples/http_web_server.cpp)
target_link_libraries(web_server ${COMMON_LIBRARIES})
target_compile_options(web_server PRIVATE ${COMMON_COMPILE_OPTIONS})

# io_test_server
add_executable(io_test_server src/log.cpp src/worker.cpp src/http.cpp src/awaitable.cpp src/io_uring_wrapper.cpp examples/io_test_server.cpp)
target_link_libraries(io_test_server ${COMMON_LIBRARIES})
target_compile_options(io_test_server PRIVATE ${COMMON_COMPILE_OPTIONS})

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
message("PROJECT_SOURCE_DIR=${PROJECT_SOURCE_DIR}")
