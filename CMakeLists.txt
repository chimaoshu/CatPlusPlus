CMAKE_MINIMUM_REQUIRED(VERSION 3.20)

PROJECT(CatPlusPlus CXX)

SET(CMAKE_C_COMPILER gcc-10)
SET(CMAKE_CXX_COMPILER g++-10)
SET(CMAKE_CXX_STANDARD 20)
SET(CMAKE_CXX_STANDARD_REQUIRED ON)
SET(CMAKE_BUILD_TYPE Debug)

INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/src)
LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/src)

# log_test
ADD_EXECUTABLE(log_test src/log.cpp test/log_test.cpp)
TARGET_LINK_LIBRARIES(log_test uring pthread)

# http_echo_server
ADD_EXECUTABLE(echo_server src/log.cpp src/worker.cpp src/http.cpp src/awaitable.cpp test/http_echo_server.cpp)
TARGET_COMPILE_OPTIONS(echo_server PRIVATE -fcoroutines)
TARGET_LINK_LIBRARIES(echo_server uring pthread)

# http_web_server
ADD_EXECUTABLE(web_server src/log.cpp src/worker.cpp src/http.cpp src/awaitable.cpp test/http_web_server.cpp)
TARGET_COMPILE_OPTIONS(web_server PRIVATE -fcoroutines)
TARGET_LINK_LIBRARIES(web_server uring pthread)

SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
MESSAGE("PROJECT_SOURCE_DIR=${PROJECT_SOURCE_DIR}")
