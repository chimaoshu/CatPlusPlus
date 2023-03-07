#ifndef __LOG_H__
#define __LOG_H__

#include <iostream>
#include <string>
#include <ctime>
#include <sstream>
#include <atomic>
#include <vector>
#include <unordered_map>
#include <mutex>

#include <boost/lockfree/queue.hpp>

#include <string.h>

#include <liburing.h>

#include "config.hpp"

#include "util.hpp"

using LogName = std::string;
using FileDescriptor = int;

class Logger
{
private:
    // 日志文件信息
    struct FileInfo
    {
        int fd;
        int file_index; // 在register file中的下标
        FileInfo() = default;
        FileInfo(int fd, int registered_index) : fd(fd), file_index(registered_index) {}
    };

    // buffer信息
    struct BufferInfo
    {
        iovec iov;
        int buf_index; // 在register buffer中的下标

        BufferInfo() = default;
        BufferInfo(void *ptr, size_t len, int buf_index) : buf_index(buf_index)
        {
            iov.iov_base = ptr;
            iov.iov_len = len;
        }
    };

    // 单次日志的写请求
    struct LogTask
    {
        int bytes_len;
        const FileInfo *file_info;
        const BufferInfo *buffer_info;

        LogTask() = default;
        LogTask(int bytes_len, FileInfo *file_info, BufferInfo *buffer_info)
            : bytes_len(bytes_len), file_info(file_info), buffer_info(buffer_info) {}
    };

private:
    std::string log_dir_;                            // 日志目录
    const int max_log_len = 512;                     // 单则日志最大长度
    std::unordered_map<LogName, FileInfo> log_files; // 从日志名到日志文件的映射

    const int init_buffer_capacity_;                                                    // 初始缓存容量
    const int max_fixed_buffer_capacity_;                                               // buffer数量不够时会动态增长，但是不超过上限
    std::atomic<int> unused_buffer_num_;                                                // 可用缓存数量
    boost::lockfree::queue<BufferInfo *> unused_buffer_{(size_t)init_buffer_capacity_}; // 缓存池中未被使用的buffer
    std::atomic<int> new_buffer_index_{0};                                              // 注册入内核的buffer_index，保证唯一性

    boost::lockfree::queue<LogTask> unsubmitted_tasks_{(size_t)init_buffer_capacity_}; // 未提交的日志任务
    std::atomic<int> unsubmitted_tasks_num_{0};                                        // 未提交日志任务的数量
    std::atomic<int> submitted_unconsumed_task_num_{0};                                // 已提交到io_uring但未收割的任务数量

    struct io_uring ring_;                   // io_uring实例
    std::recursive_mutex io_uring_sq_mutex_; // 锁，保证只有一个线程操作sqe
    std::recursive_mutex io_uring_cq_mutex_; // 锁，保证只有一个线程操作cqe
    const int io_uring_entries_ = 1024;      // io_uring初始化entry数量

    // 格式化为日志格式
    void format_str(const std::string &type, const std::string &msg, std::string &result);

    // 准备提交日志任务，即填充sqe，需后续调用io_uring_submit()一次性提交
    void io_uring_prep_submit_task(const LogTask &task);

    // 遍历cqe，回收buffer
    void for_each_cqe_retrieve_buffer();

    // 从多种不同渠道尝试获取一块buffer，优先等级：
    // 1、已创建buffer池中可用buffer优先（多数情况）
    // 2、新建fixed buffer（多数情况）
    // 3、新建无法注册的buffer（少数情况）
    // 4、阻塞等待cqe回收buffer（极少数情况）
    BufferInfo *get_buffer();

public:
    // @param log_dir 日志文件目录
    // @param logname_list 日志名数组, e.g. std::vector<string>{"debug", "info", "warn", "error"}
    // @param init_fixed_buffer_num fixed buffer初始数量
     // @param max_fixed_buffer_pool_size fixed buffer不足时会扩展，该字段为其数量上限。受制于linux内核io_uring实现\
    fixed buffer数组并不能动态扩展，幸运的是，linux 5.19后支持先开辟指定数量的空fixed buffer，后续再填充，\
    该字段即为空fixed buffer的数量
    // @param io_uring_entries sqe大小，不能少于max_fixed_buffer_pool_size
    Logger(const std::string &log_dir, const std::vector<LogName> &logname_list, int init_fixed_buffer_num,
           int max_fixed_buffer_pool_size, int io_uring_entries);

    ~Logger();

    // 写日志
    void log(const std::string &logname, const std::string &msg);
    void log(const std::string &logname, const std::stringstream &msg);
};

// 全局静态变量写日志
class Log
{
private:
    // meyers singleton mode
    // 局部静态变量只会在第一次被调用时实例化第一次，以后不会再实例化
    // 从而实现单例模式且可以充当全局变量，且保证调用时是已经被初始化的状态
    static Logger &get_logger()
    {
        // 运行时期的日志
        static Logger runtime_logger(
            config::get("log_dir"),
            std::vector<LogName>{"debug", "info", "warn", "error"},
            std::stoi(config::get("init_fixed_buffer_num", "32")),
            std::stoi(config::get("max_fixed_buffer_pool_size", "512")),
            std::stoi(config::get("io_uring_entries", "512")));
        return runtime_logger;
    }

public:
    static void debug(const std::string &msg)
    {
        get_logger().log("debug", msg);
    }

    static void info(const std::string &msg)
    {
        get_logger().log("info", msg);
    }

    static void warn(const std::string &msg)
    {
        get_logger().log("warn", msg);
    }

    static void error(const std::string &msg)
    {
        get_logger().log("error", msg);
    }
};

#endif // __LOG_H__