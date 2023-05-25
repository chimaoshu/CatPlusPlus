#ifndef __WORKER_H__
#define __WORKER_H__

#include <arpa/inet.h>
#include <liburing.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <map>
#include <queue>

// #include <boost/beast/http.hpp>
#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/parser.hpp>
#include <boost/beast/http/serializer.hpp>
#include <boost/beast/http/string_body.hpp>

#include <boost/lockfree/queue.hpp>
#include <coroutine>
#include <list>
#include <thread>
#include <variant>
#include <vector>

#include "log.h"
#include "util.hpp"
#include "io_uring_wrapper.h"

using namespace boost::beast;

// cqe的柔性数组在c++20协程中会出问题
// 因此定义一个没有柔性数组的cqe，在协程中使用
struct coroutine_cqe
{
  __u64 user_data; /* sqe->data submission passed back */
  __s32 res;       /* result code for this event */
  __u32 flags;
};

struct cqe_status
{
  coroutine_cqe cqe;
  bool has_recv = false;
};

class Worker;
struct ConnectionTask
{
public:
  struct promise_type
  {
  public:
    // 返回值
    int ret;
    bool is_quit = false;

    // 负责当前io的worker，在accept后设置，为了使用fixed
    // file特性，协程的recv与send均由该worker完成
    Worker *io_worker = NULL;
    // 负责process任务的worker，可能与io_worker不同
    Worker *process_worker = NULL;

    // 当前需要处理的IO任务的cqe，拷贝一份
    struct coroutine_cqe cqe;

    // 当前正在进行的IO
    IOType current_io = IOType::NONE;

    // 进行SEND_FILE操作时，记录多个SQE的完成与否，全部完成后才可以恢复协程
    std::vector<cqe_status> sendfile_cqes;
    std::vector<cqe_status> multiple_send_cqes;
    // sendmsg使用到的iovec
    struct iovec *sendmsg_iov = NULL;

  public:
    // 协程在开始执行之前会调用该方法，返回协程句柄
    auto get_return_object()
    {
      return ConnectionTask{
          std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    // co_return返回值设置
    void return_value(int ret)
    {
      this->ret = ret;
      is_quit = true;
    }
    // 协程初始化时挂起
    auto initial_suspend() noexcept { return std::suspend_always{}; }
    // destroy时立即销毁协程
    auto final_suspend() noexcept { return std::suspend_never{}; }
    // 处理异常
    void unhandled_exception() { std::terminate(); }
  };

  std::coroutine_handle<promise_type> handler; // 协程句柄
  ConnectionTask(std::coroutine_handle<promise_type> handler)
      : handler(handler) {}
};

using ConnectionTaskHandler =
    typename std::coroutine_handle<typename ConnectionTask::promise_type>;

// process函数的参数
struct process_func_args
{
  // 以web server方式处理，以及响应的文件路径
  bool use_web_server = false;
  std::string file_path;
};

using ProcessFuncStringBody =
    std::function<int(http::request<http::string_body> &, http::response<http::string_body> &,
                      process_func_args &args)>;

using ProcessFuncBufferBody =
    std::function<int(http::request<http::string_body> &, http::response<http::buffer_body> &,
                      process_func_args &args)>;

using ProcessFuncType = std::variant<ProcessFuncStringBody, ProcessFuncBufferBody>;

// 拷贝cqe
void copy_cqe(struct coroutine_cqe &dest, struct io_uring_cqe &src);

class Service
{
private:
  const int worker_num;
  std::vector<Worker *> workers;
  std::vector<std::thread> threads;
  // 全局process任务队列
  boost::lockfree::queue<ConnectionTaskHandler> global_queue;
  friend Worker;

public:
  Service(ProcessFuncType http_handler);
  ~Service();

  void start();
};

class Worker
{
private:
  const int self_worker_id_; // 工作线程唯一标识符
  // 开启的连接，key为direct socket的index
  std::map<int, ConnectionTask> connections_;

  int listen_fd_;
  bool is_accepting_ = false;

  // 包含global queue与其他worker信息
  Service *service_;

  // work stealing任务队列，结构无锁，可被其他worker访问
  // 只装process任务
  boost::lockfree::queue<ConnectionTaskHandler, boost::lockfree::fixed_sized<true>> ws_process_task_queue;

  // 本地私有队列，只有本worker访问，串行无锁，用于read与write请求的提交
  // 只装io任务
  boost::lockfree::queue<int> private_io_task_queue;

  // http处理器
  ProcessFuncType processor_;

  // io_uring实例
  IOUringWrapper io_uring_instance;

  // 各awaitable对象的友元声明
  friend struct socket_recv_awaitable;
  friend struct socket_send_awaitable;
  friend struct socket_close_awaitable;
  friend struct add_process_task_to_wsq_awaitable;
  friend struct add_io_task_back_to_io_worker_awaitable;
  friend struct file_read_awaitable;
  friend struct file_open_awaitable;
  friend struct file_close_awaitable;
  friend struct file_send_awaitable;
  friend struct serialize_awaitable;

public:
  bool is_accpeting() { return is_accepting_; };
  // 提交send_zc请求
  int get_worker_id();
  // 添加process任务至work-stealing-queue
  void add_process_task(ConnectionTaskHandler h);
  // 添加IO恢复任务至private-io-queue
  void add_io_resume_task(int sock_fd_idx);
  // 获取io resume task
  bool try_get_io_task_queue(ConnectionTaskHandler &h);
  IOUringWrapper &get_io_uring() { return io_uring_instance; }

private:
  void handle_accept(struct io_uring_cqe *cqe);
  void start_accept();

public:
  Worker(int worker_id, ProcessFuncType processor, Service *service);

  void run();
};

#endif // __WORKER_H__