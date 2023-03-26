#ifndef __WORKER_H__
#define __WORKER_H__

#include <arpa/inet.h>
#include <liburing.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <map>
#include <queue>

// #include <boost/beast/http.hpp>
#include <boost/beast/http/parser.hpp>
#include <boost/beast/http/serializer.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/lockfree/queue.hpp>
#include <coroutine>
#include <list>
#include <mutex>
#include <thread>
#include <vector>

#include "log.h"
#include "util.hpp"

using namespace boost::beast;

enum IOType : uint8_t { ACCEPT, RECV, SEND, CLOSE, SHUTDOWN, NONE };

// cqe的柔性数组在c++20协程中会出问题
// 因此定义一个没有柔性数组的cqe，在协程中使用
struct coroutine_cqe {
  __u64 user_data; /* sqe->data submission passed back */
  __s32 res;       /* result code for this event */
  __u32 flags;
  IOType type;
};

struct IORequestInfo;
class Worker;
struct ConnectionTask {
 public:
  struct promise_type {
   public:
    // 返回值，见errno.h
    int ret;

    // 负责当前io的worker，在accept后设置，为了使用fixed
    // file特性，协程的recv与send均由该worker完成
    Worker *io_worker = NULL;
    // 负责process任务的worker，可能与io_worker不同
    Worker *process_worker = NULL;

    // 当前需要处理的IO任务的cqe，拷贝一份
    struct coroutine_cqe cqe;

    // 当前正在进行的IO
    IOType current_io = IOType::NONE;

   public:
    // 协程在开始执行之前会调用该方法，返回协程句柄
    auto get_return_object() {
      return ConnectionTask{CoroutineHandler::from_promise(*this)};
    }
    // co_return返回值设置
    void return_value(int ret) { this->ret = ret; }
    // 协程初始化时挂起
    std::suspend_always initial_suspend() noexcept { return {}; }
    // destroy时立即销毁协程
    std::suspend_never final_suspend() noexcept { return {}; }
    // 处理异常
    void unhandled_exception() {
#ifdef PRODUCTION
      Log::error("xxxxx");
#else
      std::terminate();
#endif
    }
  };

 public:
  using CoroutineHandler = std::coroutine_handle<promise_type>;
  CoroutineHandler handler;  // 协程句柄

 public:
  ConnectionTask(CoroutineHandler handler) : handler(handler) {}
};

struct IORequestInfo {
  int fd;
  // 对于使用io_link串联的写请求来说，中间的cqe不需要恢复协程，只有最后一个cqe出现时才恢复协程
  bool need_resume;
  // IO类型
  IOType type;
};

// 拷贝cqe
void copy_cqe(struct coroutine_cqe &dest, struct io_uring_cqe &src);

// socket_recv
struct socket_recv_awaitable {
  int sock_fd_idx;                                  // 读数据socket
  http::request_parser<http::string_body> &parser;  // http parser

  ConnectionTask::CoroutineHandler handler;
  Worker *io_worker;

  bool await_ready();
  void await_suspend(ConnectionTask::CoroutineHandler h);
  // 返回是否已经完成
  // false-读取未完成，需要重新co_await调用
  // true-读取已经完成/解析出错，无需重新co_await调用
  bool await_resume();
};
socket_recv_awaitable socket_recv(
    int sock_fd_idx, http::request_parser<http::string_body> &parser);

// socket_send
struct socket_send_awaitable {
  int sock_fd_idx;

  // 是否需要重新调用
  bool finish_send = false;

  // 发送是否失败
  bool &send_error_occurs;

  // 被序列化后的buffer
  std::list<boost::asio::const_buffer> &serialized_buffers;

  // 记录被用于send的buffer id，后续需要回收
  std::list<int> used_buffer_id;

  Worker *io_worker = NULL;
  ConnectionTask::CoroutineHandler handler;

  bool await_ready();
  void await_suspend(ConnectionTask::CoroutineHandler h);

  // 返回是否已经完成
  // false-写入未完成，需要重新co_await调用
  // true-写入已经完成
  bool await_resume();
};
socket_send_awaitable socket_send(int sock_fd_idx,
                                  std::list<boost::asio::const_buffer> &buffers,
                                  bool &send_error_occurs);

// socket_close
struct socket_close_awaitable {
  int sock_fd_idx;
  ConnectionTask::CoroutineHandler handler;
  bool await_ready();
  // 提交close请求
  void await_suspend(ConnectionTask::CoroutineHandler h);
  void await_resume();
};
socket_close_awaitable socket_close(int sock_fd_idx);

// add current coroutine to work-stealing queue
// 将当前协程添加到ws队列（本地满了就加global），可以被其他线程偷窃
struct add_process_task_to_wsq_awaitable {
  ConnectionTask::CoroutineHandler handler;
  bool await_ready();
  void await_suspend(ConnectionTask::CoroutineHandler h);
  void await_resume();
};
add_process_task_to_wsq_awaitable add_process_task_to_wsq();

// add current coroutine to io_worker private io task queue
// 其他worker偷窃协程，处理完process()任务后，将协程的执行权交还给io_worker
struct add_io_task_back_to_io_worker_awaitable {
  int sock_fd_idx;
  bool await_ready();
  bool await_suspend(ConnectionTask::CoroutineHandler h);
  void await_resume();
};
add_io_task_back_to_io_worker_awaitable add_io_task_back_to_io_worker(int sock_fd_idx);

ConnectionTask handle_http_request(
    int sock_fd_idx, std::function<int(http::request<http::string_body> &,
                                       http::response<http::string_body> &)>
                         processor);

class Service {
 private:
  const int worker_num;
  std::vector<Worker *> workers;
  std::vector<std::thread> threads;
  // 全局process任务队列
  boost::lockfree::queue<ConnectionTask::CoroutineHandler> global_queue;
  friend Worker;

 public:
  Service(int worker_num, int max_conn_num, const std::string &ip, int port,
          int init_buffer_num, int max_buffer_num,
          std::function<int(http::request<http::string_body> &,
                            http::response<http::string_body> &)>
              http_handler);
  ~Service();

  void start();
};

class Worker {
 private:
  const int max_conn_num_;                      // 最大连接数
  const int io_uring_entries_;                  // io_uring sqe容量
  const int max_fixed_file_num_;                // 最大注册文件数
  const int max_buffer_num_;                    // 最大缓存数
  const int self_worker_id_;                    // 工作线程唯一标识符
  const int page_size = sysconf(_SC_PAGESIZE);  // 系统页大小，作为缓存大小

  std::vector<void *> read_buffer_pool_;   // 缓存池
  std::vector<void *> write_buffer_pool_;  // 缓存池

  // 未使用的写缓存id，由于send只会由本工作线程提交，因此此处串行无锁，无需与log一样使用lock-free-queue
  std::queue<int> unused_write_buffer_id_;
  // 用于管理prov_buf的环状队列，称为buf ring
  struct io_uring_buf_ring *buf_ring_ = NULL;

  // 开启的连接，key为direct socket的index
  std::map<int, ConnectionTask> connections_;

  int listen_fd_;

  // 包含global queue与其他worker信息
  Service *service_;

  // 此字段没有用，因为multishot每次accept一个请求都会更新该字段，但是进行accept仍然需要该字段
  struct sockaddr client_addr;
  socklen_t client_len = sizeof(client_addr);

  // work stealing任务队列，结构无锁，可被其他worker访问
  // 只装process任务
  boost::lockfree::queue<ConnectionTask::CoroutineHandler,
                         boost::lockfree::fixed_sized<true>>
      ws_process_task_queue;

  // 本地私有队列，只有本worker访问，串行无锁，用于read与write请求的提交
  // 只装io任务
  std::queue<int> private_io_task_queue;

  struct io_uring ring;

  // http处理器
  std::function<int(http::request<http::string_body> &,
                    http::response<http::string_body> &)>
      processor_;

  // 各awaitable对象的友元声明
  friend socket_recv_awaitable;
  friend socket_send_awaitable;
  friend socket_close_awaitable;
  friend add_process_task_to_wsq_awaitable;
  friend add_io_task_back_to_io_worker_awaitable;

 private:
  // 提交multishot_accept请求
  void add_multishot_accept(int listen_fd);
  // 提交multishot_recv请求
  void add_recv(int sock_fd_idx, ConnectionTask::CoroutineHandler handler,
                bool poll_first);
  // 提交send_zc请求
  void add_zero_copy_send(int sock_fd_idx,
                          ConnectionTask::CoroutineHandler handler,
                          std::list<int> used_buffer_id, int msg_len);
  // 关闭连接（提交close请求）
  void disconnect(int sock_fd_idx, ConnectionTask::CoroutineHandler handler);
  // 扩展写缓存池
  void extend_write_buffer_pool(int extend_buf_num);
  // 回收prov_buf
  void retrive_prov_buf(int prov_buf_id);
  // 回收write buf
  void retrive_write_buf(int buf_id);
  // 添加prov_buf
  bool try_extend_prov_buf();
  // 获取buf
  void *get_write_buf(int buf_id);
  void *get_prov_buf(int buf_id);
  // 将序列化后的buffer发送给客户端
  void send_to_client(int sock_fd_idx,
                      std::list<boost::asio::const_buffer> &serialized_buffers,
                      std::list<int> &used_buffer_id,
                      ConnectionTask::CoroutineHandler h, bool &finish_send);
  int get_worker_id();
  // 添加process任务至work-stealing-queue
  void add_process_task(ConnectionTask::CoroutineHandler h);
  // 添加IO恢复任务至private-io-queue 
  void add_io_resume_task(int sock_fd_idx);
  // 获取io resume task
  bool try_get_io_task_queue(ConnectionTask::CoroutineHandler &h);
  // 处理accept请求
  void handle_accept(const struct io_uring_cqe *cqe);

 public:
  Worker(int max_conn_num, const std::string &ip, int port, int worker_id,
         int init_buffer_num, int max_buffer_num,
         std::function<int(http::request<http::string_body> &,
                           http::response<http::string_body> &)>
             processor,
         Service *service);
  void run();
};

#endif  // __WORKER_H__