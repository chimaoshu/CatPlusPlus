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

using namespace boost::beast;

enum IOType : uint8_t
{
  ACCEPT,
  RECV,
  SEND,
  CLOSE,
  SHUTDOWN,
  NONE,
  READ
};

// cqe的柔性数组在c++20协程中会出问题
// 因此定义一个没有柔性数组的cqe，在协程中使用
struct coroutine_cqe
{
  __u64 user_data; /* sqe->data submission passed back */
  __s32 res;       /* result code for this event */
  __u32 flags;
};

struct IORequestInfo;
class Worker;
struct ConnectionTask
{
public:
  struct promise_type
  {
  public:
    // 返回值，见errno.h
    int ret;

    // 负责当前io的worker，在accept后设置，为了使用fixed
    // file特性，协程的recv与send均由该worker完成
    Worker *net_io_worker = NULL;
    // 负责process任务的worker，可能与io_worker不同
    Worker *process_worker = NULL;

    // 当前需要处理的IO任务的cqe，拷贝一份
    struct coroutine_cqe cqe;

    // 当前正在进行的IO
    IOType current_io = IOType::NONE;

    // 进行RECV操作时，记录多个SQE的完成与否，全部完成后才可以恢复协程
    std::map<int, bool> recv_sqe_complete;

  public:
    // 协程在开始执行之前会调用该方法，返回协程句柄
    auto get_return_object()
    {
      return ConnectionTask{
          std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    // co_return返回值设置
    void return_value(int ret) { this->ret = ret; }
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

using ResponseType = std::variant<http::response<http::string_body>,
                                  http::response<http::buffer_body>>;

using SerializerType =
    std::variant<std::monostate, http::response_serializer<http::string_body>,
                 http::response_serializer<http::buffer_body>>;

// process函数的参数
struct process_func_args
{
  // 以web server方式处理，以及响应的文件路径
  bool use_web_server = false;
  std::string file_path;
  // 若使用buffer_body，可将buffer注册到此处，send完成后释放
  std::list<void *> buffer_to_delete;
};

using ProcessFuncType =
    std::function<int(http::request<http::string_body> &, ResponseType &,
                      process_func_args &args)>;

struct IORequestInfo
{
  int fd;
  // 对于使用io_link串联的写请求来说，需要记录串联请求的编号
  int16_t recv_id;
  // IO类型
  IOType type;
};

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
  const int max_conn_num_;                     // 最大连接数
  const int io_uring_entries_;                 // io_uring sqe容量
  const int max_fixed_file_num_;               // 最大注册文件数
  const int max_buffer_num_;                   // 最大缓存数
  const int self_worker_id_;                   // 工作线程唯一标识符
  const int page_size = sysconf(_SC_PAGESIZE); // 系统页大小，作为缓存大小

  std::vector<void *> read_buffer_pool_;  // 缓存池
  std::vector<void *> write_buffer_pool_; // 缓存池

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
  boost::lockfree::queue<ConnectionTaskHandler,
                         boost::lockfree::fixed_sized<true>>
      ws_process_task_queue;

  // 本地私有队列，只有本worker访问，串行无锁，用于read与write请求的提交
  // 只装io任务
  boost::lockfree::queue<int> private_io_task_queue;

  struct io_uring ring;

  // http处理器
  ProcessFuncType processor_;

  // 各awaitable对象的友元声明
  friend struct socket_recv_awaitable;
  friend struct socket_send_awaitable;
  friend struct socket_close_awaitable;
  friend struct add_process_task_to_wsq_awaitable;
  friend struct add_io_task_back_to_io_worker_awaitable;
  friend struct file_read_awaitable;

private:
  // 提交multishot_accept请求
  void add_multishot_accept(int listen_fd);
  // 提交multishot_recv请求
  void add_recv(int sock_fd_idx, ConnectionTaskHandler handler,
                bool poll_first);
  // 提交send_zc请求
  struct send_buf_info
  {
    int buf_id;
    const void *buf;
    int len;
    send_buf_info(int buf_id, const void *buf, int len) : buf_id(buf_id), buf(buf), len(len) {}
  };
  void add_zero_copy_send(
      int sock_fd_idx, ConnectionTaskHandler handler,
      const std::list<send_buf_info> &buf_infos);
  // 关闭连接（提交close请求）
  void disconnect(int sock_fd_idx, ConnectionTaskHandler handler);
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
                      std::list<send_buf_info> &buf_infos,
                      ConnectionTaskHandler h, bool &send_submitted,
                      const std::map<const void *, int> &read_file_buf);
  int get_worker_id();
  // 添加process任务至work-stealing-queue
  void add_process_task(ConnectionTaskHandler h);
  // 添加IO恢复任务至private-io-queue
  void add_io_resume_task(int sock_fd_idx);
  // 获取io resume task
  bool try_get_io_task_queue(ConnectionTaskHandler &h);
  // 处理accept请求
  void handle_accept(const struct io_uring_cqe *cqe);
  // 提交read请求
  void add_read(int sock_fd_idx, int read_file_fd, int file_size, void **buf,
                int buf_idx);
  // 读取文件
  void read_file(int sock_fd_idx, int read_file_fd, int &used_buffer_id,
                 void **buf);

public:
  Worker(int worker_id, ProcessFuncType processor, Service *service);

  void run();
};

#endif // __WORKER_H__