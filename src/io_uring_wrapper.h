#ifndef __IO_URING_WRAPPER_H__
#define __IO_URING_WRAPPER_H__

#include <boost/lockfree/queue.hpp>

#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/parser.hpp>
#include <boost/beast/http/serializer.hpp>
#include <boost/beast/http/string_body.hpp>

#include <coroutine>
#include <list>
#include <thread>
#include <queue>
#include <variant>
#include <vector>

#include "log.h"
#include "util.hpp"

enum IOType : uint8_t
{
  ACCEPT,
  RECV_SOCKET,
  MULTIPLE_SEND_SOCKET,
  MULTIPLE_SEND_ZC_SOCKET,
  SENDMSG_SOCKET,
  SENDMSG_ZC_SOCKET,
  CLOSE_SOCKET,
  SHUTDOWN,
  NONE,
  READ_FILE,
  OPEN_FILE,
  CLOSE_FILE,
  SEND_FILE,
  CANCEL_ACCPET
};

struct IORequestInfo
{
  int fd;
  // 对于使用io_link串联的写请求来说，需要记录串联请求的编号
  int16_t req_id;
  // IO类型
  IOType type;
  // 填充
  char padding = '0';
};

struct send_buf_info
{
  // buffer在写缓冲池中的id，-1表示临时创建，不属于缓冲池
  int buf_id = -1;
  const void *buf = NULL;
  int len = 0;
  send_buf_info() = default;
  send_buf_info(int buf_id, const void *buf, int len) : buf_id(buf_id), buf(buf), len(len) {}
};

class IOUringWrapper
{
private:
  const int max_conn_num_;                     // 最大连接数
  const int io_uring_entries_;                 // io_uring sqe容量
  const int max_fixed_file_num_;               // 最大注册文件数
  const int max_buffer_num_;                   // 最大缓存数
  const int page_size = sysconf(_SC_PAGESIZE); // 系统页大小，作为缓存大小

  std::vector<void *> read_buffer_pool_;  // 缓存池
  std::vector<void *> write_buffer_pool_; // 缓存池

  // 未使用的写缓存id，由于send只会由本工作线程提交，因此此处串行无锁，无需与log一样使用lock-free-queue
  std::queue<int> unused_write_buffer_id_;
  // 用于管理prov_buf的环状队列，称为buf ring
  struct io_uring_buf_ring *buf_ring_ = NULL;

  struct io_uring ring;

  // 此字段没有用，因为multishot每次accept一个请求都会更新该字段，但是进行accept仍然需要该字段
  struct sockaddr client_addr;
  socklen_t client_len = sizeof(client_addr);

public:
  IOUringWrapper();
  // 扩展写缓存池
  void extend_write_buffer_pool(int extend_buf_num);
  // 回收prov_buf
  void retrive_prov_buf(int prov_buf_id);
  // 回收write buf
  void retrive_write_buf(int buf_id);
  // 添加prov_buf
  bool try_extend_prov_buf();
  // 获取buf
  void *get_write_buf_by_id(int buf_id);
  void *get_prov_buf(int buf_id);
  // 获取或创建
  send_buf_info get_write_buf_or_create();

  // 提交multishot_accept请求
  void add_multishot_accept(int listen_fd);
  // 取消IO请求
  bool cancel_socket_accept(int fd);
  // 提交multishot_recv请求
  bool add_recv(int sock_fd_idx, bool poll_first);

  // 关闭连接（提交close请求）
  bool disconnect(int sock_fd_idx);
  // 提交read请求
  void add_read(int sock_fd_idx, int read_file_fd_idx, const send_buf_info &read_buf, bool fixed_file);
  // 读取文件
  bool read_file(int sock_fd_idx, int read_file_fd_idx, int file_size, send_buf_info &read_buf, bool fixed_file);
  // 打开文件
  bool open_file_direct(int sock_fd_idx, const std::string &path, mode_t mode);
  // 关闭文件
  bool close_direct_file(int sock_fd_idx, int file_fd_idx);
  // sendfile
  bool sendfile(int sock_fd_idx, int file_fd_idx, int chunk_size, int *sqe_num, int *pipefd);

  // 一次性提交多个send
  bool multiple_send(int sock_fd_idx, const std::list<send_buf_info> &buf_infos, bool zero_copy);
  bool add_zero_copy_send(int sock_fd_idx, const send_buf_info &info, int req_id, bool zero_copy, bool submit);
  bool add_zero_copy_sendmsg(int sock_fd_idx, const std::list<send_buf_info> &infos, iovec *sendmsg_iov, bool zero_copy);

  // get
  struct io_uring *get_struct() { return &ring; }
  int get_max_fixed_file_num() { return max_fixed_file_num_; }
};

#endif // __IO_URING_WRAPPER_H__