#pragma once

#include "worker.h"

struct socket_recv_awaitable
{
  int sock_fd_idx;                                 // 读数据socket
  http::request_parser<http::string_body> &parser; // http parser

  ConnectionTaskHandler handler;
  Worker *io_worker;

  bool await_ready();
  void await_suspend(ConnectionTaskHandler h);
  // 返回是否已经完成
  // false-读取未完成，需要重新co_await调用
  // true-读取已经完成/解析出错，无需重新co_await调用
  bool await_resume();
};
inline auto socket_recv(
    int sock_fd_idx, http::request_parser<http::string_body> &parser)
{
  return socket_recv_awaitable{.sock_fd_idx = sock_fd_idx, .parser = parser};
}

// socket_send
struct socket_send_awaitable
{
  int sock_fd_idx;

  // 是否需要重新调用
  bool send_submitted = false;

  // 发送是否失败
  bool &send_error_occurs;

  // 被序列化后的buffer
  std::list<boost::asio::const_buffer> &serialized_buffers;

  // 记录被用于send的buffer id，后续需要回收
  std::list<Worker::send_buf_info> buf_infos;

  const std::map<const void *, int> &used_write_buf;

  Worker *io_worker = NULL;
  ConnectionTaskHandler handler;

  bool await_ready();
  void await_suspend(ConnectionTaskHandler h);

  // 返回是否已经完成
  // false-写入未完成，需要重新co_await调用
  // true-写入已经完成
  bool await_resume();
};
// socket_send会先将serializer产生的buffers中内容拷贝到write_buf中，再进行发送
// 若buffers有些buffers本来就是write_buf，可以将其buf与id添加到write_used_buffer中
// socket_send将会跳过这部分buffer的拷贝
inline auto socket_send(int sock_fd_idx,
                        std::list<boost::asio::const_buffer> &buffers,
                        bool &send_error_occurs,
                        const std::map<const void *, int> &used_write_buf)
{
  return socket_send_awaitable{.sock_fd_idx = sock_fd_idx,
                               .send_error_occurs = send_error_occurs,
                               .serialized_buffers = buffers,
                               .used_write_buf = used_write_buf};
}

// socket_close
struct socket_close_awaitable
{
  int sock_fd_idx;
  ConnectionTaskHandler handler;
  bool await_ready();
  // 提交close请求
  void await_suspend(ConnectionTaskHandler h);
  void await_resume();
};
inline auto socket_close(int sock_fd_idx)
{
  return socket_close_awaitable{.sock_fd_idx = sock_fd_idx};
}

// add current coroutine to work-stealing queue
// 将当前协程添加到ws队列（本地满了就加global），可以被其他线程偷窃
struct add_process_task_to_wsq_awaitable
{
  ConnectionTaskHandler handler;
  bool await_ready();
  void await_suspend(ConnectionTaskHandler h);
  void await_resume();
};
inline auto add_process_task_to_wsq()
{
  return add_process_task_to_wsq_awaitable{};
}

// add current coroutine to io_worker private io task queue
// 其他worker偷窃协程，处理完process()任务后，将协程的执行权交还给io_worker
struct add_io_task_back_to_io_worker_awaitable
{
  int sock_fd_idx;
  bool await_ready();
  bool await_suspend(ConnectionTaskHandler h);
  void await_resume();
};
inline auto add_io_task_back_to_io_worker(
    int sock_fd_idx)
{
  return add_io_task_back_to_io_worker_awaitable{.sock_fd_idx = sock_fd_idx};
}

// open file
struct file_open_awaitable
{
  int sock_fd_idx;
  const std::string &path;
  mode_t mode;
  int *file_fd_idx;

  Worker *io_worker = NULL;
  ConnectionTaskHandler handler;

  bool await_ready();
  void await_suspend(ConnectionTaskHandler h);
  bool await_resume();
};
inline auto file_open(int sock_fd_idx, const std::string &path, mode_t mode, int *file_fd_idx)
{
  return file_open_awaitable{
      .sock_fd_idx = sock_fd_idx,
      .path = path,
      .mode = mode,
      .file_fd_idx = file_fd_idx,
  };
}

// close file
struct file_close_awaitable
{
  int sock_fd_idx;
  int file_fd_idx;

  Worker *io_worker = NULL;
  ConnectionTaskHandler handler;

  bool await_ready();
  void await_suspend(ConnectionTaskHandler h);
  bool await_resume();
};
inline auto file_close(int sock_fd_idx, int file_fd_idx)
{
  return file_close_awaitable {
    .sock_fd_idx = sock_fd_idx,
    .file_fd_idx = file_fd_idx
  };
}

// 读取磁盘文件
struct file_read_awaitable
{
  int sock_fd_idx;
  int read_file_fd;
  int file_size;
  // 读取文件使用的buffer：fixed buffer或者temp buffer
  void **buf;
  // used_buffer_id=-1，表示使用temp buffer，否则为write buffer
  int *used_buffer_id;
  // 读取buffer大小
  int *bytes_num;

  // read、write操作发生在io_worker中
  Worker *io_worker = NULL;
  ConnectionTaskHandler handler;

  bool await_ready();
  void await_suspend(ConnectionTaskHandler h);
  // 返回是否成功: true-读取成功 false-读取失败
  bool await_resume();
};
// 读取磁盘文件
inline auto file_read(int sock_fd_idx, int read_file_fd, int file_size,
                      void **buf, int *used_buffer_id, int *bytes_num)
{
  return file_read_awaitable{.sock_fd_idx = sock_fd_idx,
                             .read_file_fd = read_file_fd,
                             .file_size = file_size,
                             .buf = buf,
                             .used_buffer_id = used_buffer_id,
                             .bytes_num = bytes_num};
}

// send file from file to socket
struct file_send_awaitable
{
  int sock_fd_idx;
  int read_file_fd_idx;
  int file_size;
  bool *is_success;
  int pipefd[2];

  Worker *io_worker = NULL;
  ConnectionTaskHandler handler;

  bool await_ready();
  void await_suspend(ConnectionTaskHandler h);
  bool await_resume();
};
inline auto file_send(int sock_fd_idx, int read_file_fd_idx, int file_size, bool *is_success)
{
  return file_send_awaitable{
      .sock_fd_idx = sock_fd_idx,
      .read_file_fd_idx = read_file_fd_idx,
      .file_size = file_size,
      .is_success = is_success};
}