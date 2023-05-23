#pragma once

#include "worker.h"

#include <boost/beast/core/make_printable.hpp>

// 调用awaitable对象的返回结果
struct awaitable_result
{
  bool is_submitted; // 是否成功提交sqe
  int res;           // cqe.res
};

struct socket_recv_awaitable
{
  int sock_fd_idx;                                 // 读数据socket
  http::request_parser<http::string_body> &parser; // http parser

  ConnectionTaskHandler handler;
  Worker *io_worker;

  bool is_submitted = false;

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
  bool zero_copy;
  bool is_submitted = false;
  std::list<send_buf_info> buf_infos;

  Worker *io_worker = NULL;
  ConnectionTaskHandler handler;

  bool await_ready();
  void await_suspend(ConnectionTaskHandler h);
  awaitable_result await_resume();
};
inline auto socket_send(int sock_fd_idx, std::list<send_buf_info> buf_infos, bool zero_copy)
{
  return socket_send_awaitable{.sock_fd_idx = sock_fd_idx,
                               .zero_copy = zero_copy,
                               .buf_infos = buf_infos};
}
inline auto socket_send(int sock_fd_idx, send_buf_info buf_info, bool zero_copy)
{
  std::list<send_buf_info> buf_infos{buf_info};
  return socket_send_awaitable{.sock_fd_idx = sock_fd_idx,
                               .zero_copy = zero_copy,
                               .buf_infos = buf_infos};
}

struct socket_sendmsg_awaitable
{
  int sock_fd_idx;
  std::list<send_buf_info> buf_infos;
  bool is_submitted = false;
  bool zero_copy;

  Worker *io_worker = NULL;
  ConnectionTaskHandler handler;

  bool await_ready();
  void await_suspend(ConnectionTaskHandler h);
  awaitable_result await_resume();
};
inline auto socket_sendmsg(int sock_fd_idx, std::list<send_buf_info> buf_infos, bool zero_copy)
{
  return socket_sendmsg_awaitable{
      .sock_fd_idx = sock_fd_idx,
      .buf_infos = buf_infos,
      .zero_copy = zero_copy};
}

// socket_close
struct socket_close_awaitable
{
  int sock_fd_idx;

  bool is_submitted = false;

  ConnectionTaskHandler handler;
  bool await_ready();
  // 提交close请求
  void await_suspend(ConnectionTaskHandler h);
  awaitable_result await_resume();
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
inline auto add_io_task_back_to_io_worker(int sock_fd_idx)
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

  bool is_submitted = false;

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

  bool is_submitted = false;

  Worker *io_worker = NULL;
  ConnectionTaskHandler handler;

  bool await_ready();
  void await_suspend(ConnectionTaskHandler h);
  bool await_resume();
};
inline auto file_close(int sock_fd_idx, int file_fd_idx)
{
  return file_close_awaitable{
      .sock_fd_idx = sock_fd_idx,
      .file_fd_idx = file_fd_idx};
}

// 读取磁盘文件
struct file_read_awaitable
{
  int sock_fd_idx;
  int read_file_fd_idx;
  int file_size;
  send_buf_info &read_buf;

  // read、write操作发生在io_worker中
  Worker *io_worker = NULL;
  ConnectionTaskHandler handler;
  bool is_submitted = false;

  bool await_ready();
  void await_suspend(ConnectionTaskHandler h);
  // 返回是否成功: true-读取成功 false-读取失败
  awaitable_result await_resume();
};
// 读取磁盘文件
inline auto file_read(int sock_fd_idx, int read_file_fd_idx, int file_size, send_buf_info &read_buf)
{
  return file_read_awaitable{.sock_fd_idx = sock_fd_idx,
                             .read_file_fd_idx = read_file_fd_idx,
                             .file_size = file_size,
                             .read_buf = read_buf};
}

// send file from file to socket
struct file_send_awaitable
{
  int sock_fd_idx;
  int read_file_fd_idx;
  int file_size;
  bool *is_success;
  bool is_pipe_init = false;
  int pipefd[2];

  bool is_submitted = false;
  Worker *io_worker = NULL;
  ConnectionTaskHandler handler;

  bool fixed_file;

  bool await_ready();
  void await_suspend(ConnectionTaskHandler h);
  bool await_resume();
};
inline auto file_send(int sock_fd_idx, int read_file_fd_idx, int file_size, bool *is_success,
                      bool fixed_file)
{
  return file_send_awaitable{
      .sock_fd_idx = sock_fd_idx,
      .read_file_fd_idx = read_file_fd_idx,
      .file_size = file_size,
      .is_success = is_success,
      .fixed_file = fixed_file};
}

void copy_serialized_buffer_to_write_buffer(IOUringWrapper &io_uring,
                                            std::list<boost::asio::const_buffer> &src_bufs,
                                            std::list<send_buf_info> &dest_bufs);

struct serialize_awaitable
{
  // 入参
  std::variant<http::response_serializer<http::string_body> *, http::response_serializer<http::buffer_body> *> serializer;
  std::list<send_buf_info> &send_bufs;
  bool header_only;

  // 返回值
  error_code err = {};

  Worker *io_worker = NULL;

  bool await_ready() { return false; }
  bool await_suspend(ConnectionTaskHandler h)
  {
    // header与body分开处理
    auto visit_func = [&](auto &sr)
    {
      io_worker = h.promise().io_worker;
      sr->split(true);

      IOUringWrapper &io_uring = io_worker->get_io_uring();

      // 序列化header
      std::list<boost::asio::const_buffer> header_sr_bufs;
      do
      {
        sr->next(err,
                 [&](error_code &ec, auto const &buffer)
                 {
                   ec = {};
                   // 收集header序列化后零碎的buffer
                   for (auto it = buffer.begin(); it != buffer.end(); it++)
                     header_sr_bufs.push_back(*it);
                   // debug模式下输出buffer内容
                   if constexpr (UtilEnv::BuildMode == UtilEnv::DEBUG)
                     std::cout << make_printable(buffer);
                   // 消费header中的buffer
                   sr->consume(boost::asio::buffer_size(buffer));
                 });
      } while (!err && !sr->is_header_done());

      // error
      if (err)
        return;

      // 拷贝数据到写缓存中
      copy_serialized_buffer_to_write_buffer(io_uring, header_sr_bufs, send_bufs);

      // 完成则退出
      if (sr->is_done())
        return;

      // body
      std::list<boost::asio::const_buffer> body_sr_bufs;
      do
      {
        sr->next(err,
                 [&](error_code &ec, auto const &buffer)
                 {
                   ec = {};
                   // 收集header序列化后零碎的buffer
                   for (auto it = buffer.begin(); it != buffer.end(); it++)
                     body_sr_bufs.push_back(*it);
                   // debug模式下输出buffer内容
                   if constexpr (UtilEnv::BuildMode == UtilEnv::DEBUG)
                     std::cout << make_printable(buffer);
                   // 消费header中的buffer
                   sr->consume(boost::asio::buffer_size(buffer));
                 });
      } while (!err && !sr->is_done());

      // error
      if (err)
        return;

      // header部分的最后一块缓存，可能未满，可以塞一些body的数据
      send_buf_info &header_buf_info = send_bufs.back();
      const int page_size = sysconf(_SC_PAGESIZE);
      for (auto &buf : body_sr_bufs)
      {
        // 若body数据量小，可以塞到header的缓存中，则拷贝数据
        if (header_buf_info.len + buf.size() <= page_size)
        {
          memcpy((char *)header_buf_info.buf + header_buf_info.len, buf.data(), buf.size());
          header_buf_info.len += buf.size();
        }
        // 若body数据量大，无法塞到header的缓存中，则保留原状
        else
        {
          send_bufs.emplace_back(-1, buf.data(), buf.size());
        }
      };
    };

    std::visit(visit_func, serializer);

    // 退出
    return false;

    // using BodyType = typename std::decay_t<decltype(res)>::body_type;
    // if constexpr (std::is_same_v<BodyType, http::string_body>)
  }

  error_code await_resume() { return err; }
};

template <typename T>
inline auto serialize(T *serializer, std::list<send_buf_info> &send_bufs, bool header_only)
{
  return serialize_awaitable{
      .serializer = serializer,
      .send_bufs = send_bufs,
      .header_only = header_only};
}

// TODO：header和body分开序列化，header手动拷贝到fixed buffer中
// body如果是string，也分两次发送
// body如果是buf，直接分两次发送
// 用那个静态判断，按照模板类型的不同，写不同代码
// 总之最后的结果就是返回一个 list<buf_info>
// 之后处理一下cqe的挂起和恢复的问题

struct retrive_buffer_awaitable
{
  const send_buf_info &buf;
  bool await_ready();
  bool await_suspend(ConnectionTaskHandler h);
  void await_resume();
};

inline auto retrive_write_buf(const send_buf_info &buf)
{
  return retrive_buffer_awaitable{
      .buf = buf};
}