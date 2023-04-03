#include "http.h"
#include "awaitable.h"

#include <boost/optional/optional_io.hpp>

void serialize(SerializerType &sr,
               std::list<boost::asio::const_buffer> &buffers, error_code &ec,
               int &data_to_consume)
{
  // 序列化函数
  auto visit_func = [&buffers, &ec, &data_to_consume](auto &sr)
  {
    using T = std::decay_t<decltype(sr)>;
    // std::monostate类型直接报错
    if constexpr (std::is_same_v<T, std::monostate>)
    {
      FORCE_ASSERT(false);
    }
    // 其他类型：进行序列化
    else
    {
      data_to_consume = 0;
      bool is_finish = false;
      do
      {
        sr.next(ec, [&](error_code &ec, auto const &buffer)
                {
                  // hack: 不consume的话，next会循环输出，遇到一样的说明结束了
                  // 通过这种方式避免一次数据拷贝，等到拷贝到write_buf发送完再consume
                  boost::asio::const_buffer buf = *buffer.begin();
                  if (buffers.front().data() == buf.data())
                  {
                    Log::debug("get same buf with next()",
                               ", this means that the serialization is done.");
                    is_finish = true;
                    return;
                  }

                  ec.assign(0, ec.category());
                  for (auto it = buffer.begin(); it != buffer.end(); it++)
                  {
                    buffers.push_back(*it);
                  }
                  data_to_consume += boost::asio::buffer_size(buffer);
                  // sr.consume(boost::asio::buffer_size(buffer));
                });
      } while (!ec && !sr.is_done() && !is_finish);
    };
  };

  std::visit(visit_func, sr);
}

ConnectionTask handle_http_request(int sock_fd_idx, ProcessFuncType processor)
{
  while (true)
  {
    // 初始化parser
    http::request_parser<http::string_body> parser;
    parser.eager(true);

    // 读取数据
    bool finish_read = false;
    auto recv_awaitable = socket_recv(sock_fd_idx, parser);
    while (!finish_read)
    {
      Log::debug("co_await recv_awaitable with sock_fd_idx=", sock_fd_idx);
      // 此处会提交recv请求并挂起协程，直到recv完成
      finish_read = co_await recv_awaitable;
    }

    // 读取结束，只读到EOF，说明客户端已经关闭
    // 也不用发bad request，直接关闭连接即可
    if (!parser.got_some())
    {
      Log::debug("close socket");
      co_await socket_close(sock_fd_idx);
      co_return 0;
    }

    // 将当前协程挂起，放到work-stealings
    // queue，后续process()操作可由其他worker处理
    // 若被其他worker处理，到了send的时候再挂起，将控制权交还给io-worker，保证IO操作均由同一个worker完成
    // 这样可以利用fixed file加速
    if (config::force_get_int("ENABLE_WORK_STEALING"))
      co_await add_process_task_to_wsq();

    // 构造response
    http::request<http::string_body> request;
    ResponseType response;

    // processor参数
    struct process_func_args args;

    // 完成解析
    bool bad_request = !parser.is_done();
    if (!bad_request)
    {
      request = parser.release();
      processor(request, response, args);
    }
    // 解析失败
    else
    {
      Log::error(
          "failed to process http "
          "request|is_header_done|content_length|remain_content_length|",
          parser.is_header_done(), "|",
          parser.is_header_done() ? parser.content_length() : -1, "|",
          parser.is_header_done() ? parser.content_length_remaining() : -1);
      // 构造一个bad request报文
      response = http::response<http::string_body>{};
      auto &res = std::get<http::response<http::string_body>>(response);
      res.result(http::status::bad_request);
      res.set(http::field::content_type, "text/html");
      res.keep_alive(false);
      res.body() = "bad request: error while parsing http request";
    }

    // 将控制权交还给io_worker
    if (config::force_get_int("ENABLE_WORK_STEALING"))
      co_await add_io_task_back_to_io_worker(sock_fd_idx);

    // 记录web server用于读取文件数据的buffer
    std::map<const void *, int> used_buf;
    // process函数要求当前请求以web server方式处理
    if (args.use_web_server)
    {
      response = http::response<http::buffer_body>{};
      auto &res_buf = std::get<http::response<http::buffer_body>>(response);
      res_buf.set(http::field::content_type, "text/html");

      // open file
      // TODO: use io_uring_prep_openat_direct
      int fd = open(args.file_path.c_str(), O_RDONLY);

      // open success
      bool read_success;
      if (fd != -1)
      {
        // read file
        int bytes_num = -1, used_buffer_id = -1;
        void *buf = NULL;
        read_success = co_await file_read(sock_fd_idx, fd, &buf, used_buffer_id, bytes_num);

        // success
        if (read_success)
        {
          used_buf[buf] = used_buffer_id;

          // header
          res_buf.version(request.version());
          res_buf.result(http::status::ok);

          // body
          res_buf.body().data = buf;
          res_buf.body().size = bytes_num;
          res_buf.body().more = false;
        }
        close(fd);
      }

      // open or read fail
      if (fd == -1 || !read_success)
      {
        if (fd == -1)
          Log::error("open file failed with file_path=", args.file_path,
                     " reason: ", strerror(errno));
        else
          Log::error("read file failed, file_path=", args.file_path, "|sock_fd_idx=", sock_fd_idx);

        res_buf.version(request.version());
        res_buf.result(http::status::not_found);
        res_buf.body().data = NULL;
        res_buf.body().size = 0;
        res_buf.body().more = false;
      }
    }

    // 序列化response
    std::list<boost::asio::const_buffer> buffers;
    error_code ec;
    int data_to_consume = 0;
    // 为了减少拷贝次数，serializer需要在闭包外初始化，等后续在consume
    SerializerType serializer;

    std::visit(
        [&](auto &res)
        {
          res.prepare_payload();
          using BodyType = typename std::decay_t<decltype(res)>::body_type;
          serializer.emplace<http::response_serializer<BodyType>>(res);
          serialize(serializer, buffers, ec, data_to_consume);
        },
        response);

    if (ec)
    {
      Log::error(ec.message());
      co_await socket_close(sock_fd_idx);
      co_return EPROTO;
    }

    // 返回数据给客户端
    bool finish_send = false, send_error_occurs = false;
    auto awaitable_send =
        socket_send(sock_fd_idx, buffers, send_error_occurs, used_buf, args.buffer_to_delete);
    while (!finish_send)
    {
      Log::debug("co_await awaitable_send with sock_fd_idx=", sock_fd_idx);
      finish_send = co_await awaitable_send;
    }

    // 清理serializer数据
    std::visit(
        [=](auto &sr)
        {
          using T = std::decay_t<decltype(sr)>;
          // std::monostate类型直接报错
          if constexpr (std::is_same_v<T, std::monostate>)
          {
            FORCE_ASSERT(false);
          }
          // consume
          else
          {
            sr.consume(data_to_consume);
          }
        },
        serializer);

    // 断开连接
    bool keep_alive =
        std::visit([](auto &res)
                   { return res.keep_alive(); },
                   response);
    if (send_error_occurs || !request.keep_alive() || !keep_alive)
    {
      Log::debug("close socket");
      co_await socket_close(sock_fd_idx);
      co_return 0;
    }
  }
}