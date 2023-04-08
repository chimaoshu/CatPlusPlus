#include "http.h"
#include "awaitable.h"

#include <filesystem>

#include <boost/beast/http/span_body.hpp>
#include <boost/optional/optional_io.hpp>
#include <boost/beast/core/make_printable.hpp>

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
      bool finish_close = false;
      while (!finish_close)
        finish_close = co_await socket_close(sock_fd_idx);
      co_return 0;
    }

    // 将当前协程挂起，放到work-stealings
    // queue，后续process()操作可由其他worker处理
    // 若被其他worker处理，到了send的时候再挂起，将控制权交还给io-worker，保证IO操作均由同一个worker完成
    // 这样可以利用fixed file加速
    static int enable_work_stealing = config::force_get_int("ENABLE_WORK_STEALING");
    if (enable_work_stealing)
      co_await add_process_task_to_wsq();

    // 构造response
    http::request<http::string_body> request;
    http::response<http::string_body> response;
    bool use_splice_to_transfer_body = false;

    bool use_buf_body = false;
    http::response<http::buffer_body> response_buf;

    // processor参数
    struct process_func_args args;

    // web server相关参数
    int web_server_file_fd = -1;
    int web_server_file_size = -1;
    static bool use_direct_file = config::force_get_int("USE_DIRECT_FILE");
    std::map<const void *, int> web_file_used_buf;

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
      response.result(http::status::bad_request);
      response.set(http::field::content_type, "text/html");
      response.keep_alive(false);
      response.body() = "bad request: error while parsing http request";
    }

    // 将控制权交还给io_worker
    if (enable_work_stealing)
      co_await add_io_task_back_to_io_worker(sock_fd_idx);

    do
    {
      // process函数要求当前请求以web server方式处理
      if (!bad_request && args.use_web_server)
      {
        response.set(http::field::content_type, "text/html");

        // check path
        std::filesystem::path fs_path(args.file_path);
        FORCE_ASSERT(std::filesystem::is_regular_file(fs_path));

        // get file size
        web_server_file_size = std::filesystem::file_size(fs_path);

        // 打开文件，使用direct_file
        if (use_direct_file)
        {
          auto open_awaitable = file_open(sock_fd_idx, args.file_path, O_RDONLY, &web_server_file_fd);
          bool finish_open = false;
          while (!finish_open)
          {
            Log::debug("co_await open, sock_fd_idx=", sock_fd_idx);
            finish_open = co_await open_awaitable;
          }
        }
        // 打开文件，使用普通open
        else
        {
          web_server_file_fd = open(args.file_path.c_str(), O_RDONLY);
          if (web_server_file_fd == -1)
            Log::error("open file failed, path=", args.file_path);
        }

        // 打开文件失败
        if (web_server_file_fd == -1)
        {
          Log::error("open file failed, file_path=", args.file_path, "|sock_fd_idx=", sock_fd_idx);
          response.version(request.version());
          response.keep_alive(request.keep_alive());
          response.result(http::status::not_found);
          response.body() = "404 not found";

          // 跳到serialize-send阶段，直接发送错误响应
          break;
        }

        // web file size大于阈值使用splice进行发送
        static int splice_threshold = config::force_get_int("SPLICE_THRESHOLD");
        use_splice_to_transfer_body = ((web_server_file_fd != -1) &&
                                       (web_server_file_size > splice_threshold));

        assert(web_server_file_fd != -1);
        assert(!bad_request);
        assert(args.use_web_server);

        // 使用splice传输body，此处只要构造header，后续只要序列化并send header
        // send header 结束后再使用splice传输body
        if (use_splice_to_transfer_body)
        {
          // header
          response.version(request.version());
          response.keep_alive(request.keep_alive());
          response.result(http::status::ok);
          response.content_length(web_server_file_size);

          // 跳到serialize-send阶段
          break;
        }

        // 不使用splice模式，而是使用read+send模式
        else if (!use_splice_to_transfer_body)
        {
          void *buf = NULL;
          int used_buf_id = -1;
          bool read_success = false;
          auto awaitable = file_read(sock_fd_idx, web_server_file_fd,
                                     web_server_file_size, &buf, &used_buf_id,
                                     &read_success, use_direct_file);

          // 读取
          bool finish_read = false;
          while (!finish_read)
          {
            Log::debug("co_await file_read, sock_fd_idx=", sock_fd_idx);
            finish_read = co_await awaitable;
          }

          // 读取成功
          if (read_success)
          {
            web_file_used_buf[buf] = used_buf_id;
            use_buf_body = true;

            // header
            response_buf.set(http::field::content_type, "text/html");
            response_buf.version(request.version());
            response_buf.result(http::status::ok);
            response_buf.keep_alive(request.keep_alive());

            // length
            response_buf.content_length(web_server_file_size);

            // body
            response_buf.body().data = buf;
            response_buf.body().size = web_server_file_size;
            response_buf.body().more = false;
          }
          // 读取失败
          else if (!read_success)
          {
            Log::error("read file failed, file_path=", args.file_path, "|sock_fd_idx=", sock_fd_idx);
            response.version(request.version());
            response.keep_alive(request.keep_alive());
            response.result(http::status::not_found);
            response.body() = "404 not found";
          }

          // 关闭文件 direct
          if (use_direct_file)
          {
            bool finish_close = false;
            while (!finish_close)
              finish_close = co_await file_close(sock_fd_idx, web_server_file_fd);
          }
          // 关闭文件 regular
          else
          {
            close(web_server_file_fd);
          }
        }
      }
    } while (false);

    // 序列化response
    error_code ec;
    int data_to_consume = 0;
    std::list<boost::asio::const_buffer> buffers;
    http::response_serializer<http::string_body> *serializer = NULL;
    http::response_serializer<http::buffer_body> *serializer_buf = NULL;
    if (use_buf_body)
    {
      if (!use_splice_to_transfer_body)
        response_buf.prepare_payload();
      serializer_buf = new http::response_serializer<http::buffer_body>(response_buf);
      serialize(serializer_buf, buffers, ec, data_to_consume, use_splice_to_transfer_body);
    }
    else
    {
      if (!use_splice_to_transfer_body)
        response.prepare_payload();
      serializer = new http::response_serializer<http::string_body>(response);
      serialize(serializer, buffers, ec, data_to_consume, use_splice_to_transfer_body);
    }

    // 序列化出错，直接关闭连接
    if (ec)
    {
      Log::error(ec.message());
      bool finish_close = false;
      while (!finish_close)
        finish_close = co_await socket_close(sock_fd_idx);
      co_return EPROTO;
    }

    // 返回数据给客户端
    bool finish_send = false, send_error_occurs = false;
    auto awaitable_send = socket_send(sock_fd_idx, buffers, send_error_occurs, web_file_used_buf);
    while (!finish_send)
    {
      Log::debug("co_await awaitable_send with sock_fd_idx=", sock_fd_idx);
      finish_send = co_await awaitable_send;
    }

    // 清理serializer数据
    if (use_buf_body)
    {
      serializer_buf->consume(data_to_consume);
      delete serializer_buf;
    }
    else
    {
      serializer->consume(data_to_consume);
      delete serializer;
    }

    // 如果是splice模式，前面只发送了header，现在body部分需要通过splice来实现file到socket的零拷贝发送
    if (use_splice_to_transfer_body && !send_error_occurs)
    {
      bool is_success = false;
      bool finish_sendfile = false;
      auto file_send_awaitable = file_send(sock_fd_idx, web_server_file_fd,
                                           web_server_file_size, &is_success,
                                           use_direct_file);
      while (!finish_sendfile)
      {
        Log::debug("co_await file_send, sock_fd_idx=", sock_fd_idx);
        finish_sendfile = co_await file_send_awaitable;
      }

      if (is_success)
      {
        Log::debug("splice send success, sock_fd_idx=", sock_fd_idx, "|file_fd_idx=", web_server_file_fd);
        send_error_occurs = false;
      }
      else
      {
        Log::error("splice send failed, sock_fd_idx=", sock_fd_idx, "|file_fd_idx=", web_server_file_fd);
        send_error_occurs = true;
      }

      if (use_direct_file)
      {
        bool finish_close = false;
        while (!finish_close)
          finish_close = co_await file_close(sock_fd_idx, web_server_file_fd);
      }
      else
      {
        close(web_server_file_fd);
      }
    }

    // 断开连接
    if (send_error_occurs ||
        !request.keep_alive() ||
        (!use_buf_body && !response.keep_alive()) ||
        (use_buf_body && !response_buf.keep_alive()))
    {
      Log::debug("prepare to close socket");
      bool finish_close = false;
      while (!finish_close)
        finish_close = co_await socket_close(sock_fd_idx);
      co_return 0;
    }
  }
}