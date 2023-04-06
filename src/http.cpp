#include "http.h"
#include "awaitable.h"

#include <filesystem>

#include <boost/optional/optional_io.hpp>
#include <boost/beast/core/make_printable.hpp>

void serialize(http::response_serializer<http::string_body> &sr,
               std::list<boost::asio::const_buffer> &buffers,
               error_code &ec, int &data_to_consume, bool header_only)
{
  // using BodyType = typename std::decay_t<decltype(res)>::body_type;
  data_to_consume = 0;
  bool is_finish = false;

  sr.split(header_only);

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
#ifndef PRODUCTION
              std::cout << make_printable(buffer);
#endif
              // sr.consume(boost::asio::buffer_size(buffer));
            });
  } while (!ec && ((!header_only && !sr.is_done()) || (header_only && !sr.is_header_done())) && !is_finish);
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
    static int enable_work_stealing = config::force_get_int("ENABLE_WORK_STEALING");
    if (enable_work_stealing)
      co_await add_process_task_to_wsq();

    // 构造response
    http::request<http::string_body> request;
    http::response<http::string_body> response;

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
      response.result(http::status::bad_request);
      response.set(http::field::content_type, "text/html");
      response.keep_alive(false);
      response.body() = "bad request: error while parsing http request";
    }

    // 将控制权交还给io_worker
    if (enable_work_stealing)
      co_await add_io_task_back_to_io_worker(sock_fd_idx);

    int web_server_file_fd_idx = -1, web_server_file_size = -1;
    // process函数要求当前请求以web server方式处理
    if (args.use_web_server)
    {
      response.set(http::field::content_type, "text/html");

      // check path
      std::filesystem::path fs_path(args.file_path);
      FORCE_ASSERT(std::filesystem::is_regular_file(fs_path));

      // get file size
      web_server_file_size = std::filesystem::file_size(fs_path);

      // 先打开文件，后续需要先序列化并发送header，然后body使用splice一边读取一边写入socket
      co_await file_open(sock_fd_idx, args.file_path, O_RDONLY, &web_server_file_fd_idx);

      // open success
      if (web_server_file_fd_idx != -1)
      {
        // header
        response.version(request.version());
        response.result(http::status::ok);
        response.content_length(web_server_file_size);
      }
      // open or read fail
      else if (web_server_file_fd_idx == -1)
      {
        Log::error("read file failed, file_path=", args.file_path, "|sock_fd_idx=", sock_fd_idx);
        response.version(request.version());
        response.result(http::status::not_found);
        response.body() = "404 not found";
      }
    }

    // prepare_payload会根据重新设置content-length，web server模式不能使用，否则body会为0
    bool use_splice_to_transfer_body = (web_server_file_fd_idx != -1);
    if (!use_splice_to_transfer_body)
      response.prepare_payload();

    // 序列化response
    error_code ec;
    int data_to_consume = 0;
    std::list<boost::asio::const_buffer> buffers;
    http::response_serializer<http::string_body> serializer(response);
    serialize(serializer, buffers, ec, data_to_consume, use_splice_to_transfer_body);
    if (ec)
    {
      Log::error(ec.message());
      co_await socket_close(sock_fd_idx);
      co_return EPROTO;
    }

    // 返回数据给客户端
    std::map<const void *, int> used_buf;
    bool finish_send = false, send_error_occurs = false;
    auto awaitable_send = socket_send(sock_fd_idx, buffers, send_error_occurs, used_buf);
    while (!finish_send)
    {
      Log::debug("co_await awaitable_send with sock_fd_idx=", sock_fd_idx);
      finish_send = co_await awaitable_send;
    }

    // 清理serializer数据
    serializer.consume(data_to_consume);

    // 如果是splice模式，前面只发送了header，现在body部分需要通过splice来实现file到socket的零拷贝发送
    if (use_splice_to_transfer_body)
    {
      bool is_success = false;
      co_await file_send(sock_fd_idx, web_server_file_fd_idx, web_server_file_size, &is_success);

      if (is_success)
      {
        Log::debug("splce send success, sock_fd_idx=", sock_fd_idx, "|file_fd_idx=", web_server_file_fd_idx);
        send_error_occurs = false;
      }
      else
      {
        Log::error("splice send failed, sock_fd_idx=", sock_fd_idx, "|file_fd_idx=", web_server_file_fd_idx);
        send_error_occurs = true;
      }
      co_await file_close(sock_fd_idx, web_server_file_fd_idx);
    }

    // 断开连接
    if (send_error_occurs || !request.keep_alive() || !response.keep_alive())
    {
      Log::debug("prepare to close socket");
      co_await socket_close(sock_fd_idx);
      co_return 0;
    }
  }
}