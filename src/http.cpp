#include "http.h"
#include "awaitable.h"

#include <sys/socket.h>
#include <netinet/tcp.h>

#include <chrono>
#include <filesystem>
#include <sys/sendfile.h>

#include <boost/beast/http/span_body.hpp>
#include <boost/optional/optional_io.hpp>
#include <boost/beast/core/make_printable.hpp>

// 计算耗时
#define CALCULATE_DURATION(func, tag)                                                                   \
  {                                                                                                     \
    auto startTime = std::chrono::steady_clock::now();                                                  \
    func auto endTime = std::chrono::steady_clock::now();                                               \
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count(); \
    std::cout << tag << "(ms):" << duration << std::endl;                                               \
  }

// 设置套接字
#define SET_SOCKRT(fd, arg, status)                                          \
  {                                                                          \
    int flag = status;                                                       \
    if (setsockopt(sock_fd_idx, IPPROTO_TCP, arg, &flag, sizeof(flag)) != 0) \
      UtilError::error_exit("set " #arg " failed", true);                    \
  }

#define OPEN_SOCKET_FEAT(fd, arg) SET_SOCKRT(fd, arg, 1)
#define CLOSE_SOCKET_FEAT(fd, arg) SET_SOCKRT(fd, arg, 0)

// 关闭特性

// 进行IO操作
#define ASYNC_IO(sock_fd_idx, func, result)                          \
  while (true)                                                       \
  {                                                                  \
    Log::debug("co_await " #func " with sock_fd_idx=", sock_fd_idx); \
    result = co_await func;                                          \
                                                                     \
    if (!result.is_submitted)                                        \
      continue;                                                      \
    break;                                                           \
  }

// 关闭连接并退出协程
#define CLOSE_AND_EXIT(sock_fd_idx, result)                       \
  ASYNC_IO(sock_fd_idx, socket_close(sock_fd_idx), result)        \
  if (result.res < 0)                                             \
    Log::error("close socket failed, sock_fd_idx=", sock_fd_idx); \
  co_return 0;

// TODO: awaitable_result重构
#define CLOSE_FILE(fd)                                     \
  if (use_direct_file)                                     \
  {                                                        \
    bool finish_close = false;                             \
    while (!finish_close)                                  \
      finish_close = co_await file_close(sock_fd_idx, fd); \
  }                                                        \
  else                                                     \
  {                                                        \
    close(fd);                                             \
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
      OPEN_SOCKET_FEAT(sock_fd_idx, TCP_QUICKACK);
      finish_read = co_await recv_awaitable;
    }

    // 读取结束，只读到EOF，说明客户端已经关闭
    // 也不用发bad request，直接关闭连接即可
    if (!parser.got_some())
    {
      awaitable_result close_result;
      CLOSE_AND_EXIT(sock_fd_idx, close_result);
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
    bool use_sendfile_to_transfer_body = false;

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
      // 调用开发者定义的process函数
      request = parser.release();
      if (std::holds_alternative<ProcessFuncStringBody>(processor))
      {
        use_buf_body = false;
        std::get<ProcessFuncStringBody>(processor)(request, response, args);
      }
      else if (std::holds_alternative<ProcessFuncBufferBody>(processor))
      {
        use_buf_body = true;
        std::get<ProcessFuncBufferBody>(processor)(request, response_buf, args);
      }
      else
      {
        FORCE_ASSERT(false);
      }
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
      response.prepare_payload();
      use_buf_body = false;
    }

    // 将控制权交还给io_worker
    if (enable_work_stealing)
      co_await add_io_task_back_to_io_worker(sock_fd_idx);

    // process函数要求当前请求以web server方式处理
    if (!bad_request && args.use_web_server)
    {
      // check path
      std::filesystem::path fs_path(args.file_path);

      bool is_regular_file = std::filesystem::is_regular_file(fs_path);
      if (!is_regular_file)
        Log::error("file path is not regular file|path=", args.file_path);

      // get file size
      if (is_regular_file)
        web_server_file_size = std::filesystem::file_size(fs_path);

      // 打开文件，使用direct_file
      if (is_regular_file && use_direct_file)
      {
        // TODO: 非direct file也支持使用异步打开
        auto open_awaitable = file_open(sock_fd_idx, args.file_path, O_RDONLY, &web_server_file_fd);
        while (true)
        {
          Log::debug("co_await open, sock_fd_idx=", sock_fd_idx);
          bool finish_open = co_await open_awaitable;
          if (!finish_open)
            continue;
        }
      }
      // 打开文件，使用普通open
      else if (is_regular_file && !use_direct_file)
      {
        web_server_file_fd = open(args.file_path.c_str(), O_RDONLY);
        if (web_server_file_fd == -1)
          Log::error("open file failed, path=", args.file_path, "|ret=", errno);
      }

      // 文件不合法或打开文件失败
      if (!is_regular_file || web_server_file_fd == -1)
      {
        if (!is_regular_file)
          Log::error("not a regular file, file_path=", args.file_path, "|sock_fd_idx=", sock_fd_idx);
        else
          Log::error("open file failed, file_path=", args.file_path, "|sock_fd_idx=", sock_fd_idx);
        response.set(http::field::content_type, "text/html");
        response.version(request.version());
        response.keep_alive(request.keep_alive());
        response.result(http::status::not_found);
        response.body() = "404 not found";
        response.prepare_payload();
        use_buf_body = false;
      }
      // 打开文件成功
      else
      {
        response.set(http::field::content_type, "text/html");
        response.version(request.version());
        response.keep_alive(request.keep_alive());
        response.result(http::status::ok);
        response.content_length(web_server_file_size);
        use_buf_body = false;
      }

      // web file size大于阈值使用sendfile进行发送
      static int sendfile_threshold = config::force_get_int("SENDFILE_THRESHOLD");
      use_sendfile_to_transfer_body = ((sendfile_threshold != -1) &&
                                       (web_server_file_fd != -1) &&
                                       (web_server_file_size >= sendfile_threshold));

      assert(web_server_file_fd != -1);
      assert(!bad_request);
      assert(args.use_web_server);
    }

    // 序列化response
    // 常规模式下，序列化header与body
    // 但如果使用web_server，则只序列化header，后续另外处理body
    error_code ec;
    std::list<send_buf_info> send_bufs;
    if (use_buf_body)
    {
      http::response_serializer<http::buffer_body> serializer_buf(response_buf);
      ec = co_await serialize<http::response_serializer<http::buffer_body>>(&serializer_buf, send_bufs, args.use_web_server);
    }
    else
    {
      http::response_serializer<http::string_body> serializer(response);
      ec = co_await serialize<http::response_serializer<http::string_body>>(&serializer, send_bufs, args.use_web_server);
    }

    // 序列化出错，直接关闭连接退出协程
    if (ec)
    {
      Log::error(ec.message());
      awaitable_result close_result;
      CLOSE_AND_EXIT(sock_fd_idx, close_result);
    }

    // web_server常规模式，若web文件较小，则完全读取到内存中再发送
    // 目前认为>=4M属于大文件
    bool large_web_file = (web_server_file_size >= 4 * 1048576);
    send_buf_info body_buf_info{.buf_id = -1, .buf = NULL, .len = 0};
    if (args.use_web_server && !use_sendfile_to_transfer_body && !large_web_file)
    {
      // 读取整个web文件
      awaitable_result file_read_result;
      ASYNC_IO(sock_fd_idx, file_read(sock_fd_idx, web_server_file_fd, web_server_file_size, body_buf_info), file_read_result)

      // 读取失败，关闭文件、连接并退出
      if (file_read_result.res < 0)
      {
        // 关闭文件
        CLOSE_FILE(web_server_file_fd);
        awaitable_result close_result;
        CLOSE_AND_EXIT(sock_fd_idx, close_result)
      }

      // 读取成功，且body较小，将body合并到header中
      if (web_server_file_size + send_bufs.back().len <= sysconf(_SC_PAGESIZE))
      {
        send_buf_info &header_buf_info = send_bufs.back();
        FORCE_ASSERT(body_buf_info.len + header_buf_info.len <= sysconf(_SC_PAGESIZE));
        memcpy((char *)header_buf_info.buf + header_buf_info.len, body_buf_info.buf, body_buf_info.len);
        header_buf_info.len += body_buf_info.len;

        CLOSE_FILE(web_server_file_fd)
      }
      // 读取成功，且body无法与header合并，将body独立成一块buffer
      else
      {
        send_bufs.push_back(body_buf_info);
        CLOSE_FILE(web_server_file_fd)
      }
    }

    // 将send_bufs发送给客户端
    // 1、对非web_server模式来说，发送header+body
    // 2、对web_server常规模式来说，发送header或header+body，取决于body能不能塞进header所在的缓存
    // 3、对web_server的sendfile模式来说，发送header
    // 考虑使用sendmsg一次发送
    // for (const send_buf_info &buf_info : send_bufs)
    {
      if (config::force_get_int("TCP_NODELAY"))
        OPEN_SOCKET_FEAT(sock_fd_idx, TCP_NODELAY);

      // 非大文件时使用zero-copy才有优势
      bool use_zero_copy = !large_web_file && !use_sendfile_to_transfer_body;
      awaitable_result send_result;
      ASYNC_IO(sock_fd_idx, socket_send(sock_fd_idx, send_bufs, use_zero_copy), send_result)

      // 发送出错，关闭连接并退出协程
      if (send_result.res < 0)
      {
        // TODO：清理资源的回调函数（如果有的话）
        awaitable_result close_result;
        CLOSE_AND_EXIT(sock_fd_idx, close_result)
      }
      Log::debug("send success, sock_fd_idx=", sock_fd_idx, "send_bufs.size()=", send_bufs.size());
    }

    // 回收body使用的buffer
    if (body_buf_info.buf != NULL)
      co_await retrive_write_buf(body_buf_info);

    // 剩下2种情况需要处理body：
    // 1、webserver常规模式，且web文件过大
    // 2、webserver的sendfile模式

    // 1、webserver常规模式，web文件太大，则分块处理body
    // 选择使用循环读一页、写一页的方式处理
    if (args.use_web_server && !use_sendfile_to_transfer_body && large_web_file)
    {
      // 剩余的需要发送的数据量
      const int page_size = sysconf(_SC_PAGESIZE);
      int remain_size = web_server_file_size;
      assert(remain_size > 0);
      while (remain_size > 0)
      {
        // 需要读取的数据量
        int read_size = std::min(page_size, remain_size);

        // 读取一页
        send_buf_info buf_info;
        awaitable_result file_read_result;
        ASYNC_IO(sock_fd_idx, file_read(sock_fd_idx, web_server_file_fd, read_size, buf_info), file_read_result)

        // 读取失败
        if (file_read_result.res < 0)
        {
          // 关闭文件
          CLOSE_FILE(web_server_file_fd)
          awaitable_result close_result;
          CLOSE_AND_EXIT(sock_fd_idx, close_result)
        }

        // 读取成功，更新剩余字节数量
        remain_size -= read_size;

        // 写一页
        awaitable_result send_result;
        // send_result.res = send(sock_fd_idx, buf_info.buf, buf_info.len, 0);
        ASYNC_IO(sock_fd_idx, socket_send(sock_fd_idx, buf_info, false), send_result)
        // ASYNC_IO(sock_fd_idx, socket_send(sock_fd_idx, buf_info, true), send_result)

        // 写失败
        if (send_result.res < 0)
        {
          // 关闭文件
          CLOSE_FILE(web_server_file_fd)
          awaitable_result close_result;
          CLOSE_AND_EXIT(sock_fd_idx, close_result)
        }
      }
    }

    // 2、sendfile模式，前面只发送了header，现在body部分需要通过sendfile来实现file到socket的零拷贝发送
    else if (args.use_web_server && use_sendfile_to_transfer_body)
    {
      bool sendfile_success = false;

      // 是否使用异步的sendfile（目前需要使用splice实现）
      static bool use_async_sendfile = config::force_get_int("USE_ASYNC_SENDFILE");
      if (use_async_sendfile)
      {
        if (config::force_get_int("TCP_NODELAY"))
          OPEN_SOCKET_FEAT(sock_fd_idx, TCP_NODELAY);
        bool finish_sendfile = false;
        auto file_send_awaitable = file_send(sock_fd_idx, web_server_file_fd,
                                             web_server_file_size, &sendfile_success,
                                             use_direct_file);

        while (!finish_sendfile)
        {
          Log::debug("co_await file_send, sock_fd_idx=", sock_fd_idx);
          finish_sendfile = co_await file_send_awaitable;
        }
      }

      // 使用同步的sendfile，这将阻塞工作线程
      else
      {
        // 使用direct file的话无法使用sendfile
        if (use_direct_file)
          UtilError::error_exit("config error: USE_DIRECT_FILE must be disable if USE_ASYNC_SENDFILE is disable", false);

        off_t offset = 0;
        int remain_bytes = web_server_file_size;
        int sendfile_chunked = sysconf(_SC_PAGE_SIZE) * 16;
        sendfile_success = true;
        while (remain_bytes > 0)
        {
          int ret = sendfile(sock_fd_idx, web_server_file_fd, &offset, sendfile_chunked);
          if (ret == -1)
          {
            sendfile_success = false;
            Log::error("sync sendfile failed, sock_fd_idx=", sock_fd_idx, "|ret=", ret, "|file_fd=", web_server_file_fd);
            break;
          }
          remain_bytes -= ret;
        }
      }

      // 关闭文件
      CLOSE_FILE(web_server_file_fd)

      // sendfile失败，断开连接并退出协程
      if (!sendfile_success)
      {
        awaitable_result close_result;
        CLOSE_AND_EXIT(sock_fd_idx, close_result)
      }
    }

    // TODO: 考虑加个callback函数，或者PorcessFuncArg里面加个变量
    // 可以销毁buffer body使用的buffer

    // 断开连接
    if (
        !request.keep_alive() ||
        (!use_buf_body && !response.keep_alive()) ||
        (use_buf_body && !response_buf.keep_alive()))
    {
      awaitable_result close_result;
      CLOSE_AND_EXIT(sock_fd_idx, close_result)
    }
  }
}

// TODO: 重复性代码换成宏