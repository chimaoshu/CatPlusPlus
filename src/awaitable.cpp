#include "awaitable.h"

// socket_recv
bool socket_recv_awaitable::await_ready() { return false; }

void socket_recv_awaitable::await_suspend(ConnectionTaskHandler h)
{
  // struct成员赋值
  handler = h;
  net_io_worker = h.promise().net_io_worker;

  // 提交recv请求
  Log::debug("add recv at sock_fd_idx=", sock_fd_idx);
  net_io_worker->add_recv(sock_fd_idx, h, true);

  // 设置IO状态
  h.promise().current_io = IOType::RECV;
}

// 返回是否已经完成
// false-读取未完成，需要重新co_await调用
// true-读取已经完成/解析出错，无需重新co_await调用
bool socket_recv_awaitable::await_resume()
{
  auto &promise = handler.promise();
  struct coroutine_cqe &cqe = promise.cqe;

  // 确认resume正确，恢复当前IO状态
  FORCE_ASSERT(promise.current_io == IOType::RECV);
  promise.current_io = IOType::NONE;

  // recv success
  if (cqe.res > 0)
  {
    int prov_buf_id = cqe.flags >> 16;
    Log::debug("prov_buf_id=", prov_buf_id, " is used to recv");

    // 把buffer丢进parser
    error_code err;
    parser.put(
        boost::asio::buffer(net_io_worker->get_prov_buf(prov_buf_id), cqe.res),
        err);

    // 由于parser内部会拷贝buffer内容，所以这里可以将占用的prov_buf进行回收
    net_io_worker->retrive_prov_buf(prov_buf_id);

    if (err)
    {
      Log::error("parse http request error", err.message());
      return true;
    }

    // 数据没有读完，需要再次读取
    bool recv_finished;
    if (cqe.flags & IORING_CQE_F_SOCK_NONEMPTY)
    {
      recv_finished = false;
    }
    // 数据读完了
    else
    {
      // 是chunked-encoding，若还未done，则需要继续读取
      if (parser.chunked())
      {
        recv_finished = parser.is_done();
      }
      // 不是chunked，无论是否is_done()，都不应该继续读取了，根据协议，非chunked形式需要一次性发送
      // 如果is_done()=false，说明是bad request
      else
      {
        if (parser.need_eof())
        {
          parser.put_eof(err);
          if (err)
          {
            Log::error("parser put eof error", err.message());
          }
        }
        recv_finished = true;
      }
    }
    return recv_finished;
  }
  // EOF，说明客户端已经关闭
  else if (cqe.res == 0)
  {
    // 没有收到任何其他消息，直接受到eof，说明关闭连接了，不再处理
    if (!parser.got_some())
      return true;

    // parser有数据，则设置eof
    error_code err;
    parser.put_eof(err);
    if (err)
      Log::error("put eof error", err.message());

    // 不再重试
    return true;
  }
  // recv失败，原因是prov_buf不够用
  else if (cqe.res == -ENOBUFS)
  {
    Log::debug("recv failed for lack of provide buffer");
    // 扩展内存(如果未到上限)
    net_io_worker->try_extend_prov_buf();
    assert(!(cqe.flags & IORING_CQE_F_MORE));
    // 重试
    return false;
  }
  // 其他失败
  else
  {
    Log::error("error recv socket_fd_idx=", sock_fd_idx, " ret=", cqe.res);
    return false;
#ifndef PRODUCTION
    std::terminate();
#endif
    // 不重试，直接退出
    return true;
  }
}

// socket_send
bool socket_send_awaitable::await_ready() { return false; }

void socket_send_awaitable::await_suspend(ConnectionTaskHandler h)
{
  handler = h;
  net_io_worker = h.promise().net_io_worker;
  net_io_worker->send_to_client(sock_fd_idx, serialized_buffers,
                                buf_infos, h, send_submitted,
                                read_file_buf);
  // 记录占用资源，若协程中途销毁，可对资源进行回收

  h.promise().current_io = IOType::SEND;
}

// 返回是否已经完成
// false-写入未完成，需要重新co_await调用
// true-写入已经完成
bool socket_send_awaitable::await_resume()
{
  // 要么buffer不够没完成，要么占用了buffer然后完成
  assert((!send_submitted && buf_infos.empty()) || (send_submitted && !buf_infos.empty()));

  auto &promise = handler.promise();
  struct coroutine_cqe &cqe = promise.cqe;

  // 确认状态正确，并还原
  FORCE_ASSERT(promise.current_io == IOType::SEND);
  promise.current_io = IOType::NONE;
  promise.recv_sqe_complete.clear();
  Log::debug("resume, clear recv_sqe_complete, sock_fd_idx=", sock_fd_idx);

  // 未成功发送，需要重新调用，直接返回false
  if (!send_submitted)
    return false;

  // 发送成功，回收内存
  if (cqe.res >= 0)
  {
    Log::debug("send success, sock_fd_idx=", sock_fd_idx);
    send_error_occurs = false;
  }
  // 其他错误直接放弃重试，直接disconnect
  else if (cqe.res < 0)
  {
    Log::debug("send failed with cqe->res=", cqe.res);
    send_error_occurs = true;
  }

  // 清理用于提交write请求的内存
  for (auto &buf_info : buf_infos)
  {
    if (buf_info.buf_id != -1)
      net_io_worker->retrive_write_buf(buf_info.buf_id);
    else
      delete (char *)const_cast<void *>(buf_info.buf);
  }

  // 清理process使用的内存
  for (void *buf : buffer_to_delete)
    delete (char *)buf;

  return true;
}

// 提交close请求
bool socket_close_awaitable::await_ready() { return false; }

void socket_close_awaitable::await_suspend(ConnectionTaskHandler h)
{
  handler = h;
  auto &promise = h.promise();

  // IO状态
  promise.current_io = IOType::CLOSE;

  // 关闭连接
  Worker *net_io_worker = promise.net_io_worker;
  net_io_worker->disconnect(sock_fd_idx, h);
  Log::debug("submit socket close IO request, sock_fd_idx=", sock_fd_idx);
}

void socket_close_awaitable::await_resume()
{
  auto &promise = handler.promise();
  struct coroutine_cqe &cqe = promise.cqe;

  // 检查状态
  FORCE_ASSERT(promise.current_io == IOType::CLOSE);
  promise.current_io = IOType::NONE;

  promise.net_io_worker->connections_.erase(sock_fd_idx);

  if (cqe.res < 0)
    Log::error("close failed with cqe->res=", std::to_string(cqe.res), false);
  else
    Log::debug("socket closed, sock_fd_idx=", sock_fd_idx);
}

// add current coroutine to work-stealing queue
// 将当前协程添加到ws队列（本地满了就加global），可以被其他线程偷窃
bool add_process_task_to_wsq_awaitable::await_ready() { return false; }

void add_process_task_to_wsq_awaitable::await_suspend(ConnectionTaskHandler h)
{
  handler = h;
  Worker *net_io_worker = h.promise().net_io_worker;
  net_io_worker->add_process_task(h);
}

void add_process_task_to_wsq_awaitable::await_resume() {}

// add current coroutine to net_io_worker private io task queue
// 其他worker偷窃协程，处理完process()任务后，将协程的执行权交还给io_worker
bool add_io_task_back_to_io_worker_awaitable::await_ready() { return false; }

bool add_io_task_back_to_io_worker_awaitable::await_suspend(
    ConnectionTaskHandler h)
{
  Worker *net_io_worker = h.promise().net_io_worker;
  Worker *process_worker = h.promise().process_worker;

  // 同一个worker，不用挂起了，直接继续执行
  if (net_io_worker->get_worker_id() == process_worker->get_worker_id())
  {
    assert(net_io_worker == process_worker);
    Log::debug(
        "the net_io_worker and process_worker is the same worker, no need to "
        "suspend, worker_id=",
        net_io_worker->get_worker_id());
    // 恢复协程
    return false;
  }
  // 不同worker，需要加入队列
  else
  {
    net_io_worker->add_io_resume_task(sock_fd_idx);
    Log::debug(
        "add io_task back to net_io_worker=", net_io_worker->get_worker_id(),
        ", process_worker=", process_worker->get_worker_id());
    // 挂起协程
    return true;
  }
}

void add_io_task_back_to_io_worker_awaitable::await_resume() {}

bool file_read_awaitable::await_ready() { return false; }

void file_read_awaitable::await_suspend(ConnectionTaskHandler h)
{
  net_io_worker = h.promise().net_io_worker;
  handler = h;
  h.promise().current_io = IOType::READ;
  // 读取文件
  net_io_worker->read_file(sock_fd_idx, read_file_fd, used_buffer_id, buf);
}

// 返回是否成功: true-读取成功 false-读取失败
bool file_read_awaitable::await_resume()
{
  // 恢复状态
  FORCE_ASSERT(handler.promise().current_io == IOType::READ);
  handler.promise().current_io = NONE;

  auto cqe = handler.promise().cqe;
  if (cqe.res < 0)
  {
    Log::error("read failed, read_file_fd=", read_file_fd, "|used_buffer_id=", used_buffer_id, "|buffer=", *buf);
    // retrive buffer
    if (used_buffer_id == -1)
      delete (char *)*buf;
    else
      net_io_worker->retrive_prov_buf(used_buffer_id);
    return false;
  }

  bytes_num = cqe.res;
  Log::debug("read file finish, sock_fd_idx=", sock_fd_idx,
             "|read_file_fd=", read_file_fd, "|cqe.res=", cqe.res);

  // 此时还不能删除buffer或者回收buffer，因为后续需要使用该buffer进行send操作
  return true;
}

socket_recv_awaitable socket_recv(
    int sock_fd_idx, http::request_parser<http::string_body> &parser)
{
  return socket_recv_awaitable{.sock_fd_idx = sock_fd_idx, .parser = parser};
}

socket_send_awaitable socket_send(
    int sock_fd_idx, std::list<boost::asio::const_buffer> &buffers,
    bool &send_error_occurs, const std::map<const void *, int> &read_file_buf,
    std::list<void *> &buffer_to_delete)
{
  return socket_send_awaitable{.sock_fd_idx = sock_fd_idx,
                               .send_error_occurs = send_error_occurs,
                               .serialized_buffers = buffers,
                               .read_file_buf = read_file_buf,
                               .buffer_to_delete = buffer_to_delete};
}

socket_close_awaitable socket_close(int sock_fd_idx)
{
  return socket_close_awaitable{.sock_fd_idx = sock_fd_idx};
}

add_process_task_to_wsq_awaitable add_process_task_to_wsq()
{
  return add_process_task_to_wsq_awaitable{};
}

add_io_task_back_to_io_worker_awaitable add_io_task_back_to_io_worker(
    int sock_fd_idx)
{
  return add_io_task_back_to_io_worker_awaitable{.sock_fd_idx = sock_fd_idx};
}

file_read_awaitable file_read(int sock_fd_idx, int read_file_fd, void **buf,
                              int &used_buffer_id, int &bytes_num)
{
  return file_read_awaitable{.sock_fd_idx = sock_fd_idx,
                             .read_file_fd = read_file_fd,
                             .buf = buf,
                             .used_buffer_id = used_buffer_id,
                             .bytes_num = bytes_num};
}