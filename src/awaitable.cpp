#include "awaitable.h"

// socket_recv
bool socket_recv_awaitable::await_ready() { return false; }

void socket_recv_awaitable::await_suspend(ConnectionTaskHandler h)
{
  // struct成员赋值
  handler = h;
  io_worker = h.promise().io_worker;

  // 提交recv请求
  Log::debug("add recv at sock_fd_idx=", sock_fd_idx);
  submit_success = io_worker->add_recv(sock_fd_idx, h, true);
  // FORCE_ASSERT(submit_success);

  if (!submit_success)
  {
    io_worker->add_io_resume_task(sock_fd_idx);
    Log::error("sqe not enough while adding recv, add back to queue, sock_fd_idx=", sock_fd_idx);
  }

  // 设置IO状态
  h.promise().current_io = IOType::RECV_SOCKET;
}

// 返回是否已经完成
// false-读取未完成，需要重新co_await调用
// true-读取已经完成/解析出错，无需重新co_await调用
bool socket_recv_awaitable::await_resume()
{
  auto &promise = handler.promise();

  // 确认resume正确，恢复当前IO状态
  FORCE_ASSERT(promise.current_io == IOType::RECV_SOCKET);
  promise.current_io = IOType::NONE;

  if (!submit_success)
  {
    Log::debug("socket recv resume, but submit failed on suspend, retry again, sock_fd_idx=", sock_fd_idx);
    return false;
  }

  struct coroutine_cqe &cqe = promise.cqe;

  // recv success
  if (cqe.res > 0)
  {
    int prov_buf_id = cqe.flags >> 16;
    Log::debug("prov_buf_id=", prov_buf_id, " is used to recv");

#ifndef PRODUCTION
    std::cout << std::string_view((char *)io_worker->get_prov_buf(prov_buf_id), cqe.res) << std::flush;
#endif

    // 把buffer丢进parser
    error_code err;
    parser.put(boost::asio::buffer(io_worker->get_prov_buf(prov_buf_id), cqe.res), err);

    // 由于parser内部会拷贝buffer内容，所以这里可以将占用的prov_buf进行回收
    io_worker->retrive_prov_buf(prov_buf_id);

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
    io_worker->try_extend_prov_buf();
    assert(!(cqe.flags & IORING_CQE_F_MORE));
    // 重试
    return false;
  }
  else if (cqe.res == -EAGAIN)
  {
    Log::error("recv failed with EAGAIN, cqe.res=-11, retry it, sock_fd_idx=", sock_fd_idx);
    return false;
  }
  else if (cqe.res == -ECONNRESET)
  {
    Log::error("client unexpertedly close the connection, sock_fd_idx=", sock_fd_idx,
               "|cqe.res=", cqe.res);
    // 不重试，直接退出
    return true;
  }
  // 其他失败
  else
  {
    Log::error("error recv socket_fd_idx=", sock_fd_idx, " ret=", cqe.res);
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
  io_worker = h.promise().io_worker;
  submit_success = io_worker->send_to_client(sock_fd_idx, serialized_buffers,
                                             buf_infos, h, used_write_buf);

  // 将本协程放入io_queue
  // 等待后续resume()后，由协程再次co_await该对象
  if (!submit_success)
  {
    Log::error("buffer or entries not enough whiling sending socket, add back to io_resume_task_queue, sock_fd_idx=",
               sock_fd_idx);
    io_worker->add_io_resume_task(sock_fd_idx);
  }

  h.promise().current_io = IOType::SEND_SOCKET;
}

// 返回是否已经完成
// false-写入未完成，需要重新co_await调用
// true-写入已经完成
bool socket_send_awaitable::await_resume()
{
  // 要么buffer或者entries不够没完成，要么占用了buffer然后完成
  assert((!submit_success && buf_infos.empty()) || (submit_success && !buf_infos.empty()));

  auto &promise = handler.promise();

  // 确认状态正确，并还原
  FORCE_ASSERT(promise.current_io == IOType::SEND_SOCKET);
  promise.current_io = IOType::NONE;
  promise.send_sqe_complete.clear();
  Log::debug("resume, clear send_sqe_complete, sock_fd_idx=", sock_fd_idx);

  // 未成功发送，需要重新调用，直接返回false
  if (!submit_success)
  {
    Log::debug("socket send resume, but submit failed on suspend, retry again, sock_fd_idx=", sock_fd_idx);
    return false;
  }

  struct coroutine_cqe &cqe = promise.cqe;

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
      io_worker->retrive_write_buf(buf_info.buf_id);
    else
      delete (char *)const_cast<void *>(buf_info.buf);
  }
  return true;
}

// 提交close请求
bool socket_close_awaitable::await_ready() { return false; }

void socket_close_awaitable::await_suspend(ConnectionTaskHandler h)
{
  handler = h;
  auto &promise = h.promise();

  // IO状态
  promise.current_io = IOType::CLOSE_SOCKET;

  // 关闭连接
  Worker *io_worker = promise.io_worker;
  submit_success = io_worker->disconnect(sock_fd_idx, h);
  // FORCE_ASSERT(submit_success);

  if (!submit_success)
  {
    io_worker->add_io_resume_task(sock_fd_idx);
    Log::error("sqe not enough while closing socket, add back to queue, sock_fd_idx=", sock_fd_idx);
  }

  Log::debug("submit socket close IO request, sock_fd_idx=", sock_fd_idx);
}

bool socket_close_awaitable::await_resume()
{
  auto &promise = handler.promise();

  // 检查状态
  FORCE_ASSERT(promise.current_io == IOType::CLOSE_SOCKET);
  promise.current_io = IOType::NONE;

  if (!submit_success)
  {
    Log::debug("socket close resume, but submit failed on suspend, retry again, sock_fd_idx=", sock_fd_idx);
    return false;
  }

  struct coroutine_cqe &cqe = promise.cqe;
  promise.io_worker->connections_.erase(sock_fd_idx);

  if (cqe.res < 0)
    Log::error("close failed with cqe->res=", std::to_string(cqe.res), false);
  else
    Log::debug("socket closed, sock_fd_idx=", sock_fd_idx);
  return true;
}

// add current coroutine to work-stealing queue
// 将当前协程添加到ws队列（本地满了就加global），可以被其他线程偷窃
bool add_process_task_to_wsq_awaitable::await_ready() { return false; }
void add_process_task_to_wsq_awaitable::await_suspend(ConnectionTaskHandler h)
{
  handler = h;
  Worker *io_worker = h.promise().io_worker;
  io_worker->add_process_task(h);
}
void add_process_task_to_wsq_awaitable::await_resume() {}

// add current coroutine to io_worker private io task queue
// 其他worker偷窃协程，处理完process()任务后，将协程的执行权交还给io_worker
bool add_io_task_back_to_io_worker_awaitable::await_ready() { return false; }
bool add_io_task_back_to_io_worker_awaitable::await_suspend(
    ConnectionTaskHandler h)
{
  Worker *io_worker = h.promise().io_worker;
  Worker *process_worker = h.promise().process_worker;

  // 同一个worker，不用挂起了，直接继续执行
  if (io_worker->get_worker_id() == process_worker->get_worker_id())
  {
    assert(io_worker == process_worker);
    Log::debug(
        "the io_worker and process_worker is the same worker, no need to "
        "suspend, worker_id=",
        io_worker->get_worker_id());
    // 恢复协程
    return false;
  }
  // 不同worker，需要加入队列
  else
  {
    io_worker->add_io_resume_task(sock_fd_idx);
    Log::debug(
        "add io_task back to io_worker=", io_worker->get_worker_id(),
        ", process_worker=", process_worker->get_worker_id());
    // 挂起协程
    return true;
  }
}
void add_io_task_back_to_io_worker_awaitable::await_resume() {}

bool file_open_awaitable::await_ready() { return false; }
void file_open_awaitable::await_suspend(ConnectionTaskHandler h)
{
  handler = h;
  io_worker = h.promise().io_worker;

  submit_success = io_worker->open_file_direct(sock_fd_idx, path, mode);
  // FORCE_ASSERT(submit_success);

  if (!submit_success)
  {
    io_worker->add_io_resume_task(sock_fd_idx);
    Log::error("sqe not enough while opening socket, add back to queue, sock_fd_idx=", sock_fd_idx);
  }

  h.promise().current_io = OPEN_FILE;
}
bool file_open_awaitable::await_resume()
{
  // 恢复状态
  FORCE_ASSERT(handler.promise().current_io == IOType::OPEN_FILE);
  handler.promise().current_io = NONE;

  if (!submit_success)
  {
    Log::debug("file open resume, but submit failed on suspend, retry again, sock_fd_idx=", sock_fd_idx);
    return false;
  }

  auto cqe = handler.promise().cqe;
  if (cqe.res < 0)
  {
    Log::error("open file failed, sock_fd_idx=", sock_fd_idx, "|cqe.res=", cqe.res, "|file_path=", path);
    if (cqe.res == -ENFILE)
      Log::error("registered file space not enough, please set a high value");
    *file_fd_idx = -1;
    return true;
  }
  else
  {
    Log::debug("open file sucess");
    *file_fd_idx = cqe.res;
    return true;
  }
  assert(false);
}

bool file_close_awaitable::await_ready() { return false; }
void file_close_awaitable::await_suspend(ConnectionTaskHandler h)
{
  io_worker = h.promise().io_worker;
  handler = h;

  h.promise().current_io = CLOSE_FILE;

  submit_success = io_worker->close_direct_file(sock_fd_idx, file_fd_idx);
  // FORCE_ASSERT(submit_success);
  if (!submit_success)
  {
    Log::error("sqe not enough while closeing file, add back to queue, sock_fd_idx=", sock_fd_idx);
    io_worker->add_io_resume_task(sock_fd_idx);
  }

  Log::debug("submit file close request, sock_fd_idx=", sock_fd_idx, "|file_fd_idx=", file_fd_idx);
}
bool file_close_awaitable::await_resume()
{
  // 恢复状态
  FORCE_ASSERT(handler.promise().current_io == IOType::CLOSE_FILE);
  handler.promise().current_io = NONE;

  if (!submit_success)
  {
    Log::debug("file close resume, but submit failed on suspend, retry again, sock_fd_idx=", sock_fd_idx);
    return false;
  }

  auto cqe = handler.promise().cqe;
  if (cqe.res < 0)
  {
    Log::error("close file direct failed, sock_fd_idx=", sock_fd_idx, "|file_fd_idx=", file_fd_idx,
               "|cqe.res=", cqe.res);
    UtilError::error_exit("close file failed", false);
  }
  else
  {
    Log::debug("close file direct success, sock_fd_idx=", sock_fd_idx, "|file_fd_idx=", file_fd_idx);
    return true;
  }
  assert(false);
}

bool file_read_awaitable::await_ready() { return false; }
void file_read_awaitable::await_suspend(ConnectionTaskHandler h)
{
  io_worker = h.promise().io_worker;
  handler = h;
  h.promise().current_io = IOType::READ_FILE;
  // 读取文件
  submit_success = io_worker->read_file(sock_fd_idx, read_file_fd_idx, file_size,
                                        used_buffer_id, buf, fixed_file);
  // FORCE_ASSERT(submit_success);

  if (!submit_success)
  {
    Log::error("sqe not enough while reading file, add back to queue, sock_fd_idx=", sock_fd_idx);
    io_worker->add_io_resume_task(sock_fd_idx);
  }
}
// 返回是否重试: true-完成 false-未完成, 需要重试
bool file_read_awaitable::await_resume()
{
  // 恢复状态
  FORCE_ASSERT(handler.promise().current_io == IOType::READ_FILE);
  handler.promise().current_io = NONE;

  if (!submit_success)
  {
    Log::debug("file read resume, but submit failed on suspend, retry again, sock_fd_idx=", sock_fd_idx);
    *read_success = false;
    return false;
  }

  auto cqe = handler.promise().cqe;
  if (cqe.res < 0)
  {
    Log::error("read failed, read_file_fd_idx=", read_file_fd_idx, "|used_buffer_id=", *used_buffer_id, "|buffer=", *buf);
    // retrive buffer
    if (*used_buffer_id == -1)
      delete (char *)*buf;
    else
      io_worker->retrive_prov_buf(*used_buffer_id);
    *read_success = false;
    return true;
  }

  Log::debug("read file finish, sock_fd_idx=", sock_fd_idx,
             "|read_file_fd_idx=", read_file_fd_idx, "|cqe.res=", cqe.res);

  // 此时还不能删除buffer或者回收buffer，因为后续需要使用该buffer进行send操作
  *read_success = true;
  return true;
}

bool file_send_awaitable::await_ready() { return false; }
void file_send_awaitable::await_suspend(ConnectionTaskHandler h)
{
  io_worker = h.promise().io_worker;
  handler = h;
  h.promise().current_io = IOType::SEND_FILE;

  if (!is_pipe_init)
  {
    int ret = pipe(pipefd);
    if (ret == -1)
      UtilError::error_exit("create pipe failed, check open file limit", false);
    is_pipe_init = true;
  }

  submit_success = io_worker->sendfile(sock_fd_idx, read_file_fd_idx, file_size,
                                       h.promise().sendfile_sqe_complete, pipefd, fixed_file);
  // FORCE_ASSERT(submit_success);

  // entires不足，将协程放回队列
  if (!submit_success)
  {
    io_worker->add_io_resume_task(sock_fd_idx);
    Log::error("sqe not enough while sending file, add back to queue, sock_fd_idx=", sock_fd_idx);
  }
}
bool file_send_awaitable::await_resume()
{
  // 恢复状态
  FORCE_ASSERT(handler.promise().current_io == IOType::SEND_FILE);
  handler.promise().current_io = NONE;

  if (!submit_success)
  {
    Log::debug("file send resume, but submit failed on suspend, retry again, sock_fd_idx=", sock_fd_idx);
    return false;
  }

  // close
  close(pipefd[0]);
  close(pipefd[1]);

  // 恢复协程，要么是出错，要么是一大串sendfile的io完成
  // 如果是出错，cqe一定是出错的任务的cqe
  auto cqe = handler.promise().cqe;
  if (cqe.res < 0)
  {
    if (cqe.res == -EPIPE || cqe.res == -ECANCELED)
      Log::error("sendfile failed, client unexpertedly closed connection, sock_fd_idx=", sock_fd_idx,
                 "|file_fd_idx=", read_file_fd_idx,
                 "|cqe.res=", cqe.res);
    else
      Log::error("sendfile failed, sock_fd_idx=", sock_fd_idx,
                 "|file_fd_idx=", read_file_fd_idx,
                 "|cqe.res=", cqe.res);
    *is_success = false;
  }
  else
  {
    Log::debug("sendfile success, sock_fd_idx=", sock_fd_idx,
               "|file_fd_idx=", read_file_fd_idx);
    *is_success = true;
  }
  return true;
}