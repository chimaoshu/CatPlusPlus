#include "awaitable.h"

#include <sys/socket.h>
#include <netinet/tcp.h>

// socket_recv
bool socket_recv_awaitable::await_ready() { return false; }

void socket_recv_awaitable::await_suspend(ConnectionTaskHandler h)
{
  // struct成员赋值
  handler = h;
  io_worker = h.promise().io_worker;

  // 提交recv请求
  Log::debug("add recv at sock_fd_idx=", sock_fd_idx);
  is_submitted = io_worker->get_io_uring().add_recv(sock_fd_idx, true);
  // FORCE_ASSERT(is_submitted);

  if (!is_submitted)
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

  if (!is_submitted)
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
    std::cout << std::string_view((char *)io_worker->get_io_uring().get_prov_buf(prov_buf_id), cqe.res) << std::flush;
#endif

    // 把buffer丢进parser
    error_code err;
    parser.put(boost::asio::buffer(io_worker->get_io_uring().get_prov_buf(prov_buf_id), cqe.res), err);

    // 由于parser内部会拷贝buffer内容，所以这里可以将占用的prov_buf进行回收
    io_worker->get_io_uring().retrive_prov_buf(prov_buf_id);

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
            Log::error("parser put eof error", err.message());
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
    io_worker->get_io_uring().try_extend_prov_buf();
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

  is_submitted = io_worker->get_io_uring().multiple_send(sock_fd_idx, buf_infos, zero_copy);

  // 将本协程放入io_queue
  // 等待后续resume()后，由协程再次co_await该对象
  if (!is_submitted)
  {
    Log::error("submit send failed because sqe not enough, add back to queue, sock_fd_idx=", sock_fd_idx);
    io_worker->add_io_resume_task(sock_fd_idx);
  }

  // 初始化各请求的cqe信息
  h.promise().multiple_send_cqes.resize(buf_infos.size());
  for (auto &cqe : h.promise().multiple_send_cqes)
    cqe.has_recv = false;

  h.promise().current_io = (zero_copy ? IOType::MULTIPLE_SEND_ZC_SOCKET : IOType::MULTIPLE_SEND_SOCKET);
}

awaitable_result socket_send_awaitable::await_resume()
{
  auto &promise = handler.promise();

  // 确认状态正确，并还原
  FORCE_ASSERT(promise.current_io == (zero_copy ? IOType::MULTIPLE_SEND_ZC_SOCKET : IOType::MULTIPLE_SEND_SOCKET));
  promise.current_io = IOType::NONE;

  // 未成功发送，需要重新调用，直接返回
  if (!is_submitted)
  {
    Log::debug("socket send resume, but submit failed on suspend, retry again, sock_fd_idx=", sock_fd_idx);
    return awaitable_result{
        .is_submitted = false,
        .res = -EAGAIN};
  }

  // 处理多个send请求对应的cqe
  int send_num = 0;
  const std::vector<cqe_status> &cqes = handler.promise().multiple_send_cqes;
  for (const cqe_status &it : cqes)
  {
    const struct coroutine_cqe &cqe = it.cqe;

    // 失败直接返回
    if (cqe.res < 0)
    {
      return awaitable_result{
          .is_submitted = true,
          .res = cqe.res};
    }

    // 成功叠加发送的字节数
    send_num += cqe.res;
  }

  return awaitable_result{
      .is_submitted = true,
      .res = send_num};
}

bool socket_sendmsg_awaitable::await_ready() { return false; }

void socket_sendmsg_awaitable::await_suspend(ConnectionTaskHandler h)
{
  handler = h;
  io_worker = h.promise().io_worker;

  // 设置TCP发送缓冲区的大小，按page_size大小向上取整
  int send_buf_size = 0, page_size = sysconf(_SC_PAGESIZE);
  for (const send_buf_info &buf : buf_infos)
    send_buf_size += buf.len;
  send_buf_size = (send_buf_size + page_size - 1) / page_size * page_size;
  if (!config::force_get_int("USE_DIRECT_FILE") && setsockopt(sock_fd_idx, SOL_SOCKET, SO_SNDBUF, &send_buf_size, sizeof(send_buf_size)) == -1)
    Log::error("set send buf failed|sock_fd_idx=", sock_fd_idx, "|buf_size=", send_buf_size);

  struct iovec *sendmsg_iov = h.promise().sendmsg_iov;
  is_submitted = io_worker->get_io_uring().add_zero_copy_sendmsg(sock_fd_idx, buf_infos, sendmsg_iov, zero_copy);

  // 将本协程放入io_queue
  // 等待后续resume()后，由协程再次co_await该对象
  if (!is_submitted)
  {
    Log::error("submit sendmsg failed because sqe not enough, add back to queue, sock_fd_idx=", sock_fd_idx);
    io_worker->add_io_resume_task(sock_fd_idx);
  }

  h.promise().current_io = (zero_copy ? IOType::SENDMSG_ZC_SOCKET : IOType::SENDMSG_SOCKET);
}

awaitable_result socket_sendmsg_awaitable::await_resume()
{
  auto &promise = handler.promise();

  // 确认状态正确，并还原
  FORCE_ASSERT(promise.current_io == (zero_copy ? IOType::SENDMSG_ZC_SOCKET : IOType::SENDMSG_SOCKET));
  promise.current_io = IOType::NONE;

  // 回收sqe的iov数据
  delete promise.sendmsg_iov;

  // 未成功发送，需要重新调用，直接返回
  if (!is_submitted)
  {
    Log::debug("socket sendmsg resume, but submit failed on suspend, retry again, sock_fd_idx=", sock_fd_idx);
    return awaitable_result{
        .is_submitted = false,
        .res = -EAGAIN};
  }

  struct coroutine_cqe &cqe = promise.cqe;

  if (cqe.res >= 0)
    Log::debug("sendmsg success, sock_fd_idx=", sock_fd_idx);
  else if (cqe.res < 0)
    Log::debug("sendmsg failed|sock_fd_idx=", sock_fd_idx, "|cqe->res=", cqe.res);

  return awaitable_result{
      .is_submitted = true,
      .res = cqe.res};
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
  is_submitted = io_worker->get_io_uring().disconnect(sock_fd_idx);
  // FORCE_ASSERT(is_submitted);

  if (!is_submitted)
  {
    io_worker->add_io_resume_task(sock_fd_idx);
    Log::error("sqe not enough while closing socket, add back to queue, sock_fd_idx=", sock_fd_idx);
  }

  Log::debug("submit socket close IO request, sock_fd_idx=", sock_fd_idx);
}

awaitable_result socket_close_awaitable::await_resume()
{
  auto &promise = handler.promise();
  struct coroutine_cqe &cqe = promise.cqe;

  // 检查状态
  FORCE_ASSERT(promise.current_io == IOType::CLOSE_SOCKET);
  promise.current_io = IOType::NONE;

  if (!is_submitted)
  {
    Log::debug("socket close resume, but submit failed on suspend, retry again, sock_fd_idx=", sock_fd_idx);
    return awaitable_result{
        .is_submitted = false,
        .res = -EAGAIN};
  }

  promise.io_worker->connections_.erase(sock_fd_idx);

  if (cqe.res < 0)
    Log::error("close failed with cqe->res=", std::to_string(cqe.res), false);

  return awaitable_result{
      .is_submitted = true,
      .res = cqe.res};
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
bool add_io_task_back_to_io_worker_awaitable::await_suspend(ConnectionTaskHandler h)
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

  is_submitted = io_worker->get_io_uring().open_file_direct(sock_fd_idx, path, mode);
  // FORCE_ASSERT(is_submitted);

  if (!is_submitted)
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

  if (!is_submitted)
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

  is_submitted = io_worker->get_io_uring().close_direct_file(sock_fd_idx, file_fd_idx);
  // FORCE_ASSERT(is_submitted);
  if (!is_submitted)
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

  if (!is_submitted)
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
    return true;
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
  is_submitted = io_worker->get_io_uring().read_file(sock_fd_idx, read_file_fd_idx, file_size, read_buf);

  if (!is_submitted)
  {
    Log::error("sqe not enough while reading file, add back to queue, sock_fd_idx=", sock_fd_idx);
    io_worker->add_io_resume_task(sock_fd_idx);
  }
}
// 返回是否重试: true-完成 false-未完成, 需要重试
awaitable_result file_read_awaitable::await_resume()
{
  // 恢复状态
  FORCE_ASSERT(handler.promise().current_io == IOType::READ_FILE);
  handler.promise().current_io = NONE;

  // 提交失败，需要重试
  if (!is_submitted)
  {
    Log::debug("file read resume, but submit failed on suspend, retry again, sock_fd_idx=", sock_fd_idx);
    return awaitable_result{
        .is_submitted = false,
        .res = -EAGAIN};
  }

  auto cqe = handler.promise().cqe;

  // 失败，回收缓冲区
  if (cqe.res < 0)
  {
    Log::error("read failed, read_file_fd_idx=", read_file_fd_idx, "|buf_id=", read_buf.buf_id, "|buffer=", read_buf.buf);

    if (read_buf.buf_id == -1)
      delete (char *)read_buf.buf;
    else
      io_worker->get_io_uring().retrive_prov_buf(read_buf.buf_id);
  }
  // 成功
  else
  {
    Log::debug("read file finish, sock_fd_idx=", sock_fd_idx,
               "|read_file_fd_idx=", read_file_fd_idx, "|cqe.res=", cqe.res);
  }

  // 返回结果
  return awaitable_result{
      .is_submitted = true,
      .res = cqe.res};
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

  is_submitted = io_worker->get_io_uring().sendfile(sock_fd_idx, read_file_fd_idx, file_size,
                                                    h.promise().sendfile_sqe_complete, pipefd, fixed_file);
  // FORCE_ASSERT(is_submitted);

  // entires不足，将协程放回队列
  if (!is_submitted)
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

  if (!is_submitted)
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

void copy_serialized_buffer_to_write_buffer(IOUringWrapper &io_uring,
                                            std::list<boost::asio::const_buffer> &src_bufs,
                                            std::list<send_buf_info> &dest_bufs)
{
  int page_size = sysconf(_SC_PAGESIZE);
  FORCE_ASSERT(src_bufs.size() != 0);

  send_buf_info buf_info;                  // 目的地buffer
  auto it = src_bufs.begin();              // 源buffer
  int dest_buf_used = 0, src_buf_used = 0; // 目的地buffer与源buffer已用空间

  while (true)
  {
    int src_buf_remain = it->size() - src_buf_used;
    assert(src_buf_remain >= 0);

    // 源buffer消耗完
    if (src_buf_remain == 0)
    {
      it++;

      // 若全部拷贝完成则退出循环
      if (it == src_bufs.end())
      {
        // 最后一块目的地buffer，如果没用过则回收
        if (dest_buf_used == 0)
        {
          Log::debug("dest_buf_used=0, retrive write buf_id=", buf_info.buf_id);
          if (buf_info.buf_id == -1)
            delete (char *)buf_info.buf;
          else
            io_uring.retrive_write_buf(buf_info.buf_id);
        }
        // 用过，则写入最后一块write_buf，保存len
        else
        {
          buf_info.len = dest_buf_used;
          dest_bufs.push_back(buf_info);
        }
        break;
      }
      // 更新源buffer的剩余空间
      src_buf_remain = it->size();
      src_buf_used = 0;
    }

    // 更新目的地buffer剩余空间
    int dest_buf_remain = page_size - dest_buf_used;
    assert(dest_buf_remain >= 0);

    // 需要取下一块目的地buffer（初始化或者剩余空间消耗完）
    if (buf_info.buf == NULL || dest_buf_remain == 0)
    {
      // 目的地buffer消耗完，保存len
      if (dest_buf_remain == 0)
      {
        buf_info.len = page_size;
        dest_bufs.push_back(buf_info);
      }

      // 换一块新的目的地buffer
      buf_info = io_uring.get_write_buf_or_create();
      dest_buf_remain = page_size;
      dest_buf_used = 0;
      Log::debug("use write buffer id=", buf_info.buf_id);
    }

    // 需要拷贝的字节数
    int bytes_to_copy = std::min(src_buf_remain, dest_buf_remain);

    // 进行拷贝
    memcpy((char *)buf_info.buf + dest_buf_used, (char *)it->data() + src_buf_used, bytes_to_copy);
    dest_buf_used += bytes_to_copy;
    src_buf_used += bytes_to_copy;
  }
}

bool retrive_buffer_awaitable::await_ready() { return false; }
bool retrive_buffer_awaitable::await_suspend(ConnectionTaskHandler h)
{
  if (buf.buf_id == -1)
    delete (char *)buf.buf;
  else
    h.promise().io_worker->get_io_uring().retrive_write_buf(buf.buf_id);
  // 不挂起
  return false;
}
void retrive_buffer_awaitable::await_resume() {}
