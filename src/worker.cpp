#include "worker.h"

void Worker::add_multishot_accept(int listen_fd) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  FORCE_ASSERT(sqe != NULL);

  // 设置SQE
  io_uring_prep_multishot_accept_direct(sqe, listen_fd, &client_addr,
                                        &client_len, 0);

  // 设置user_data
  IORequestInfo req_info{.fd = listen_fd, .need_resume = true, .type = ACCEPT};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));

  // 提交
  Log::debug("add multishot accept: listen_fd=", listen_fd);
  io_uring_submit(&ring);
}

// 提交recv请求
void Worker::add_recv(int sock_fd_idx, ConnectionTask::CoroutineHandler handler,
                      bool poll_first) {
  // 设置SQE
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  FORCE_ASSERT(sqe != NULL);
  io_uring_prep_recv(sqe, sock_fd_idx, NULL, 0, 0);
  sqe->buf_group = 0;
  sqe->flags |= IOSQE_FIXED_FILE;
  sqe->flags |= IOSQE_BUFFER_SELECT;
  sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;

  // 设置user_data
  IORequestInfo req_info{.fd = sock_fd_idx, .need_resume = true, .type = RECV};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));

  // 提交
  io_uring_submit(&ring);
}

// 提交send_zc请求
void Worker::add_zero_copy_send(int sock_fd_idx,
                                ConnectionTask::CoroutineHandler handler,
                                std::list<int> used_buffer_id, int msg_len) {
  // 循环准备SQE，使用IOSQE_IO_LINK串联起来
  int remain_bytes = msg_len;
  for (auto buf_id : used_buffer_id) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    FORCE_ASSERT(sqe != NULL);

    // 填充sqe
    int buf_size = std::min(remain_bytes, page_size);
    io_uring_prep_send_zc_fixed(sqe, sock_fd_idx, write_buffer_pool_[buf_id],
                                buf_size, MSG_WAITALL, 0, buf_id);
    sqe->flags |= IOSQE_FIXED_FILE;

    // 长度大于1则需要链接，以保证先后顺序，最后一个不设置，表示链接结束
    if (used_buffer_id.size() > 1 && buf_id != used_buffer_id.back())
      sqe->flags |= IOSQE_IO_LINK;

    // 请求信息——最后一个sqe的user_data需要need_resume_coroutine=true
    IORequestInfo req_info{.fd = sock_fd_idx,
                           .need_resume = (buf_id == used_buffer_id.back()),
                           .type = SEND};
    memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));
  }

  // 提交
  Log::debug("add send request, sock_fd_idx=", sock_fd_idx,
             "|used_buffer_id.size()=", used_buffer_id.size());
  io_uring_submit(&ring);
}

// 关闭连接（提交close请求）
void Worker::disconnect(int sock_fd_idx,
                        ConnectionTask::CoroutineHandler handler) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  FORCE_ASSERT(sqe != NULL);

  io_uring_prep_close_direct(sqe, sock_fd_idx);
  IORequestInfo req_info{.fd = sock_fd_idx, .need_resume = true, .type = CLOSE};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));
}

// 扩展写缓存池
void Worker::extend_write_buffer_pool(int extend_buf_num) {
  // 扩展相应数量的write buffer
  for (int i = 0; i < extend_buf_num; i++) {
    // 新建
    void *buf = new char[page_size];
    // 入队列、缓存池
    write_buffer_pool_.push_back(buf);
    int buf_id = write_buffer_pool_.size() - 1;
    unused_write_buffer_id_.push(buf_id);
    // 注册到io_uring
    iovec iov{.iov_base = buf, .iov_len = (size_t)page_size};
    io_uring_register_buffers_update_tag(&ring, buf_id, &iov, 0, 1);
  }
  Log::debug("extend write buffer num=", extend_buf_num);
}

// 回收prov_buf
void Worker::retrive_prov_buf(int prov_buf_id) {
  io_uring_buf_ring_add(buf_ring_, read_buffer_pool_[prov_buf_id], page_size,
                        prov_buf_id, io_uring_buf_ring_mask(max_buffer_num_),
                        0);
  io_uring_buf_ring_advance(buf_ring_, 1);
  Log::debug("recycle id=", prov_buf_id, " provide buffer to ring buf");
}

void Worker::retrive_write_buf(int buf_id) {
  unused_write_buffer_id_.push(buf_id);
  Log::debug("recycle id=", buf_id, " write buf");
}

// 添加prov_buf
bool Worker::try_extend_prov_buf() {
  if (read_buffer_pool_.size() < max_buffer_num_) {
    // 新建内存
    void *new_buf = new char[page_size];
    read_buffer_pool_.push_back(new_buf);

    // 添加到ring buf
    int prov_buf_id = read_buffer_pool_.size() - 1;
    io_uring_buf_ring_add(buf_ring_, new_buf, page_size, prov_buf_id,
                          io_uring_buf_ring_mask(max_buffer_num_), 0);

    // 提交
    io_uring_buf_ring_advance(buf_ring_, 1);
    Log::debug("extend id=", prov_buf_id, " provide buffer to ring buf");
    return true;
  }
  return false;
}

// 获取buf
void *Worker::get_write_buf(int buf_id) { return write_buffer_pool_[buf_id]; }
void *Worker::get_prov_buf(int buf_id) { return read_buffer_pool_[buf_id]; }

void Worker::send_to_client(
    int sock_fd_idx, std::list<boost::asio::const_buffer> &serialized_buffers,
    std::list<int> &used_buffer_id, ConnectionTask::CoroutineHandler h,
    bool &finish_send) {
  // 计算所需buffer字节数与所需页数
  int msg_len = 0;
  for (auto buf : serialized_buffers) {
    msg_len += buf.size();
  }
  int page_num = (msg_len + page_size - 1) / page_size;

  // buffer不足，尝试扩展
  if (unused_write_buffer_id_.size() < page_num) {
    // 可以扩展的buffer数量，不能超过max_buffer_num
    int extend_buf_num = std::min(max_buffer_num_ - write_buffer_pool_.size(),
                                  page_num - unused_write_buffer_id_.size());
    extend_write_buffer_pool(extend_buf_num);
  }

  // buffer足够，将serializer产生的buffer拷贝到wirte_buffer中
  if (unused_write_buffer_id_.size() >= page_num) {
    int dest_start_pos = 0, src_start_pos = 0, buf_id = -1;
    auto it = serialized_buffers.begin();
    while (true) {
      // 需要取下一块serialzed_buffer
      int src_buf_remain_bytes = it->size() - src_start_pos;
      assert(src_buf_remain_bytes >= 0);
      if (src_buf_remain_bytes == 0) {
        it++;
        // 若已全部拷贝则退出循环
        if (it == serialized_buffers.end()) break;
        // 更新remain bytes
        src_buf_remain_bytes = it->size();
        src_start_pos = 0;
      }

      // 需要取下一块write buffer
      int dest_buf_remain_bytes = page_size - dest_start_pos;
      assert(dest_buf_remain_bytes >= 0);
      if (buf_id == -1 || dest_buf_remain_bytes == 0) {
        buf_id = unused_write_buffer_id_.front();
        unused_write_buffer_id_.pop();
        dest_buf_remain_bytes = page_size;
        dest_start_pos = 0;
        used_buffer_id.push_back(buf_id);
        Log::debug("use write buffer id=", buf_id);
      }

      // 需要拷贝的字节数
      int bytes_to_copy = std::min(src_buf_remain_bytes, dest_buf_remain_bytes);

      // 进行拷贝
      memcpy((char *)write_buffer_pool_[buf_id] + dest_start_pos,
             (char *)it->data() + src_start_pos, bytes_to_copy);
      dest_start_pos += bytes_to_copy;
      src_start_pos += bytes_to_copy;
    }
    // 提交到io_uring，使用IOSQE_IO_LINK串联起来
    add_zero_copy_send(sock_fd_idx, h, used_buffer_id, msg_len);
    finish_send = true;
    return;
  }

  // buffer不能扩展或者扩展后仍然不足，需要重新co_await调用该对象
  if (unused_write_buffer_id_.size() < page_num) {
    // 将本协程放入private_queue（涉及io_uring的提交均由private queue完成）
    // 等待后续resume()后，由协程再次co_await该对象
    add_io_resume_task(sock_fd_idx);
    finish_send = false;
    return;
  }

  // 不会到达这里
  assert(false);
}

int Worker::get_worker_id() { return self_worker_id_; }

void Worker::add_process_task(ConnectionTask::CoroutineHandler h) {
  if (ws_process_task_queue.push(h))
    Log::debug("add a process task to wsq");
  else
    FORCE_ASSERT(service_->global_queue.push(h));
}

void Worker::add_io_resume_task(int sock_fd_idx) {
  private_io_task_queue.push(sock_fd_idx);
}

bool Worker::try_get_io_task_queue(ConnectionTask::CoroutineHandler &h) {
  // 空返回false
  if (private_io_task_queue.empty()) return false;
  // 非空设置协程句柄
  int sock_fd_idx = private_io_task_queue.front();
  private_io_task_queue.pop();
  // h = connections_[sock_fd_idx].handler;
  h = connections_.at(sock_fd_idx).handler;
  return true;
}

void Worker::handle_accept(const struct io_uring_cqe *cqe) {
  int sock_fd_idx = cqe->res;
  // accept成功
  if (sock_fd_idx >= 0) {
    // 函数前置声明
    ConnectionTask handle_http_request(
        int sock_fd_idx, std::function<int(http::request<http::string_body> &,
                                           http::response<http::string_body> &)>
                             processor);

    // fd记录还在，又accept了一个相同的fd
    if (connections_.count(sock_fd_idx)) {
      UtilError::error_exit(
          "accept a unclosed socket, this should not happen, sock_fd_idx=" +
              std::to_string(sock_fd_idx),
          false);
    }

    // 创建协程、添加连接
    connections_.emplace(sock_fd_idx,
                         handle_http_request(sock_fd_idx, processor_));
    Log::debug("accept new connection, sock_fd_idx=", sock_fd_idx);

    // 设置worker
    // 后续该协程的所有IO操作都通过此worker完成，使用fixed file加速
    connections_.at(sock_fd_idx).handler.promise().io_worker = this;
    add_io_resume_task(sock_fd_idx);
  }
  // accept错误
  else {
    Log::error("accept failed cqe->res=", cqe->res);
    // fixed file槽位不足
    if (cqe->res == -ENFILE) {
      UtilError::error_exit(
          "fixed file not enough, please set a higher fixed file num, "
          "connections_num=" +
              std::to_string(connections_.size()),
          false);
    }

    // 检查IORING_CQE_F_MORE，未设置说明出现了一些错误
    if (!(cqe->flags & IORING_CQE_F_MORE)) {
      Log::error(
          "some error occured and multishot-accept is "
          "terminated, worker_id:",
          self_worker_id_, " cqe->res=", cqe->res);
#ifdef PRODUCTION
      // 生产环境下，重新添加回去
      add_multishot_accept(listen_fd_);
#else
      UtilError::error_exit("multishot-accept is terminated", false);
#endif
    }
  }
}

Worker::Worker(int max_conn_num, const std::string &ip, int port, int worker_id,
               int init_buffer_num, int max_buffer_num,
               std::function<int(http::request<http::string_body> &,
                                 http::response<http::string_body> &)>
                   processor,
               Service *service)
    : max_conn_num_(max_conn_num),
      io_uring_entries_(2 * max_conn_num),
      max_fixed_file_num_(max_conn_num + 50),
      max_buffer_num_(max_buffer_num),
      self_worker_id_(worker_id),
      ws_process_task_queue(10),
      processor_(processor),
      service_(service) {
  FORCE_ASSERT(io_uring_entries_ > 0 && io_uring_entries_ > max_conn_num_);
  FORCE_ASSERT(ws_process_task_queue.is_lock_free());
  FORCE_ASSERT(port > 1023 && port < 65535);
  FORCE_ASSERT(sizeof(IORequestInfo) <= sizeof(io_uring_sqe::user_data));

  if ((max_buffer_num_ & (max_buffer_num_ - 1)) != 0)
    UtilError::error_exit("max_buffer_num must be the power of 2", false);

  // 初始化 socket
  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) UtilError::error_exit("socket failed", true);
  {
    int optval = 1;
    if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEPORT, &optval,
                   sizeof(optval)) < 0)
      UtilError::error_exit("setsockopt failed", true);

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET,
    server_addr.sin_addr.s_addr = inet_addr(ip.c_str());
    server_addr.sin_port = htons(port);

    if (bind(listen_fd_, (struct sockaddr *)&server_addr, sizeof(server_addr)) <
        0)
      UtilError::error_exit("bind failed", true);

    // 限定最大连接数
    if (listen(listen_fd_, max_conn_num) < 0)
      UtilError::error_exit("listen failed", true);
  }

  // 初始化 io_uring
  {
    io_uring_params params;
    memset(&params, 0, sizeof(params));
    params.flags = IORING_SETUP_SQPOLL;
    if (io_uring_queue_init_params(io_uring_entries_, &ring, &params) < 0)
      UtilError::error_exit("io_uring setup failed", true);
    if (!(params.features & IORING_FEAT_FAST_POLL))
      UtilError::error_exit(
          "IORING_FEAT_FAST_POLL not supported in current kernel", false);
  }

  // 初始化 buf ring
  {
    FORCE_ASSERT((max_buffer_num_ & (max_buffer_num_ - 1)) == 0);
    struct io_uring_buf_reg reg = {};

    // 分配内存，按页对齐
    if (posix_memalign((void **)&buf_ring_, page_size,
                       max_buffer_num_ * sizeof(struct io_uring_buf)) != 0)
      UtilError::error_exit("posix_memalign failed", true);

    // 注册 buf ring
    reg.ring_addr = (unsigned long)buf_ring_;
    reg.ring_entries = max_buffer_num_;
    reg.bgid = 0;
    int ret = io_uring_register_buf_ring(&ring, &reg, 0);
    if (ret != 0)
      UtilError::error_exit(
          "io_uring_register_buf_ring failed with" + std::to_string(ret),
          false);

    // 初始化write buffer与read buffer
    read_buffer_pool_.reserve(init_buffer_num);
    write_buffer_pool_.reserve(init_buffer_num);
    io_uring_buf_ring_init(buf_ring_);
    io_uring_register_buffers_sparse(&ring, max_buffer_num_);
    FORCE_ASSERT(init_buffer_num <= max_conn_num_);
    FORCE_ASSERT(init_buffer_num <= max_buffer_num);
    for (int i = 0; i < init_buffer_num; i++) {
      void *new_read_buf = new char[page_size];
      void *new_write_buf = new char[page_size];
      // read buf注册为prov_buf
      read_buffer_pool_.push_back(new_read_buf);
      int buf_id = i;
      io_uring_buf_ring_add(buf_ring_, new_read_buf, page_size, buf_id,
                            io_uring_buf_ring_mask(max_buffer_num_), i);
      // write buf注册为register buf
      write_buffer_pool_.push_back(new_write_buf);
      iovec iov{.iov_base = new_write_buf, .iov_len = (size_t)page_size};
      io_uring_register_buffers_update_tag(&ring, buf_id, &iov, 0, 1);
      unused_write_buffer_id_.push(buf_id);
    }
    // 提交provide buffer
    io_uring_buf_ring_advance(buf_ring_, init_buffer_num);
  }

  // 初始化 register file
  io_uring_register_files_sparse(&ring, max_fixed_file_num_);

  // 每个worker的ring都可以注册一个eventfd，用于控制是否shutdown
  // TODO

  // 准备multishot-accept，并将listen_fd注册到内核，但不提交
  add_multishot_accept(listen_fd_);
}

void Worker::run() {
  // 提交构造函数准备好的accept请求
  while (true) {
    bool have_task = false;
    ConnectionTask::CoroutineHandler handler;
    // 优先处理io task
    if (try_get_io_task_queue(handler)) {
      Log::debug("get a task from private worker_id=", self_worker_id_);
      have_task = true;
    }
    // 从本地ws队列取（process task）
    else if (ws_process_task_queue.pop(handler)) {
      Log::debug("get a task from wsq worker_id=", self_worker_id_);
      handler.promise().process_worker = this;
      have_task = true;
    }
    // 本地队列没有，就去全局队列取
    else if (service_->global_queue.pop(handler)) {
      Log::debug("get a task from global queue worker_id=", self_worker_id_);
      handler.promise().process_worker = this;
      have_task = true;
    }
    // 全局队列没有，就从其他worker的队列偷
    else {
      for (int worker_id = 0; worker_id < service_->worker_num; worker_id++) {
        if (worker_id == self_worker_id_) continue;
        if (service_->workers[worker_id]->ws_process_task_queue.pop(handler)) {
          Log::debug("get a task from worker with worker_id=", worker_id);
          handler.promise().process_worker = this;
          have_task = true;
          break;
        }
      }
    }

    // 有任务
    if (have_task) {
      handler.resume();
      continue;
    }

    // 没任务，阻塞wait_cqe
    struct io_uring_cqe *cqe;
    int head, count = 0;
    // io_uring_wait_cqe(&ring, &cqe); TODO
    __kernel_timespec timeout{.tv_sec = 1, .tv_nsec = 0};
    io_uring_wait_cqe_timeout(&ring, &cqe, &timeout);
    if (!cqe) {
      continue;
    }

    io_uring_for_each_cqe(&ring, head, cqe) {
      count++;

      // IO请求数据
      struct IORequestInfo info;
      memcpy(&info, &cqe->user_data, sizeof(struct IORequestInfo));

      Log::debug("receive cqe, sock_fd_idx=", info.fd, "|type|cqe.res|",
                 (int)info.type, "|", cqe->res);

      IOType io_type = info.type;
      if (io_type != ACCEPT) {
        IOType current_io =
            connections_.at(info.fd).handler.promise().current_io;
        FORCE_ASSERT(current_io == io_type);
      }

      // 根据不同IO类型进行处理
      switch (io_type) {
        case IOType::ACCEPT: {
          handle_accept(cqe);
          break;
        }
        case IOType::RECV: {
          copy_cqe(connections_.at(info.fd).handler.promise().cqe, *cqe);
          add_io_resume_task(info.fd);
          break;
        }
        case IOType::SEND: {
          // need resume或出错都要resume
          bool resume = false;
          // 出错
          if (cqe->res < 0) {
            resume = true;
          }
          // 没出错，并且是最后一个请求
          else if (info.need_resume) {
            // 接收到sendzc的第一个cqe，IORING_CQE_F_MORE表示还会有一个cqe通知表示结束
            // 此时还不能恢复，需要等待收到下一个cqe通知才可以resume
            if (cqe->flags & IORING_CQE_F_MORE) {
              resume = false;
              Log::debug(
                  "last recv cqe, IORING_CQE_F_MORE is set, do not resume, "
                  "wait for notification cqe, sock_fd_idx=",
                  info.fd);
            }
            // 通知请求
            else if (cqe->flags & IORING_CQE_F_NOTIF) {
              resume = true;
              Log::debug("get notification cqe of zero copy send, sock_fd_idx=",
                         info.fd);
            }
            // 没有通知的cqe
            else {
              Log::debug("cqe without notification, sock_fd_idx=", info.fd,
                         " cqe->res=", cqe->res);
              resume = true;
            }
          }
          // 没出错，但不是最后一个请求
          else {
            resume = false;
            Log::debug("not last send cqe, ignore it, sock_fd_idx=", info.fd);
          }

          // 需要resume
          if (resume) {
            copy_cqe(connections_.at(info.fd).handler.promise().cqe, *cqe);
            Log::debug("process send cqe, need to resume, cqe->res=", cqe->res);
            add_io_resume_task(info.fd);
          } else {
            Log::debug("process send cqe and not need to resume");
          }
          break;
        }
        case IOType::CLOSE: {
          copy_cqe(connections_.at(info.fd).handler.promise().cqe, *cqe);
          // add_io_resume_task(info.fd);
          connections_.at(info.fd).handler.resume();
          break;
        }
        // TODO: 优雅退出
        case IOType::SHUTDOWN:
          break;
        default:
          Log::debug("duplicate cqe cqe->res=", cqe->res);
          break;
      }
    }
    io_uring_cq_advance(&ring, count);
  }
}

// socket_recv
bool socket_recv_awaitable::await_ready() { return false; }
void socket_recv_awaitable::await_suspend(ConnectionTask::CoroutineHandler h) {
  // struct成员赋值
  handler = h;
  io_worker = h.promise().io_worker;

  // 提交recv请求
  Log::debug("add recv at sock_fd_idx=", sock_fd_idx);
  io_worker->add_recv(sock_fd_idx, h, true);

  // 设置IO状态
  h.promise().current_io = IOType::RECV;
}
// 返回是否已经完成
// false-读取未完成，需要重新co_await调用
// true-读取已经完成/解析出错，无需重新co_await调用
bool socket_recv_awaitable::await_resume() {
  auto &promise = handler.promise();
  struct coroutine_cqe &cqe = promise.cqe;

  // 确认resume正确，恢复当前IO状态
  FORCE_ASSERT(promise.current_io == IOType::RECV);
  promise.current_io = IOType::NONE;

  // recv success
  if (cqe.res > 0) {
    int prov_buf_id = cqe.flags >> 16;
    Log::debug("prov_buf_id=", prov_buf_id, " is used to recv");

    // 把buffer丢进parser
    error_code err;
    parser.put(
        boost::asio::buffer(io_worker->get_prov_buf(prov_buf_id), cqe.res),
        err);
    if (err) {
      Log::error("parse http request error", err.message());
      return true;
    }

    // 由于parser内部会拷贝buffer内容，所以这里可以将占用的prov_buf进行回收
    io_worker->retrive_prov_buf(prov_buf_id);

    // 数据没有读完，需要再次读取
    bool recv_finished;
    if (cqe.flags & IORING_CQE_F_SOCK_NONEMPTY) {
      recv_finished = false;
    }
    // 数据读完了
    else {
      // 是chunked-encoding，若还未done，则需要继续读取
      if (parser.chunked()) {
        recv_finished = parser.is_done();
      }
      // 不是chunked，无论是否is_done()，都不应该继续读取了，根据协议，非chunked形式需要一次性发送
      // 如果is_done()=false，说明是bad request
      else {
        if (parser.need_eof()) {
          parser.put_eof(err);
          if (err) {
            Log::error("parser put eof error", err.message());
          }
        }
        recv_finished = true;
      }
    }
    return recv_finished;
  }
  // EOF，说明客户端已经关闭
  else if (cqe.res == 0) {
    // 没有收到任何其他消息，直接受到eof，说明关闭连接了，不再处理
    if (!parser.got_some()) {
      return true;
    }

    // parser有数据，则设置eof
    error_code err;
    parser.put_eof(err);
    if (err) {
      Log::error("put eof error", err.message());
    }

    // 不再重试
    return true;
  }
  // recv失败，原因是prov_buf不够用
  else if (cqe.res == -ENOBUFS) {
    Log::debug("recv failed for lack of provide buffer");
    // 扩展内存(如果未到上限)
    io_worker->try_extend_prov_buf();
    assert(!(cqe.flags & IORING_CQE_F_MORE));
    // 重试
    return false;
  }
  // 其他失败
  else {
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
void socket_send_awaitable::await_suspend(ConnectionTask::CoroutineHandler h) {
  handler = h;
  io_worker = h.promise().io_worker;
  io_worker->send_to_client(sock_fd_idx, serialized_buffers, used_buffer_id, h,
                            finish_send);
  h.promise().current_io = IOType::SEND;
}
// 返回是否已经完成
// false-写入未完成，需要重新co_await调用
// true-写入已经完成
bool socket_send_awaitable::await_resume() {
  // 要么buffer不够没完成，要么占用了buffer然后完成
  assert(!finish_send || !used_buffer_id.empty());

  auto &promise = handler.promise();
  struct coroutine_cqe &cqe = promise.cqe;

  // 确认状态正确，并还原
  FORCE_ASSERT(promise.current_io == IOType::SEND);
  promise.current_io = IOType::NONE;

  // 未成功发送，直接返回
  if (!finish_send) {
    return false;
  }

  // 发送成功，回收内存
  if (cqe.res >= 0) {
    for (auto buf_id : used_buffer_id) {
      io_worker->retrive_write_buf(buf_id);
    }
  }
  // 其他错误直接放弃，直接disconnect
  else if (cqe.res < 0) {
    Log::debug("send failed with cqe->res=", cqe.res);
    send_error_occurs = true;
  }
  return true;
}

// 提交close请求
bool socket_close_awaitable::await_ready() { return false; }
void socket_close_awaitable::await_suspend(ConnectionTask::CoroutineHandler h) {
  handler = h;
  auto &promise = h.promise();

  // IO状态
  promise.current_io = IOType::CLOSE;

  // 关闭连接
  Worker *io_worker = promise.io_worker;
  io_worker->disconnect(sock_fd_idx, h);
  Log::debug("submit socket close IO request, sock_fd_idx=", sock_fd_idx);
}
void socket_close_awaitable::await_resume() {
  auto &promise = handler.promise();
  struct coroutine_cqe &cqe = promise.cqe;

  // 检查状态
  FORCE_ASSERT(promise.current_io == IOType::CLOSE);
  promise.current_io = IOType::NONE;

  promise.io_worker->connections_.erase(sock_fd_idx);

  if (cqe.res < 0)
    UtilError::error_exit(
        "close failed with cqe->res=" + std::to_string(cqe.res), false);
  Log::debug("socket closed, sock_fd_idx=", sock_fd_idx);
}

// add current coroutine to work-stealing queue
// 将当前协程添加到ws队列（本地满了就加global），可以被其他线程偷窃
bool add_process_task_to_wsq_awaitable::await_ready() { return false; }
void add_process_task_to_wsq_awaitable::await_suspend(
    ConnectionTask::CoroutineHandler h) {
  handler = h;
  Worker *io_worker = h.promise().io_worker;
  io_worker->add_process_task(h);
}
void add_process_task_to_wsq_awaitable::await_resume() {}

// add current coroutine to io_worker private io task queue
// 其他worker偷窃协程，处理完process()任务后，将协程的执行权交还给io_worker
bool add_io_task_back_to_io_worker_awaitable::await_ready() { return false; }
bool add_io_task_back_to_io_worker_awaitable::await_suspend(
    ConnectionTask::CoroutineHandler h) {
  Worker *io_worker = h.promise().io_worker;
  Worker *process_worker = h.promise().process_worker;

  // 同一个worker，不用挂起了，直接继续执行
  if (io_worker->get_worker_id() == process_worker->get_worker_id()) {
    assert(io_worker == process_worker);
    Log::debug(
        "the io_worker and process_worker is the same worker, no need to "
        "suspend, worker_id=",
        io_worker->get_worker_id());
    // 恢复协程
    return false;
  }
  // 不同worker，需要加入队列
  else {
    io_worker->add_io_resume_task(sock_fd_idx);
    Log::debug("add io_task back to io_worker=", io_worker->get_worker_id(),
               ", process_worker=", process_worker->get_worker_id());
    // 挂起协程
    return true;
  }
}
void add_io_task_back_to_io_worker_awaitable::await_resume() {}

ConnectionTask handle_http_request(
    int sock_fd_idx, std::function<int(http::request<http::string_body> &,
                                       http::response<http::string_body> &)>
                         processor) {
  while (true) {
    // 读取数据
    http::request_parser<http::string_body> parser;
    parser.eager(true);
    bool finish_read = false;
    auto recv_awaitable = socket_recv(sock_fd_idx, parser);
    while (!finish_read) {
      Log::debug("co_await recv_awaitable with sock_fd_idx=", sock_fd_idx);
      // 此处会提交recv请求并挂起协程，直到recv完成
      finish_read = co_await recv_awaitable;
    }

    // 读取结束，只读到EOF，说明客户端已经关闭
    // 也不用发bad request，直接关闭连接即可
    if (!parser.got_some()) {
      Log::debug("close socket");
      co_await socket_close(sock_fd_idx);
      co_return 0;
    }

    // 将当前协程挂起，放到work-stealings
    // queue，后续process()操作可由其他worker处理
    // 若被其他worker处理，到了send的时候再挂起，将控制权交还给io-worker，保证IO操作均由同一个worker完成
    // 这样可以利用fixed file加速
    co_await add_process_task_to_wsq();

    // 构造response
    http::response<http::string_body> response;
    http::request<http::string_body> request;

    // 完成解析
    bool bad_request = !parser.is_done();
    if (!bad_request) {
      request = parser.release();
      processor(request, response);
      response.prepare_payload();
    }
    // 解析失败
    else {
      // 构造一个bad request报文
      response.result(http::status::bad_request);
      response.set(http::field::content_type, "text/html");
      response.keep_alive(false);
      response.body() = "bad request: error while parsing http request";
      response.prepare_payload();
    }

    // 序列化response
    std::list<boost::asio::const_buffer> buffers;
    http::response_serializer<http::string_body> sr(response);
    error_code ec;
    do {
      sr.next(ec, [&sr, &buffers](error_code &ec, auto const &buffer) {
        ec.assign(0, ec.category());
        for (auto it = buffer.begin(); it != buffer.end(); it++)
          buffers.push_back(*it);
        sr.consume(boost::asio::buffer_size(buffer));
      });
    } while (!ec && !sr.is_done());

    // 将控制权交还给io_worker
    co_await add_io_task_back_to_io_worker(sock_fd_idx);

    if (ec) {
      Log::debug(ec.message());
      co_await socket_close(sock_fd_idx);
      co_return EPROTO;
    }

    // 返回数据给客户端
    bool finish_send = false, send_error_occurs = false;
    auto awaitable_send = socket_send(sock_fd_idx, buffers, send_error_occurs);
    while (!finish_send) {
      Log::debug("co_await awaitable_send with sock_fd_idx=", sock_fd_idx);
      finish_send = co_await awaitable_send;
    }

    // 断开连接
    if (send_error_occurs || !request.keep_alive() || !response.keep_alive()) {
      Log::debug("close socket");
      co_await socket_close(sock_fd_idx);
      co_return 0;
    }
  }
}

Service::Service(int worker_num, int max_conn_num, const std::string &ip,
                 int port, int init_buffer_num, int max_buffer_num,
                 std::function<int(http::request<http::string_body> &,
                                   http::response<http::string_body> &)>
                     http_handler)
    : worker_num(worker_num), global_queue(100) {
  // 初始化worker
  FORCE_ASSERT(worker_num > 0);
  for (int i = 0; i < worker_num; i++) {
    workers.reserve(worker_num);
    Worker *new_worker = new Worker(max_conn_num, ip, port, i, init_buffer_num,
                                    max_buffer_num, http_handler, this);
    workers.push_back(new_worker);
  }
}

Service::~Service() {
  // 往eventfd里面写东西
  // TODO
  for (auto &thread : threads) thread.join();
  for (Worker *worker : workers) delete worker;
}

void Service::start() {
  for (int i = 0; i < worker_num; i++) {
    std::thread worker(&Worker::run, workers[i]);
    threads.push_back(std::move(worker));
  }

  bool shutdown = false;
  std::string input;
  while (true) {
    if (input == "shutdown") break;
  }
}

// 拷贝cqe
void copy_cqe(struct coroutine_cqe &dest, struct io_uring_cqe &src) {
  dest.res = src.res;
  dest.flags = src.flags;
  dest.user_data = src.user_data;
}

socket_recv_awaitable socket_recv(
    int sock_fd_idx, http::request_parser<http::string_body> &parser) {
  return socket_recv_awaitable{.sock_fd_idx = sock_fd_idx, .parser = parser};
}

socket_send_awaitable socket_send(int sock_fd_idx,
                                  std::list<boost::asio::const_buffer> &buffers,
                                  bool &send_error_occurs) {
  return socket_send_awaitable{.sock_fd_idx = sock_fd_idx,
                               .send_error_occurs = send_error_occurs,
                               .serialized_buffers = buffers};
}

socket_close_awaitable socket_close(int sock_fd_idx) {
  return socket_close_awaitable{.sock_fd_idx = sock_fd_idx};
}

add_process_task_to_wsq_awaitable add_process_task_to_wsq() {
  return add_process_task_to_wsq_awaitable{};
}

add_io_task_back_to_io_worker_awaitable add_io_task_back_to_io_worker(
    int sock_fd_idx) {
  return add_io_task_back_to_io_worker_awaitable{.sock_fd_idx = sock_fd_idx};
}