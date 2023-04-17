#include "worker.h"

#include <netinet/tcp.h>
#include <string.h>

#include <boost/beast/core/make_printable.hpp>
#include <iostream>
#include <typeinfo>

#include "awaitable.h"
#include "http.h"

void Worker::add_multishot_accept(int listen_fd)
{
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  FORCE_ASSERT(sqe != NULL);

  // 设置SQE
  io_uring_prep_multishot_accept_direct(sqe, listen_fd, &client_addr,
                                        &client_len, 0);

  // 设置user_data
  IORequestInfo req_info{.fd = listen_fd, .req_id = 0, .type = ACCEPT};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));

  // 提交
  Log::debug("add multishot accept: listen_fd=", listen_fd);
  io_uring_submit(&ring);
}

// 提交recv请求
bool Worker::add_recv(int sock_fd_idx, ConnectionTaskHandler handler,
                      bool poll_first)
{
  // 设置SQE
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  if (sqe == NULL)
    return false;

  io_uring_prep_recv(sqe, sock_fd_idx, NULL, 0, 0);
  sqe->buf_group = 0;
  sqe->flags |= IOSQE_FIXED_FILE;
  sqe->flags |= IOSQE_BUFFER_SELECT;
  sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;

  // 设置user_data
  IORequestInfo req_info{.fd = sock_fd_idx, .req_id = 0, .type = RECV_SOCKET};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));

  // 提交
  io_uring_submit(&ring);
  return true;
}

// 提交send_zc请求
// 调用处检查sqe是否足够
void Worker::add_zero_copy_send(int sock_fd_idx, ConnectionTaskHandler handler,
                                const std::list<send_buf_info> &buf_infos)
{
  // 循环准备SQE，使用IOSQE_IO_LINK串联起来
  int req_id = 0;
  handler.promise().send_sqe_complete.clear();
  Log::debug("clear send_sqe_complete, prepare to insert, sock_fd_idx=", sock_fd_idx);
  FORCE_ASSERT(buf_infos.size() > 0);
  for (auto &buf_info : buf_infos)
  {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    FORCE_ASSERT(sqe != NULL);
    Log::debug("add zero copy send, sock_fd_idx=", sock_fd_idx,
               "|write_buf_id=", buf_info.buf_id);

    // 填充sqe
    if (buf_info.buf_id != -1)
    {
      assert(write_buffer_pool_[buf_info.buf_id] == buf_info.buf);
      io_uring_prep_send_zc_fixed(sqe, sock_fd_idx, buf_info.buf, buf_info.len,
                                  MSG_WAITALL, 0, buf_info.buf_id);
    }
    else
    {
      io_uring_prep_send_zc(sqe, sock_fd_idx, buf_info.buf, buf_info.len,
                            MSG_WAITALL, 0);
    }
    sqe->flags |= IOSQE_FIXED_FILE;

    // 长度大于1则需要链接，以保证先后顺序，最后一个不设置，表示链接结束
    // 设置也无所谓，因为IO_SQE_LINK的影响不会跨越submit
    if (buf_infos.size() > 1 && buf_info.buf_id != buf_infos.back().buf_id)
      sqe->flags |= IOSQE_IO_LINK;

    // 后续用于判断是否各SQE都完成了，才能恢复协程
    handler.promise().send_sqe_complete[req_id] = false;
    Log::debug("add send_sqe_complete req_id, sock_fd_idx=", sock_fd_idx,
               "|req_id=", req_id);

    // user data
    IORequestInfo req_info{.fd = sock_fd_idx,
                           .req_id = static_cast<int16_t>(req_id),
                           .type = SEND_SOCKET};
    memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));
    req_id++;
  }

  // 提交
  Log::debug("add send request, sock_fd_idx=", sock_fd_idx,
             "|buf_infos.size()=", buf_infos.size());
  io_uring_submit(&ring);
}

// 关闭连接（提交close请求）
bool Worker::disconnect(int sock_fd_idx, ConnectionTaskHandler handler)
{
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);

  if (sqe == NULL)
    return false;

  io_uring_prep_close_direct(sqe, sock_fd_idx);
  IORequestInfo req_info{.fd = sock_fd_idx, .req_id = 0, .type = CLOSE_SOCKET};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));
  return true;
}

// 扩展写缓存池
void Worker::extend_write_buffer_pool(int extend_buf_num)
{
  // 扩展相应数量的write buffer
  for (int i = 0; i < extend_buf_num; i++)
  {
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
void Worker::retrive_prov_buf(int prov_buf_id)
{
  io_uring_buf_ring_add(buf_ring_, read_buffer_pool_[prov_buf_id], page_size,
                        prov_buf_id, io_uring_buf_ring_mask(max_buffer_num_),
                        0);
  io_uring_buf_ring_advance(buf_ring_, 1);
  Log::debug("recycle id=", prov_buf_id, " provide buffer to ring buf");
}

void Worker::retrive_write_buf(int buf_id)
{
  unused_write_buffer_id_.push(buf_id);
  Log::debug("recycle id=", buf_id, " write buf");
}

// 添加prov_buf
bool Worker::try_extend_prov_buf()
{
  if (read_buffer_pool_.size() < max_buffer_num_)
  {
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

bool Worker::send_to_client(
    int sock_fd_idx, std::list<boost::asio::const_buffer> &serialized_buffers,
    std::list<send_buf_info> &buf_infos, ConnectionTaskHandler h,
    const std::map<const void *, int> &used_write_buf)
{
  // 计算所需buffer字节数与所需页数
  int need_page_num = 0, temp_msg_len = 0;
  for (auto buf : serialized_buffers)
  {
    // 通过`co_await file_read`已经写入write_buf的缓存
    // 前面不足一块buf的以一块buf计算
    if (used_write_buf.find(buf.data()) != used_write_buf.end())
    {
      need_page_num += (temp_msg_len + page_size - 1) / page_size;
      temp_msg_len = 0;
      continue;
    }
    // 不属于used_buf，则积累temp_msg_len
    temp_msg_len += buf.size();
  }
  need_page_num += (temp_msg_len + page_size - 1) / page_size;

  // 检查可用sqe是否足够
  int available_entries = io_uring_sq_space_left(&ring);
  if (available_entries < need_page_num + used_write_buf.size())
  {
    Log::debug("io_uring entries not enough, sock_fd_idx=", sock_fd_idx,
               "|need entries=", need_page_num + used_write_buf.size(),
               "|available_entries=", available_entries);
    return false;
  }

  // buffer不足，尝试扩展
  if (unused_write_buffer_id_.size() < need_page_num)
  {
    // 可以扩展的buffer数量，不能超过max_buffer_num
    int extend_buf_num =
        std::min(max_buffer_num_ - write_buffer_pool_.size(),
                 need_page_num - unused_write_buffer_id_.size());
    extend_write_buffer_pool(extend_buf_num);
  }

  // buffer不能扩展或者扩展后仍然不足，需要重新co_await调用该对象
  if (unused_write_buffer_id_.size() < need_page_num)
    return false;

  // buffer足够，将serializer产生的buffer拷贝到wirte_buffer中
  else if (unused_write_buffer_id_.size() >= need_page_num)
  {
    int dest_start_pos = 0, src_start_pos = 0, buf_id = -1;
    auto it = serialized_buffers.begin();
    while (true)
    {
      // 需要取下一块serialzed_buffer
      int src_buf_remain_bytes = it->size() - src_start_pos;
      assert(src_buf_remain_bytes >= 0);
      if (src_buf_remain_bytes == 0)
      {
        it++;
        // 若该buffer为write_buf（只出现在body，因此不会在一开始出现）
        if (used_write_buf.find(it->data()) != used_write_buf.end())
        {
          // 强行停止上一块write_buf（即使没有写满也要，但是全空则不动），保存buf和len
          if (dest_start_pos != 0)
            buf_infos.emplace_back(buf_id, write_buffer_pool_[buf_id], dest_start_pos);
          // 当前的write_buf直接进
          buf_infos.emplace_back(used_write_buf.at(it->data()), it->data(), it->size());
          // 下一块serialized buf
          it++;
          // 看是否结束，有时候如果使用chunked-encoding，就不会结束，后续还要继续发东西
          if (it == serialized_buffers.end())
            break;
          // 更新src信息
          src_start_pos = 0;

          // 取下一块write_buf
          buf_id = unused_write_buffer_id_.front();
          unused_write_buffer_id_.pop();
          // 更新dest信息
          dest_start_pos = 0;

          // 进入下一轮拷贝
          continue;
        }

        // 若已全部拷贝则退出循环
        if (it == serialized_buffers.end())
        {
          // 最后一块write_buffer，如果没用过则回收回去
          if (dest_start_pos == 0)
          {
            Log::debug("dest_start_pos=0, retrive write buf_id=", buf_id);
            retrive_write_buf(buf_id);
          }
          // 用过，则写入最后一块write_buf，保存buf和len
          else
          {
            buf_infos.emplace_back(buf_id, write_buffer_pool_[buf_id], dest_start_pos);
          }
          break;
        }
        // 更新remain bytes
        src_buf_remain_bytes = it->size();
        src_start_pos = 0;
      }

      // 需要取下一块write buffer
      int dest_buf_remain_bytes = page_size - dest_start_pos;
      assert(dest_buf_remain_bytes >= 0);

      if (buf_id == -1 || dest_buf_remain_bytes == 0)
      {
        // 写完完整的一页buffer，保存buf和len
        if (dest_buf_remain_bytes == 0)
          buf_infos.emplace_back(buf_id, write_buffer_pool_[buf_id], page_size);
        buf_id = unused_write_buffer_id_.front();
        unused_write_buffer_id_.pop();
        dest_buf_remain_bytes = page_size;
        dest_start_pos = 0;
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

    assert(buf_infos.size() == need_page_num + used_write_buf.size());

    // 提交到io_uring，使用IOSQE_IO_LINK串联起来
    add_zero_copy_send(sock_fd_idx, h, buf_infos);
    return true;
  }

  // 不会到达这里
  assert(false);
  return false;
}

int Worker::get_worker_id() { return self_worker_id_; }

void Worker::add_process_task(ConnectionTaskHandler h)
{
  if (ws_process_task_queue.push(h))
    Log::debug("add a process task to wsq");
  else
    FORCE_ASSERT(service_->global_queue.push(h));
}

void Worker::add_io_resume_task(int sock_fd_idx)
{
  FORCE_ASSERT(private_io_task_queue.push(sock_fd_idx));
}

bool Worker::try_get_io_task_queue(ConnectionTaskHandler &h)
{
  // 空返回false
  int sock_fd_idx;
  if (!private_io_task_queue.pop(sock_fd_idx))
    return false;

  // 非空设置协程句柄
  auto it = connections_.find(sock_fd_idx);
  if (it == connections_.end())
  {
    Log::error("get a cqe task with erased fd from io_task_queue, sock_fd_idx=", sock_fd_idx);
    return false;
  }
  h = it->second.handler;
  return true;
}

void Worker::handle_accept(const struct io_uring_cqe *cqe)
{
  int sock_fd_idx = cqe->res;
  // accept成功
  if (sock_fd_idx >= 0)
  {
    // fd记录还在，又accept了一个相同的fd
    if (connections_.count(sock_fd_idx))
    {
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
  else
  {
    Log::error("accept failed cqe->res=", cqe->res);
    // fixed file槽位不足
    if (cqe->res == -ENFILE)
    {
      UtilError::error_exit(
          "fixed file not enough, please set a higher fixed file num, "
          "current connections num=" +
              std::to_string(connections_.size()) +
              " max_fixed_file_num=" + std::to_string(max_fixed_file_num_),
          false);
    }

    // 检查IORING_CQE_F_MORE，未设置说明出现了一些错误
    if (!(cqe->flags & IORING_CQE_F_MORE))
    {
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

Worker::Worker(int worker_id, ProcessFuncType processor, Service *service)
    : max_conn_num_(config::force_get_int("WORKER_MAX_CONN_NUM")),
      io_uring_entries_(config::force_get_int("WORKER_IO_URING_ENTRIES")),
      max_fixed_file_num_(max_conn_num_ + 100),
      max_buffer_num_(config::force_get_int("WORKER_MAX_BUF_NUM")),
      self_worker_id_(worker_id),
      ws_process_task_queue(config::force_get_int("WORKER_WSQ_CAPACITY")),
      private_io_task_queue(128),
      processor_(processor),
      service_(service)
{
  FORCE_ASSERT(io_uring_entries_ > 0 && io_uring_entries_ > max_conn_num_);
  FORCE_ASSERT(ws_process_task_queue.is_lock_free());
  FORCE_ASSERT(sizeof(IORequestInfo) <= sizeof(io_uring_sqe::user_data));

  std::string addrs = config::force_get_str("SERVER_ADDRESS");
  int port = config::force_get_int("LISTEN_PORT");
  FORCE_ASSERT(port > 1023 && port < 65535);

  if ((max_buffer_num_ & (max_buffer_num_ - 1)) != 0)
    UtilError::error_exit("max_buffer_num must be the power of 2", false);

  // 初始化 socket
  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0)
    UtilError::error_exit("socket failed", true);
  {
    int optval = 1;

    bool enable_nodelay = config::force_get_int("TCP_NODELAY");
    int flags = SO_REUSEPORT | SO_REUSEADDR;
    if (enable_nodelay)
      flags |= TCP_NODELAY;
    if (setsockopt(listen_fd_, SOL_SOCKET, flags, &optval, sizeof(optval)) < 0)
      UtilError::error_exit("setsockopt failed", true);

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET,
    server_addr.sin_addr.s_addr = inet_addr(addrs.c_str());
    server_addr.sin_port = htons(port);

    if (bind(listen_fd_, (struct sockaddr *)&server_addr, sizeof(server_addr)) <
        0)
      UtilError::error_exit("bind failed", true);

    // 限定最大连接数
    if (listen(listen_fd_, max_conn_num_) < 0)
      UtilError::error_exit("listen failed", true);
  }

  // 初始化 io_uring
  {
    io_uring_params params;
    memset(&params, 0, sizeof(params));
    params.flags = IORING_SETUP_SQPOLL;
    int ret = io_uring_queue_init_params(io_uring_entries_, &ring, &params);
    if (ret < 0)
      UtilError::error_exit("io_uring setup failed, ret=" + std::to_string(ret), false);
    if (!(params.features & IORING_FEAT_FAST_POLL))
      UtilError::error_exit(
          "IORING_FEAT_FAST_POLL not supported in current kernel", false);
    if (!(params.features & IORING_FEAT_SUBMIT_STABLE))
      UtilError::error_exit(
          "IORING_FEAT_SUBMIT_STABLE not supported in current kernel", false);
    // TODO: check io_uring op code suport
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
    int init_buffer_num = config::force_get_int("WORKER_INIT_BUF_NUM");
    read_buffer_pool_.reserve(init_buffer_num);
    write_buffer_pool_.reserve(init_buffer_num);
    io_uring_buf_ring_init(buf_ring_);
    io_uring_register_buffers_sparse(&ring, max_buffer_num_);
    FORCE_ASSERT(init_buffer_num <= max_conn_num_);
    FORCE_ASSERT(init_buffer_num <= max_buffer_num_);
    for (int i = 0; i < init_buffer_num; i++)
    {
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
      int ret = io_uring_register_buffers_update_tag(&ring, buf_id, &iov, 0, 1);
      // 资源不足
      if (ret == -ENOMEM)
      {
        UtilError::error_exit(
            "ENOMEM: insufficient kernel resouces, reigster"
            " buffer failed. Try using ulimit -l to set a larger memlcok",
            false);
      }
      else if (ret < 0)
      {
        Log::error("initialize write buffer error: register failed with ret=",
                   ret, "|buf_id=", buf_id, "|worker_id=", self_worker_id_);
        UtilError::error_exit("initialize register buffer failed with ret=" + std::to_string(ret), false);
      }
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

// 提交read请求
// 调用端保证sqe可用
void Worker::add_read(int sock_fd_idx, int read_file_fd_idx, int file_size,
                      void **buf, int buf_idx, bool fixed_file)
{
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  FORCE_ASSERT(sqe != NULL);

  // 使用fixed buffer
  if (buf_idx != -1)
  {
    assert(write_buffer_pool_[buf_idx] == *buf);
    io_uring_prep_read_fixed(sqe, read_file_fd_idx, *buf, file_size, 0, buf_idx);
  }
  // 使用temp buffer
  else
  {
    io_uring_prep_read(sqe, read_file_fd_idx, *buf, file_size, 0);
  }

  if (fixed_file)
    sqe->flags |= IOSQE_FIXED_FILE;

  // user data
  IORequestInfo req_info{.fd = sock_fd_idx, .req_id = 0, .type = READ_FILE};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));

  // 提交
  io_uring_submit(&ring);
}

bool Worker::open_file_direct(int sock_fd_idx, const std::string &path,
                              mode_t mode)
{
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  if (sqe == NULL)
    return false;

  io_uring_prep_openat_direct(sqe, -1, path.c_str(), 0, mode, IORING_FILE_INDEX_ALLOC);
  IORequestInfo req_info{.fd = sock_fd_idx, .req_id = 0, .type = OPEN_FILE};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));

  io_uring_submit(&ring);
  return true;
}

// 读取文件
bool Worker::read_file(int sock_fd_idx, int read_file_fd_idx, int file_size,
                       int *used_buffer_id, void **buf, bool fixed_file)
{
  if (io_uring_sq_space_left(&ring) < 1)
  {
    Log::debug("read_file sqe not enough, sock_fd_idx=", sock_fd_idx);
    return false;
  }

  // write_buf可用，且page_num为1，使用write_buf，作为fixed_buffer可以加速
  // io_uring 不支持 readv with fixed buffer
  // 因此只在一块buffer能覆盖的情况下，使用fixed buffer
  // 使用write_buffer_pool是为了后续直接发送给客户端
  int page_num = (file_size + page_size - 1) / page_size;
  if (page_num == 1 && unused_write_buffer_id_.size() > 0)
  {
    // get buf
    *used_buffer_id = unused_write_buffer_id_.front();
    unused_write_buffer_id_.pop();
    *buf = write_buffer_pool_[*used_buffer_id];
    add_read(sock_fd_idx, read_file_fd_idx, file_size, buf, *used_buffer_id, fixed_file);
  }
  // write_buf不够，直接开辟内存
  else
  {
    *used_buffer_id = -1;
    *buf = new char[file_size];
    // -1表示不使用fixed buffer，而使用temp buffer
    add_read(sock_fd_idx, read_file_fd_idx, file_size, buf, -1, fixed_file);
  }
  return true;
}

// sendfile zero copy
// sqe不足返回false，成功提交返回false
bool Worker::sendfile(int sock_fd_idx, int file_fd_idx, int file_size,
                      std::map<int, bool> &sendfile_sqe_complete, int *pipefd,
                      bool fixed_file)
{
  // 设置PIPE容量(root)
  static bool can_set_pipe_size = true;
  int pipe_capacity = config::force_get_int("MAX_PIPE_SIZE");
  if (can_set_pipe_size)
  {
    int ret = fcntl(pipefd[0], F_SETPIPE_SZ, pipe_capacity);
    if (ret == -1)
    {
      can_set_pipe_size = false;
#ifdef PRODUCTION
      Log::error("set pipe size failed, check if the process runs as root");
#else
      UtilError::error_exit("set pipe size failed, check if the process runs as root", true);
#endif
    }
    else
    {
      ret = fcntl(pipefd[1], F_SETPIPE_SZ, pipe_capacity);
      if (ret == -1)
      {
        can_set_pipe_size = false;
#ifdef PRODUCTION
        Log::error("set pipe size failed, check if the process runs as root");
#else
        UtilError::error_exit("set pipe size failed, check if the process runs as root", true);
#endif
      }
      else
      {
        can_set_pipe_size = true;
      }
    }
  }

  // 设置失败
  if (!can_set_pipe_size)
  {
    // 获取PIPE容量
    pipe_capacity = fcntl(pipefd[0], F_GETPIPE_SZ);
    if (pipe_capacity == -1)
      UtilError::error_exit("get pipe size failed", true);
  }

  int blocks = file_size / pipe_capacity;
  int remain = file_size % pipe_capacity;

  // 需要使用的sqe数量
  int need_sqe_entires = (blocks + (remain > 0)) * 2;
  int available_sqe_entries = io_uring_sq_space_left(&ring);
  FORCE_ASSERT(io_uring_entries_ > need_sqe_entires);
  if (available_sqe_entries < need_sqe_entires)
  {
    Log::debug("sendfile failed, sqe not enough, sock_fd_idx=", sock_fd_idx,
               "|left_sqe_num=", available_sqe_entries,
               "|need_sqe_num=", need_sqe_entires);
    return false;
  }

  int16_t req_id = 0;
  sendfile_sqe_complete.clear();
  struct io_uring_sqe *last_sqe = NULL;
  for (int i = 0; i < blocks + 1; i++)
  {
    int nbytes;
    // 非最后一页
    if (i != blocks)
      nbytes = pipe_capacity;
    // 最后一页，有剩余
    else if (remain > 0)
      nbytes = remain;
    // 最后一页，无剩余
    else
      break;

    // file-->pipe, input is fixed file
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    last_sqe = sqe;
    FORCE_ASSERT(sqe != NULL);
    io_uring_prep_splice(sqe, file_fd_idx, -1, pipefd[1], -1, nbytes,
                         SPLICE_F_MOVE | SPLICE_F_MORE);
    if (fixed_file)
      sqe->splice_flags |= SPLICE_F_FD_IN_FIXED;
    sqe->flags |= IOSQE_IO_LINK;
    // IORequestInfo
    IORequestInfo req_info{.fd = sock_fd_idx, .req_id = req_id, .type = SEND_FILE};
    memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));
    // 后续用于判断是否全部SQE均完成，之后才恢复协程
    sendfile_sqe_complete[req_id] = false;
    req_id++;

    // pipe-->socket, output is fixed file
    struct io_uring_sqe *sqe2 = io_uring_get_sqe(&ring);
    last_sqe = sqe2;
    FORCE_ASSERT(sqe2 != NULL);
    io_uring_prep_splice(sqe2, pipefd[0], -1, sock_fd_idx, -1, nbytes,
                         SPLICE_F_MOVE | SPLICE_F_MORE);
    sqe2->flags |= IOSQE_FIXED_FILE;
    sqe2->flags |= IOSQE_IO_LINK;
    // IORequestInfo
    IORequestInfo req_info2{.fd = sock_fd_idx, .req_id = req_id, .type = SEND_FILE};
    memcpy(&sqe2->user_data, &req_info2, sizeof(IORequestInfo));
    // 后续用于判断是否全部SQE均完成，之后才恢复协程
    sendfile_sqe_complete[req_id] = false;
    req_id++;

    Log::debug("add splice, sock_fd_idx=", sock_fd_idx, "|file_fd_idx=", file_fd_idx, "|nbytes=", nbytes);
  }

  // 最后一个不设置IO_LINK, SPLICE_F_MORE
  last_sqe->flags &= (~IOSQE_IO_LINK);
  last_sqe->splice_flags &= (~SPLICE_F_MORE);

  io_uring_submit(&ring);
  return true;
}

bool Worker::close_direct_file(int sock_fd_idx, int file_fd_idx)
{
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  if (sqe == NULL)
    return false;

  io_uring_prep_close_direct(sqe, file_fd_idx);
  Log::debug("submit close file request, sock_fd_idx=", sock_fd_idx,
             "|file_fd_idx=", file_fd_idx);
  IORequestInfo req_info{.fd = sock_fd_idx, .req_id = 0, .type = CLOSE_FILE};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));

  io_uring_submit(&ring);
  return true;
}

void Worker::run()
{
  // 提交构造函数准备好的accept请求
  while (true)
  {
    ConnectionTaskHandler handler;

    struct io_uring_cqe *cqe;
    int head, count = 0;
    // wait for 50ms
    __kernel_timespec timeout{.tv_sec = 0, .tv_nsec = 50000000};
    int ret = io_uring_wait_cqe_timeout(&ring, &cqe, &timeout);
    if (ret < 0 && ret != -ETIME)
    {
#ifndef PRODUCTION
      std::terminate();
#else
      Log::error("wait cqe failed with ret=", ret);
#endif
    }

    // io_uring没有cqe，则从队列中取
    if (!cqe)
    {
      // 优先处理io task
      bool have_task = false;
      if (try_get_io_task_queue(handler))
      {
        Log::debug("get a task from private worker_id=", self_worker_id_);
        have_task = true;
      }
      // 从本地ws队列取（process task）
      else if (ws_process_task_queue.pop(handler))
      {
        Log::debug("get a task from wsq worker_id=", self_worker_id_);
        handler.promise().process_worker = this;
        have_task = true;
      }
      // 本地队列没有，就去全局队列取
      else if (service_->global_queue.pop(handler))
      {
        Log::debug("get a task from global queue worker_id=", self_worker_id_);
        handler.promise().process_worker = this;
        have_task = true;
      }
      // 全局队列没有，就从其他worker的队列偷
      else
      {
        for (int worker_id = 0; worker_id < service_->worker_num; worker_id++)
        {
          if (worker_id == self_worker_id_)
            continue;
          if (service_->workers[worker_id]->ws_process_task_queue.pop(handler))
          {
            Log::debug("get a task from worker with worker_id=", worker_id);
            handler.promise().process_worker = this;
            have_task = true;
            break;
          }
        }
      }

      // 有任务
      if (have_task)
        handler.resume();

      // 进入下一轮循环
      continue;
    }

    // 有就绪cqe，则进行处理
    io_uring_for_each_cqe(&ring, head, cqe)
    {
      count++;

      // IO请求数据
      struct IORequestInfo info;
      memcpy(&info, &cqe->user_data, sizeof(struct IORequestInfo));

      Log::debug("receive cqe, sock_fd_idx=", info.fd, "|type|cqe.res|",
                 (int)info.type, "|", cqe->res);

      IOType io_type = info.type;
      if (io_type != ACCEPT)
      {
        auto it = connections_.find(info.fd);
        if (it == connections_.end())
        {
          Log::error("receive a cqe of closed fd, |cqe.res|type|", cqe->res, "|", (int)info.type);
          continue;
        }
        IOType current_io = it->second.handler.promise().current_io;

        // 正常请求不会出这个错误，客户端不按流程直接关闭连接会出这个错误:
        // 例如connection reset，然后这边close之后，再收到之前还没发完的send请求
        if (current_io != io_type)
        {
          Log::error("skip sqe process, sock_fd_idx=", info.fd,
                     "|current_io=", current_io, "|cqe_io_type=", io_type);
          continue;
        }
      }

      // 根据不同IO类型进行处理
      switch (io_type)
      {
      case IOType::ACCEPT:
      {
        handle_accept(cqe);
        break;
      }
      case IOType::SEND_SOCKET:
      {
        // need resume或出错都要resume
        bool resume = false;
        // 出错
        if (cqe->res < 0)
        {
          resume = true;
        }
        // 没出错
        else
        {
          // 接收到sendzc的第一个cqe，IORING_CQE_F_MORE表示还会有一个cqe通知表示结束
          // 此时还不能恢复，需要等待收到下一个cqe通知才可以resume
          if (cqe->flags & IORING_CQE_F_MORE)
          {
            resume = false;
            Log::debug(
                "recv cqe, IORING_CQE_F_MORE is set, do not resume, "
                "wait for notification cqe, sock_fd_idx=",
                info.fd, "|req_id=", info.req_id);
          }
          // 通知请求
          // else if (cqe->flags & IORING_CQE_F_NOTIF)
          else
          {
            Log::debug("get notification cqe of zero copy send, sock_fd_idx=",
                       info.fd, "|req_id=", info.req_id);
            // 设置recv_id的complete=true
            auto &send_sqe_complete = connections_.at(info.fd).handler.promise().send_sqe_complete;
            send_sqe_complete.at(info.req_id) = true;

            resume = true;
            // 检查是否全部SQE对应的CQE都结束了
            for (auto it : send_sqe_complete)
            {
              if (!it.second)
              {
                resume = false;
                break;
              }
            }
          }
        }

        // 需要resume
        if (resume)
        {
          ConnectionTaskHandler handler = connections_.at(info.fd).handler;
          copy_cqe(handler.promise().cqe, *cqe);
          Log::debug("process send cqe, need to resume, cqe->res=", cqe->res);
          handler.resume();
        }
        else
        {
          Log::debug("process send cqe and not need to resume, sock_fd_idx=",
                     info.fd, "|req_id=", info.req_id);
        }
        break;
      }
      case IOType::SEND_FILE:
      {
        ConnectionTaskHandler handler = connections_.at(info.fd).handler;
        bool resume = false;
        // 成功，只有当全部sqe完成时再resume
        if (cqe->res > 0)
        {
          std::map<int, bool> &sendfile_sqe_complete = handler.promise().sendfile_sqe_complete;
          sendfile_sqe_complete[info.req_id] = true;
          // 检查是否全部cqe都已经收到
          resume = true;
          for (auto &it : sendfile_sqe_complete)
          {
            if (!it.second)
            {
              resume = false;
              break;
            }
          }
        }
        // 出错直接resume
        else
        {
          Log::error("senfile failed, need resume, sock_fd_idx=", info.fd,
                     "|cqe->res=", cqe->res, "|req_id=", info.req_id);
          resume = true;
        }

        if (resume)
        {
          Log::debug("sendfile resume, sock_fd_idx=", info.fd);
          copy_cqe(handler.promise().cqe, *cqe);
          handler.resume();
        }
        else
        {
          Log::debug("get cqe but no need to resume, sock_fd_idx=", info.fd);
        }
        break;
      }
      case IOType::RECV_SOCKET:
      case IOType::CLOSE_SOCKET:
      case IOType::READ_FILE:
      case IOType::OPEN_FILE:
      case IOType::CLOSE_FILE:
      {
        ConnectionTaskHandler handler = connections_.at(info.fd).handler;
        copy_cqe(handler.promise().cqe, *cqe);
        handler.resume();
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

Service::Service(ProcessFuncType http_handler)
    : worker_num(config::force_get_int("WORKER_NUM")), global_queue(128)
{
  // 初始化worker
  FORCE_ASSERT(worker_num > 0);
  for (int i = 0; i < worker_num; i++)
  {
    workers.reserve(worker_num);
    Worker *new_worker = new Worker(i, http_handler, this);
    workers.push_back(new_worker);
  }
}

Service::~Service()
{
  // 往eventfd里面写东西
  // TODO
  for (auto &thread : threads)
    thread.join();
  for (Worker *worker : workers)
    delete worker;
}

void Service::start()
{
  for (int i = 0; i < worker_num; i++)
  {
    std::thread worker(&Worker::run, workers[i]);
    threads.push_back(std::move(worker));
  }

  bool shutdown = false;
  std::string input;
  while (true)
  {
    std::cin >> input;
    if (input == "shutdown")
      // TODO: 优雅退出
      break;
  }
}

// 拷贝cqe
void copy_cqe(struct coroutine_cqe &dest, struct io_uring_cqe &src)
{
  dest.res = src.res;
  dest.flags = src.flags;
  dest.user_data = src.user_data;
}
