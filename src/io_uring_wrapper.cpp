#include "io_uring_wrapper.h"

void IOUringWrapper::add_multishot_accept(int listen_fd)
{
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  FORCE_ASSERT(sqe != NULL);

  static bool use_direct_file = config::force_get_int("USE_DIRECT_FILE");

  // 设置SQE
  if (use_direct_file)
    io_uring_prep_multishot_accept_direct(sqe, listen_fd, &client_addr, &client_len, 0);
  else
    io_uring_prep_multishot_accept(sqe, listen_fd, &client_addr, &client_len, 0);

  // 设置user_data
  IORequestInfo req_info{.fd = listen_fd, .req_id = 0, .type = ACCEPT};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));

  // 提交
  Log::debug("add multishot accept: listen_fd=", listen_fd);
  io_uring_submit(&ring);
}

// 提交recv请求
bool IOUringWrapper::add_recv(int sock_fd_idx, bool poll_first)
{
  // 设置SQE
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  if (sqe == NULL)
    return false;

  static bool use_direct_file = config::force_get_int("USE_DIRECT_FILE");

  io_uring_prep_recv(sqe, sock_fd_idx, NULL, 0, 0);
  sqe->buf_group = 0;
  if (use_direct_file)
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
void IOUringWrapper::add_zero_copy_send(int sock_fd_idx, std::map<int, bool> &send_sqe_complete,
                                        const std::list<send_buf_info> &buf_infos)
{
  // 循环准备SQE，使用IOSQE_IO_LINK串联起来
  int req_id = 0;
  send_sqe_complete.clear();
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

    static bool use_direct_file = config::force_get_int("USE_DIRECT_FILE");
    if (use_direct_file)
      sqe->flags |= IOSQE_FIXED_FILE;

    // 长度大于1则需要链接，以保证先后顺序，最后一个不设置，表示链接结束
    // 设置也无所谓，因为IO_SQE_LINK的影响不会跨越submit
    if (buf_infos.size() > 1 && buf_info.buf_id != buf_infos.back().buf_id)
      sqe->flags |= IOSQE_IO_LINK;

    // 后续用于判断是否各SQE都完成了，才能恢复协程
    send_sqe_complete[req_id] = false;
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
bool IOUringWrapper::disconnect(int sock_fd_idx)
{
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);

  if (sqe == NULL)
    return false;

  static bool use_direct_file = config::force_get_int("USE_DIRECT_FILE");
  if (use_direct_file)
    io_uring_prep_close_direct(sqe, sock_fd_idx);
  else
    io_uring_prep_close(sqe, sock_fd_idx);
  IORequestInfo req_info{.fd = sock_fd_idx, .req_id = 0, .type = CLOSE_SOCKET};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));
  return true;
}

// 扩展写缓存池
void IOUringWrapper::extend_write_buffer_pool(int extend_buf_num)
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
void IOUringWrapper::retrive_prov_buf(int prov_buf_id)
{
  io_uring_buf_ring_add(buf_ring_, read_buffer_pool_[prov_buf_id], page_size,
                        prov_buf_id, io_uring_buf_ring_mask(max_buffer_num_),
                        0);
  io_uring_buf_ring_advance(buf_ring_, 1);
  Log::debug("recycle id=", prov_buf_id, " provide buffer to ring buf");
}

void IOUringWrapper::retrive_write_buf(int buf_id)
{
  unused_write_buffer_id_.push(buf_id);
  Log::debug("recycle id=", buf_id, " write buf");
}

// 添加prov_buf
bool IOUringWrapper::try_extend_prov_buf()
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
void *IOUringWrapper::get_write_buf(int buf_id) { return write_buffer_pool_[buf_id]; }

void *IOUringWrapper::get_prov_buf(int buf_id) { return read_buffer_pool_[buf_id]; }

bool IOUringWrapper::send_to_client(
    int sock_fd_idx, std::list<boost::asio::const_buffer> &serialized_buffers,
    std::list<struct send_buf_info> &buf_infos,
    const std::map<const void *, int> &used_write_buf,
    std::map<int, bool> &send_sqe_complete)
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
    add_zero_copy_send(sock_fd_idx, send_sqe_complete, buf_infos);
    return true;
  }

  // 不会到达这里
  assert(false);
  return false;
}

IOUringWrapper::IOUringWrapper()
    : max_conn_num_(config::force_get_int("WORKER_MAX_CONN_NUM")),
      io_uring_entries_(config::force_get_int("WORKER_IO_URING_ENTRIES")),
      max_fixed_file_num_(max_conn_num_ + 100),
      max_buffer_num_(config::force_get_int("WORKER_MAX_BUF_NUM"))
{
  FORCE_ASSERT(io_uring_entries_ > 0 && io_uring_entries_ > max_conn_num_);
  FORCE_ASSERT(sizeof(IORequestInfo) <= sizeof(io_uring_sqe::user_data));

  if ((max_buffer_num_ & (max_buffer_num_ - 1)) != 0)
    UtilError::error_exit("max_buffer_num must be the power of 2", false);

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
        Log::error("initialize write buffer error: register failed with ret=", ret, "|buf_id=", buf_id);
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
}

// 提交read请求
// 调用端保证sqe可用
void IOUringWrapper::add_read(int sock_fd_idx, int read_file_fd_idx, int file_size,
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

  // TODO：重构使用USE_DIRECT_FILE
  if (fixed_file)
    sqe->flags |= IOSQE_FIXED_FILE;

  // user data
  IORequestInfo req_info{.fd = sock_fd_idx, .req_id = 0, .type = READ_FILE};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));

  // 提交
  io_uring_submit(&ring);
}

bool IOUringWrapper::open_file_direct(int sock_fd_idx, const std::string &path,
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
bool IOUringWrapper::read_file(int sock_fd_idx, int read_file_fd_idx, int file_size,
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
bool IOUringWrapper::sendfile(int sock_fd_idx, int file_fd_idx, int file_size,
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
    // TODO：重构
    if (fixed_file)
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

bool IOUringWrapper::close_direct_file(int sock_fd_idx, int file_fd_idx)
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