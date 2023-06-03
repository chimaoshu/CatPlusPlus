#include "io_uring_wrapper.h"

#include <sys/resource.h>

void IOUringWrapper::add_multishot_accept(int listen_fd)
{
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  FORCE_ASSERT(sqe != NULL);

  // 设置SQE
  io_uring_prep_multishot_accept(sqe, listen_fd, &client_addr, &client_len, 0);

  // 设置user_data
  IORequestInfo req_info{.fd = listen_fd, .req_id = 0, .type = ACCEPT};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));

  // 提交
  Log::debug("add multishot accept: listen_fd=", listen_fd);
  io_uring_submit(&ring);
}

bool IOUringWrapper::cancel_socket_accept(int fd)
{
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  if (!sqe)
    return false;

  // man page:
  // If the cancelation is successful, the cqe for
  // the request targeted for cancelation will have
  // been posted by the time submission returns.
  io_uring_prep_cancel_fd(sqe, fd, 0);
  IORequestInfo req_info{.fd = fd, .req_id = 0, .type = CANCEL_ACCPET};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));

  Log::debug("add cancel request");
  io_uring_submit(&ring);

  return true;
}

// 提交recv请求
bool IOUringWrapper::add_recv(int sock_fd_idx, bool poll_first)
{
  // 设置SQE
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  if (sqe == NULL)
    return false;

  io_uring_prep_recv(sqe, sock_fd_idx, NULL, 0, 0);
  sqe->buf_group = 0;
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
bool IOUringWrapper::add_zero_copy_send(int sock_fd_idx, const send_buf_info &info, int req_id, bool zero_copy, bool submit)
{
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  if (sqe == NULL)
    return false;

  // buffer来自write buffer pool
  if (zero_copy && info.buf_id != -1)
  {
    assert(write_buffer_pool_[info.buf_id] == info.buf);
    // 使用IO_LINK时需要加上MSG_WAITALL
    io_uring_prep_send_zc_fixed(sqe, sock_fd_idx, info.buf, info.len, MSG_WAITALL, 0, info.buf_id);
  }
  // buffer不来自write buffer pool
  else if (zero_copy && info.buf_id == -1)
  {
    // 使用IO_LINK时需要加上MSG_WAITALL
    io_uring_prep_send_zc(sqe, sock_fd_idx, info.buf, info.len, MSG_WAITALL, 0);
  }
  // 非zero copy
  else if (!zero_copy)
  {
    io_uring_prep_send(sqe, sock_fd_idx, info.buf, info.len, MSG_WAITALL);
  }

  sqe->flags |= IOSQE_IO_LINK;

  Log::debug("add zero copy send, sock_fd_idx=", sock_fd_idx,
             "|write_buf_id=", info.buf_id);

  // user data
  IORequestInfo req_info{.fd = sock_fd_idx, .req_id = static_cast<int16_t>(req_id), .type = zero_copy ? MULTIPLE_SEND_ZC_SOCKET : MULTIPLE_SEND_SOCKET};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));

  // 提交
  Log::debug("add send request",
             "|sock_fd_idx=", sock_fd_idx,
             "|buf_id=", info.buf_id,
             "|len=", info.len);

  if (submit)
    io_uring_submit(&ring);

  return true;
}

bool IOUringWrapper::add_zero_copy_sendmsg(int sock_fd_idx, const std::list<send_buf_info> &infos,
                                           iovec *sendmsg_iov, bool zero_copy)
{
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  if (sqe == NULL)
    return false;

  struct msghdr msg;
  memset(&msg, 0, sizeof(msg));

  struct iovec *iov = new iovec[infos.size()];
  memset(iov, 0, sizeof(iov));

  int i = 0;
  for (const send_buf_info &info : infos)
  {
    iov[i].iov_base = const_cast<void *>(info.buf);
    iov[i].iov_len = info.len;
    i++;
  }

  msg.msg_iov = iov;
  msg.msg_iovlen = infos.size();

  if (zero_copy)
    io_uring_prep_sendmsg_zc(sqe, sock_fd_idx, &msg, 0);
  else
    io_uring_prep_sendmsg(sqe, sock_fd_idx, &msg, 0);

  // user data
  IORequestInfo req_info{.fd = sock_fd_idx, .req_id = 0, .type = zero_copy ? SENDMSG_ZC_SOCKET : SENDMSG_SOCKET};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));

  // 提交
  Log::debug("add zero copy sendmsg, sock_fd_idx=", sock_fd_idx);
  if (UtilEnv::is_debug_mode())
  {
    std::cout << "size: " << infos.size() << std::endl;
    for (const send_buf_info &info : infos)
      std::cout << "buf=" << info.buf << "buf_id=" << info.buf_id << "len=" << info.len << std::endl;
  }

  io_uring_submit(&ring);

  // 5.4以后的内核，提交之后就iov可以删除了：IORING_FEAT_SUBMIT_STABLE
  // 但是只有当IORING_SETUP_SQPOLL没开启时才可以
  // 见：https://github.com/axboe/liburing/discussions/676

  // 将iov注册到promise中，在resume时删除。
  sendmsg_iov = iov;
  return true;
}

// 关闭连接（提交close请求）
bool IOUringWrapper::disconnect(int sock_fd_idx)
{
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  if (sqe == NULL)
    return false;

  io_uring_prep_close(sqe, sock_fd_idx);

  IORequestInfo req_info{.fd = sock_fd_idx, .req_id = 0, .type = CLOSE_SOCKET};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));

  io_uring_submit(&ring);
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
void *IOUringWrapper::get_write_buf_by_id(int buf_id) { return write_buffer_pool_[buf_id]; }

void *IOUringWrapper::get_prov_buf(int buf_id) { return read_buffer_pool_[buf_id]; }

// 合并多个send请求
bool IOUringWrapper::multiple_send(int sock_fd_idx, const std::list<send_buf_info> &buf_infos, bool zero_copy)
{
  // 检查可用sqe是否足够
  int sqe_num = io_uring_sq_space_left(&ring);
  if (sqe_num < buf_infos.size())
  {
    Log::debug("io_uring sqe not enough, sock_fd_idx=", sock_fd_idx,
               "|need sqe=", buf_infos.size(),
               "|sqe_num=", sqe_num);
    return false;
  }

  // 提交到io_uring，使用IOSQE_IO_LINK串联起来
  int req_id = 0;
  for (auto it = buf_infos.begin(); it != buf_infos.end(); it++)
  {
    const send_buf_info &buf_info = *it;
    bool submit = (std::next(it) == buf_infos.end());
    FORCE_ASSERT(add_zero_copy_send(sock_fd_idx, buf_info, req_id, zero_copy, submit));
    req_id++;
  }

  return true;
}

IOUringWrapper::IOUringWrapper()
    : max_conn_num_(config::force_get_int("WORKER_MAX_CONN_NUM")),
      io_uring_entries_(config::force_get_int("WORKER_IO_URING_ENTRIES")),
      // 每个连接最多以direct方式打开一个web_file，加100应对各种额外的fd打开开销
      max_fixed_file_num_(max_conn_num_ + 100),
      max_buffer_num_(config::force_get_int("WORKER_MAX_BUF_NUM"))
{
  FORCE_ASSERT(io_uring_entries_ > 0 && io_uring_entries_ > max_conn_num_);
  FORCE_ASSERT(sizeof(IORequestInfo) == sizeof(io_uring_sqe::user_data));

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
      UtilError::error_exit("IORING_FEAT_FAST_POLL not supported in current kernel", false);
    if (!(params.features & IORING_FEAT_SUBMIT_STABLE))
      UtilError::error_exit("IORING_FEAT_SUBMIT_STABLE not supported in current kernel", false);
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
  int ret = io_uring_register_files_sparse(&ring, max_fixed_file_num_);
  if (ret < 0)
    UtilError::error_exit("io_uring_register_files_sparse failed|ret=" + std::to_string(ret), false);

  // 设置打开文件限制
  rlimit limit;
  limit.rlim_cur = 1048576;
  limit.rlim_max = 1048576;
  if (setrlimit(RLIMIT_NOFILE, &limit) == 0)
  {
    if (setrlimit(RLIMIT_NOFILE, &limit) != 0)
      UtilError::error_exit("set max open file limit failed", true);
  }

  // 每个worker的ring都可以注册一个eventfd，用于控制是否shutdown
  // TODO
}

// 提交read请求
// 调用端保证sqe可用
void IOUringWrapper::add_read(int sock_fd_idx, int read_file_fd_idx, const send_buf_info &read_buf, bool fixed_file)
{
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  FORCE_ASSERT(sqe != NULL);

  // 使用fixed buffer
  if (read_buf.buf_id != -1)
  {
    assert(write_buffer_pool_[read_buf.buf_id] == read_buf.buf);
    io_uring_prep_read_fixed(sqe, read_file_fd_idx, const_cast<void *>(read_buf.buf), read_buf.len, 0, read_buf.buf_id);
  }
  // 使用temp buffer
  else
  {
    io_uring_prep_read(sqe, read_file_fd_idx, const_cast<void *>(read_buf.buf), read_buf.len, 0);
  }

  if (fixed_file)
    sqe->flags |= IOSQE_FIXED_FILE;

  // user data
  IORequestInfo req_info{.fd = sock_fd_idx, .req_id = 0, .type = READ_FILE};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));

  // 提交
  io_uring_submit(&ring);
}

bool IOUringWrapper::open_file_direct(int sock_fd_idx, const std::string &path, mode_t mode)
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
// 优先使用写缓冲池中的buffer（当文件大小小于一页且有可用的缓冲区时）
bool IOUringWrapper::read_file(int sock_fd_idx, int read_file_fd_idx, int file_size,
                               send_buf_info &read_buf, bool fixed_file)
{
  if (io_uring_sq_space_left(&ring) < 1)
  {
    Log::debug("read_file sqe not enough, sock_fd_idx=", sock_fd_idx);
    return false;
  }

  // 当文件大小小于一页且有可用的缓冲区时，使用缓冲池中的buffer
  int page_num = (file_size + page_size - 1) / page_size;
  if (page_num == 1 && unused_write_buffer_id_.size() > 0)
  {
    // get buf
    int buf_id = unused_write_buffer_id_.front();
    unused_write_buffer_id_.pop();
    read_buf = send_buf_info{buf_id, write_buffer_pool_[buf_id], file_size};
    add_read(sock_fd_idx, read_file_fd_idx, read_buf, fixed_file);
  }
  // 无法使用缓冲池中的buffer，直接开辟内存
  else
  {
    read_buf = send_buf_info{-1, new char[file_size], file_size};
    add_read(sock_fd_idx, read_file_fd_idx, read_buf, fixed_file);
  }
  return true;
}

// sendfile zero copy
// sqe不足返回false，成功提交返回false
bool IOUringWrapper::sendfile(int sock_fd_idx, int file_fd_idx, int chunk_size, int *sqe_num, int *pipefd)
{
  // 设置PIPE容量(root)
  static bool can_set_pipe_size = true;
  int pipe_capacity = config::force_get_int("SENDFILE_CHUNK_SIZE");
  if (can_set_pipe_size)
  {
    int ret = fcntl(pipefd[0], F_SETPIPE_SZ, pipe_capacity);
    if (ret == -1)
    {
      can_set_pipe_size = false;
      if (UtilEnv::is_production_mode())
        Log::error("set pipe size failed, check if the process runs as root");
      else if (UtilEnv::is_debug_mode())
        UtilError::error_exit("set pipe size failed, check if the process runs as root", true);
    }
    else
    {
      ret = fcntl(pipefd[1], F_SETPIPE_SZ, pipe_capacity);
      if (ret == -1)
      {
        can_set_pipe_size = false;
        if (UtilEnv::is_production_mode())
          Log::error("set pipe size failed, check if the process runs as root");
        else if (UtilEnv::is_debug_mode())
          UtilError::error_exit("set pipe size failed, check if the process runs as root", true);
      }
      else
      {
        can_set_pipe_size = true;
      }
    }
  }

  // 设置失败，使用默认的容量
  if (!can_set_pipe_size)
  {
    // 获取PIPE容量
    pipe_capacity = fcntl(pipefd[0], F_GETPIPE_SZ);
    if (pipe_capacity == -1)
      UtilError::error_exit("get pipe size failed", true);
  }

  int blocks = chunk_size / pipe_capacity;
  int remain = chunk_size % pipe_capacity;

  // 计算需要使用的sqe数量
  int need_sqe_num = (blocks + (int)(remain > 0)) * 2;
  *sqe_num = need_sqe_num;

  int available_sqe_entries = io_uring_sq_space_left(&ring);
  FORCE_ASSERT(io_uring_entries_ > need_sqe_num);
  if (available_sqe_entries < need_sqe_num)
  {
    Log::debug("sendfile failed, sqe not enough, sock_fd_idx=", sock_fd_idx,
               "|left_sqe_num=", available_sqe_entries,
               "|need_sqe_num=", need_sqe_num);
    return false;
  }

  int16_t req_id = 0;
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

    // file-->pipe, input(web file) is fixed file
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    last_sqe = sqe;
    FORCE_ASSERT(sqe != NULL);
    io_uring_prep_splice(sqe, file_fd_idx, -1, pipefd[1], -1, nbytes, SPLICE_F_MOVE | SPLICE_F_MORE);
    sqe->splice_flags |= SPLICE_F_FD_IN_FIXED;
    sqe->flags |= IOSQE_IO_LINK;

    // IORequestInfo
    IORequestInfo req_info{.fd = sock_fd_idx, .req_id = req_id, .type = SEND_FILE};
    memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));
    req_id++;

    // pipe-->socket, output(socket) is not fixed file
    struct io_uring_sqe *sqe2 = io_uring_get_sqe(&ring);
    last_sqe = sqe2;
    FORCE_ASSERT(sqe2 != NULL);
    io_uring_prep_splice(sqe2, pipefd[0], -1, sock_fd_idx, -1, nbytes, SPLICE_F_MOVE | SPLICE_F_MORE);
    sqe2->flags |= IOSQE_IO_LINK;

    // IORequestInfo
    IORequestInfo req_info2{.fd = sock_fd_idx, .req_id = req_id, .type = SEND_FILE};
    memcpy(&sqe2->user_data, &req_info2, sizeof(IORequestInfo));
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
  Log::debug("submit close file request, sock_fd_idx=", sock_fd_idx, "|file_fd_idx=", file_fd_idx);

  IORequestInfo req_info{.fd = sock_fd_idx, .req_id = 0, .type = CLOSE_FILE};
  memcpy(&sqe->user_data, &req_info, sizeof(IORequestInfo));

  io_uring_submit(&ring);
  return true;
}

send_buf_info IOUringWrapper::get_write_buf_or_create()
{
  send_buf_info buf_info;
  // 缓冲区无可拥，则new一块
  if (unused_write_buffer_id_.size() == 0)
  {
    buf_info.buf = new char[page_size];
    buf_info.buf_id = -1;
    buf_info.len = page_size;
  }
  // 缓冲区可拥，则获取一块
  else
  {
    int buf_id = unused_write_buffer_id_.front();
    unused_write_buffer_id_.pop();

    buf_info.buf = write_buffer_pool_[buf_id];
    buf_info.buf_id = buf_id;
    buf_info.len = page_size;
  }
  return buf_info;
}