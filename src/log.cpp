#include "log.h"

// 格式化为日志格式
void Logger::format_str(const std::string &type, const std::string &msg,
                        std::string &result)
{
  // 获取时间
  time_t now = time(0);
  char *time_cstr = ctime(&now);
  int len = strlen(time_cstr);
  FORCE_ASSERT(len > 1);
  time_cstr[len - 1] = '\0';
  std::string time_str(time_cstr);

  // 格式化
  std::stringstream ss;
  ss << "[" << type << "][" << time_str << "]"
     << "[" << pthread_self() << "]" << msg;
  if (msg.back() != '\n')
    ss << "\n";
  result = ss.str();
}

// 准备提交日志任务，即填充sqe，需后续调用io_uring_submit()一次性提交
// 当该函数在被log函数调用时，只有抢到锁的线程会调用，因此安全
// 当该函数在析构函数调用时，不存在多线程竞争，因此安全
// 因此无需上SQ锁
void Logger::io_uring_prep_submit_task(const LogTask &task)
{
  // 以fixed file、fixed buffer的方式提交sqe
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
  // 考虑小概率情况：SQ全满了，没有位置，则阻塞等待
  if (sqe == NULL)
  {
    // 等待cqe
    struct io_uring_cqe *cqe;
    std::cout << "SQ ring is full, blocked waiting" << std::endl;
    FORCE_ASSERT(submitted_unconsumed_task_num_ != 0);
    {
      std::unique_lock cq_lock(io_uring_cq_mutex_);
      int ret =
          io_uring_wait_cqe_nr(&ring_, &cqe, submitted_unconsumed_task_num_);
      if (ret < 0)
        UtilError::error_exit("io_uring_wait_cqe_nr failed with " +
                                  std::to_string(ret) +
                                  " while submitting task",
                              false);
      // 回收内存
      for_each_cqe_retrieve_buffer();
    }
    // 这里在sqe锁内，当前线程是唯一提交LogTask的线程，所以前面清理出来的sqe空位不会被别的线程抢占
    sqe = io_uring_get_sqe(&ring_);
    FORCE_ASSERT(sqe != NULL);
  }
  // 绝大多数情况，以register buffer方式提交
  if (task.buffer_info->buf_index < max_fixed_buffer_capacity_)
    io_uring_prep_write_fixed(sqe, task.file_info->file_index,
                              task.buffer_info->iov.iov_base, task.bytes_len, 0,
                              task.buffer_info->buf_index);
  // 极少数情况，当可注册的buffer pool扩展达到上限时，使用不注册的buffer提交
  else
    io_uring_prep_write(sqe, task.file_info->file_index,
                        task.buffer_info->iov.iov_base, task.bytes_len, 0);
  sqe->flags |= IOSQE_FIXED_FILE;
  sqe->flags |= IOSQE_IO_DRAIN;
  // 把user_data设置为buffer_info指针
  memcpy(&sqe->user_data, &task.buffer_info, sizeof(BufferInfo *));
}

// 遍历cqe，回收buffer
void Logger::for_each_cqe_retrieve_buffer()
{
  struct io_uring_cqe *cqe;
  unsigned head, count = 0;
  io_uring_for_each_cqe(&ring_, head, cqe)
  {
    count++;
    // 接收透传数据
    BufferInfo *buffer_info;
    memcpy(&buffer_info, &cqe->user_data, sizeof(BufferInfo *));
    // 回收buffer至可用队列
    unused_buffer_.push(buffer_info);
    unused_buffer_num_++;
    submitted_unconsumed_task_num_--;
    // 处理cqe结果
    if (cqe->res < 0)
      std::cout << "io_uring process io failed with buf_index="
                << buffer_info->buf_index << " res=" << cqe->res << std::endl;
  }
  // 标记cqe已经消费
  io_uring_cq_advance(&ring_, count);
}

// 从多种不同渠道尝试获取一块buffer，优先等级：
// 1、已创建buffer池中可用buffer优先（多数情况）
// 2、新建fixed buffer（多数情况）
// 3、新建无法注册的buffer（少数情况）
// 4、阻塞等待cqe回收buffer（极少数情况）
Logger::BufferInfo *Logger::get_buffer()
{
  // 尝试获取一块buffer
  BufferInfo *buffer_info;
  if (!unused_buffer_.pop(buffer_info))
  {
    // 仍然可扩展fixed buffer池
    if (new_buffer_index_ < max_fixed_buffer_capacity_)
    {
      // 无可用buffer，则新增一块
      char *new_buf = new char[max_log_len];
      int buffer_index = new_buffer_index_.fetch_add(1);
      buffer_info = new BufferInfo(new_buf, max_log_len, buffer_index);
      // 注册到io_uring内核
      int ret = io_uring_register_buffers_update_tag(&ring_, buffer_index,
                                                     &buffer_info->iov, 0, 1);
      if (ret < 0)
        UtilError::error_exit(
            "failed to register buffers, " + std::to_string(ret), false);
      std::cout << "extend buffer pool size with buffer index "
                << new_buffer_index_ << std::endl;
    }
    // 少数情况：fixed
    // buffer池已经扩展至上限，不再将buffer注册到io_uring，但仍然开辟新buffer
    else if (new_buffer_index_ < io_uring_entries_)
    {
      std::cout << "registered buffer size has reached the upper limit, extend "
                   "a new unregistered buffer buffer_index= "
                << new_buffer_index_ << std::endl;
      int buffer_index = new_buffer_index_.fetch_add(1);
      // 无可用buffer，则新增一块
      char *new_buf = new char[max_log_len];
      // 提交时检查到new_buffer_index>=max_buffer_capacity_，则不使用fixed
      // buffer提交
      buffer_info = new BufferInfo(new_buf, max_log_len, buffer_index);
    }
    // 级少数情况：内存数量已经等于io_uring_entries数量，继续开辟得到的提升不大
    else
    {
      std::cout << "run out of both registered and unregistered buffer, "
                   "blocked waiting and retrive buffer"
                << std::endl;
      while (!unused_buffer_.pop(buffer_info))
      {
        std::unique_lock cq_lock(io_uring_cq_mutex_);
        struct io_uring_cqe *cqe;
        int ret =
            io_uring_wait_cqe_nr(&ring_, &cqe, submitted_unconsumed_task_num_);
        // 回收内存
        if (ret == 0)
          for_each_cqe_retrieve_buffer();
        // submitted_unconsumed_task_num_为0，且无就绪CQE
        else if (ret == -EAGAIN)
          continue;
        // 其他错误
        else if (ret < 0)
          UtilError::error_exit("io_uring_wait_cqe_nr failed with " +
                                    std::to_string(ret) +
                                    " while waiting for buffer",
                                false);
      }
      unused_buffer_num_--;
    }
  }
  else
  {
    // 从队列中获取到了buffer
    unused_buffer_num_--;
  }
  return buffer_info;
}

Logger::Logger(const std::string &log_dir,
               const std::vector<LogName> &logname_list,
               int init_fixed_buffer_num, int fixed_buffer_pool_size,
               int io_uring_entries)
    : log_dir_(log_dir),
      init_buffer_capacity_(init_fixed_buffer_num),
      max_fixed_buffer_capacity_(fixed_buffer_pool_size),
      io_uring_entries_(io_uring_entries)
{
  // log_dir检查是否存在
  if (!UtilFile::dir_exists(log_dir_))
  {
    // 创建目录
    bool success = UtilFile::dir_create(log_dir_);
    if (!success)
      UtilError::error_exit("create log dir failed, exit", true);
  }

  // 空列表
  if (logname_list.empty())
    std::cout << "logname list is empty" << std::endl;

  // 创建文件并打开
  log_files.reserve(logname_list.size());
  for (int i = 0; i < logname_list.size(); i++)
  {
    // 空字符串
    const std::string &logname = logname_list[i];
    if (logname.empty())
      UtilError::error_exit("a logname is empty", false);

    // 创建并打开只追加写的文件
    std::string filename = log_dir_ + '/' + logname + ".log";
    int fd = open(filename.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0777);
    if (fd == -1)
      UtilError::error_exit("create/open file " + logname + " failed", true);

    // 日志名已经存在
    if (log_files.find(logname) != log_files.end())
      UtilError::error_exit("duplicated log name", false);

    // 从日志名到日志文件的映射
    log_files.emplace(logname, FileInfo(fd, i));
  }

  // 初始化io_uring
  FORCE_ASSERT(sizeof(BufferInfo *) == sizeof(io_uring_sqe::user_data));
  io_uring_params params;
  memset(&params, 0, sizeof(params));
  params.flags = IORING_SETUP_SQPOLL;
  if (io_uring_queue_init_params(io_uring_entries_, &ring_, &params) < 0)
    UtilError::error_exit("io_uring setup failed", true);
  if (!(params.features & IORING_FEAT_FAST_POLL))
    UtilError::error_exit(
        "IORING_FEAT_FAST_POLL not supported in current kernel", false);

  // 注册register file
  std::vector<int> fd_list(log_files.size(), -1);
  for (auto &&f : log_files)
    fd_list[f.second.file_index] = f.second.fd;
  int ret = io_uring_register_files(&ring_, fd_list.data(), log_files.size());
  if (ret < 0)
    UtilError::error_exit("failed to register files, " + std::to_string(ret),
                          false);

  // 初始化缓存
  std::vector<iovec> buffer_array;
  for (int i = 0; i < init_buffer_capacity_; i++)
  {
    // 开辟内存
    char *buffer = new char[max_log_len];
    // buffer数组，后续批量注册buffer
    buffer_array.emplace_back(buffer, max_log_len);
    // 原子变量自加操作保证buffer_index独一无二
    int buffer_index = new_buffer_index_.fetch_add(1);
    // buffer信息，同样在堆上开辟，队列只存指针以减少拷贝，但要注意释放内存
    BufferInfo *info = new BufferInfo(buffer, max_log_len, buffer_index);
    // buffer信息入队列
    unused_buffer_.push(info);
    unused_buffer_num_++;
  }

  // 注册register buffer
  FORCE_ASSERT(unused_buffer_.is_lock_free());
  FORCE_ASSERT(unsubmitted_tasks_.is_lock_free());
  FORCE_ASSERT(init_buffer_capacity_ > 0);
  FORCE_ASSERT(max_fixed_buffer_capacity_ > 0);
  FORCE_ASSERT(io_uring_entries_ >= 32 &&
               (io_uring_entries_ & (io_uring_entries_ - 1)) == 0)
  FORCE_ASSERT(max_fixed_buffer_capacity_ >= init_buffer_capacity_);
  FORCE_ASSERT(io_uring_entries_ >= max_fixed_buffer_capacity_);
  // 这里提前设置足够大的max_buffer_capacity_，初始化时用init_buffer_capacity_，方便后续扩展内存池
  FORCE_ASSERT(max_fixed_buffer_capacity_ < UIO_MAXIOV);
  // ret = io_uring_register_buffers(&ring_, buffer_array.data(),
  // buffer_array.size());
  ret = io_uring_register_buffers_sparse(
      &ring_, (unsigned int)max_fixed_buffer_capacity_);
  if (ret == -EINVAL)
    UtilError::error_exit(
        "failed to register buffers sparse, please check if "
        "linux kernel version >= 5.19" +
            std::to_string(ret),
        false);
  else if (ret < 0)
    UtilError::error_exit(
        "failed to register buffers sparse, " + std::to_string(ret), false);
  ret = io_uring_register_buffers_update_tag(&ring_, 0, buffer_array.data(), 0,
                                             init_buffer_capacity_);
  if (ret < 0)
    UtilError::error_exit(
        "failed to update register buffers " + std::to_string(ret), false);
  new_buffer_index_.fetch_add(init_buffer_capacity_);
}

Logger::~Logger()
{
  // 处理未提交任务
  bool submit_task = false;

  LogTask task;
  while (unsubmitted_tasks_.pop(task))
  {
    // 保证sqe不满
    FORCE_ASSERT(submitted_unconsumed_task_num_ <= io_uring_entries_);
    if (submitted_unconsumed_task_num_ == io_uring_entries_)
    {
      struct io_uring_cqe *cqe;
      int ret =
          io_uring_wait_cqe_nr(&ring_, &cqe, submitted_unconsumed_task_num_);
      if (ret < 0)
        UtilError::error_exit("io_uring_wait_cqe_nr failed with " +
                                  std::to_string(ret) + " in destructor",
                              false);
      // 回收内存
      for_each_cqe_retrieve_buffer();
    }

    // 填充sqe
    io_uring_prep_submit_task(task);
    unsubmitted_tasks_num_--;
    submitted_unconsumed_task_num_++;
    submit_task = true;
  }
  if (submit_task)
    io_uring_submit(&ring_);

  // 等待io_uring完成已提交的io
  struct io_uring_cqe *cqe;
  int ret = io_uring_wait_cqe_nr(&ring_, &cqe, submitted_unconsumed_task_num_);
  if (ret < 0)
    UtilError::error_exit("io_uring_wait_cqe_nr failed with " +
                              std::to_string(ret) + " in destructor",
                          false);

  // 清理cqe以及buffer_info
  unsigned head;
  unsigned count = 0;
  io_uring_for_each_cqe(&ring_, head, cqe)
  {
    count++;
    // cqe中的buffer在此清理
    BufferInfo *buffer_info = nullptr;
    memcpy(&buffer_info, &cqe->user_data, sizeof(BufferInfo *));
    delete buffer_info;
  }
  io_uring_cq_advance(&ring_, count);

  // 未使用的buffer在此清理
  unused_buffer_.consume_all([](const BufferInfo *info)
                             {
    delete (char *)info->iov.iov_base;
    delete info; });

  // 取消注册file
  io_uring_unregister_buffers(&ring_);

  // 取消注册buffer
  io_uring_unregister_files(&ring_);

  // 关闭io_uring
  io_uring_queue_exit(&ring_);

  // 关闭文件
  for (const auto &it : log_files)
    close(it.second.fd);
}

// 写日志
void Logger::log(const std::string &logname, const std::string &msg)
{
  // 日志名不存在
  auto log_file = log_files.find(logname);
  if (log_file == log_files.end())
    UtilError::error_exit("attempting to log in a undefined logname", false);

  // 可用buffer剩余不足一半，采取措施来增加可用缓存
  if (unsubmitted_tasks_num_ + submitted_unconsumed_task_num_ >=
      unused_buffer_num_)
  {
    // 抢到sq锁的线程向io_uring提交IO写请求，抢不到锁不阻塞
    if (std::unique_lock sq_lock(io_uring_sq_mutex_, std::try_to_lock);
        sq_lock.owns_lock())
    {
      // 填充sqe
      LogTask task;
      while (unsubmitted_tasks_.pop(task))
      {
        unsubmitted_tasks_num_--;
        io_uring_prep_submit_task(task);
        submitted_unconsumed_task_num_++;
      }
      // 提交前面填充的sqes
      io_uring_submit(&ring_);
    }

    // 抢到cq锁的线程收割cqe，恢复可用缓存，不阻塞
    if (std::unique_lock cq_lock(io_uring_cq_mutex_, std::try_to_lock);
        cq_lock.owns_lock())
    {
      struct io_uring_cqe *cqe;
      // 检查是否有新cqe，抢不到锁不阻塞
      if (io_uring_peek_cqe(&ring_, &cqe) == 0)
        // 抢到锁的线程回收内存至可用队列
        for_each_cqe_retrieve_buffer();
    }
  }

  // 获取buffer（从多种渠道尝试）
  BufferInfo *buffer_info = get_buffer();

  // 拷贝日志消息到buffer中
  // 日志消息格式化
  std::string log_msg;
  format_str(logname, msg, log_msg);
  if (log_msg.size() > max_log_len)
  {
    std::cout << "log size exceed max log length, it will be cut" << std::endl;
    log_msg[max_log_len - 1] = '\n';
  }
  int copy_len = std::min((size_t)max_log_len, log_msg.size());
  memcpy(buffer_info->iov.iov_base, log_msg.data(), copy_len);

  // 最后入任务队列
  LogTask task(copy_len, &log_file->second, buffer_info);
  unsubmitted_tasks_.push(task);
  unsubmitted_tasks_num_++;

#ifndef PRODUCTION
  std::cout << log_msg;
#endif
}