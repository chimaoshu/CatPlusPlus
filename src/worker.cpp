#include "worker.h"
#include "io_uring_wrapper.h"

#include <netinet/tcp.h>
#include <string.h>

#include <boost/beast/core/make_printable.hpp>
#include <iostream>
#include <typeinfo>

#include "awaitable.h"
#include "http.h"

int Worker::get_worker_id() { return self_worker_id_; }

void Worker::add_process_task(ConnectionTaskHandler h)
{
  if (ws_process_task_queue.push(h))
    Log::debug("add a process task to wsq");
  else
    FORCE_ASSERT(service_->global_queue.push(h));
}

void Worker::add_io_resume_task(int sock_fd_idx) { FORCE_ASSERT(private_io_task_queue.push(sock_fd_idx)); }

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

Worker::Worker(int worker_id, ProcessFuncType processor, Service *service)
    : self_worker_id_(worker_id),
      ws_process_task_queue(config::force_get_int("WORKER_WSQ_CAPACITY")),
      private_io_task_queue(128),
      processor_(processor),
      service_(service)
{
  std::string addrs = config::force_get_str("SERVER_ADDRESS");
  int port = config::force_get_int("LISTEN_PORT");
  FORCE_ASSERT(ws_process_task_queue.is_lock_free());
  FORCE_ASSERT(port > 1023 && port < 65535);

  // 初始化 socket
  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0)
    UtilError::error_exit("socket failed", true);
  {
    int optval = 1;

    int flags = SO_REUSEPORT | SO_REUSEADDR;
    if (setsockopt(listen_fd_, SOL_SOCKET, flags, &optval, sizeof(optval)) < 0)
      UtilError::error_exit("setsockopt failed", true);

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET,
    server_addr.sin_addr.s_addr = inet_addr(addrs.c_str());
    server_addr.sin_port = htons(port);

    if (bind(listen_fd_, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
      UtilError::error_exit("bind failed", true);

    // 限定最大连接数
    int max_conn_num = config::force_get_int("WORKER_MAX_CONN_NUM");
    if (listen(listen_fd_, max_conn_num) < 0)
      UtilError::error_exit("listen failed", true);
  }

  // 准备multishot-accept，并将listen_fd注册到内核，但不提交
  io_uring_instance.add_multishot_accept(listen_fd_);
}

void Worker::run()
{
  // 提交构造函数准备好的accept请求
  while (true)
  {
    ConnectionTaskHandler handler;

    static bool enable_work_stealing = config::force_get_int("ENABLE_WORK_STEALING");

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
    else if (enable_work_stealing && service_->global_queue.pop(handler))
    {
      Log::debug("get a task from global queue worker_id=", self_worker_id_);
      handler.promise().process_worker = this;
      have_task = true;
    }
    // 全局队列没有，就从其他worker的队列偷
    else if (enable_work_stealing)
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
    {
      handler.resume();
      continue;
    }

    struct io_uring *ring = io_uring_instance.get_struct();
    struct io_uring_cqe *cqe;
    int head, count = 0;
    // wait for 50ms
    __kernel_timespec timeout{.tv_sec = 0, .tv_nsec = 50000000};
    int ret = io_uring_wait_cqe_timeout(ring, &cqe, &timeout);
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
      // 进入下一轮循环
      continue;
    }

    // 有就绪cqe，则进行处理
    io_uring_for_each_cqe(ring, head, cqe)
    {
      count++;

      // 处理cqe的user_data
      struct IORequestInfo info;
      memcpy(&info, &cqe->user_data, sizeof(struct IORequestInfo));
      Log::debug("receive cqe, sock_fd_idx=", info.fd, "|type|cqe.res|", (int)info.type, "|", cqe->res);

      // 保护性代码：判断CQE正确性
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
                     "|current_io=", (int)current_io,
                     "|cqe_io_type=", (int)io_type);
          UtilError::error_exit("receive unknown cqe", false);
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
      // multiple send需要多个sqe完成才能恢复
      case IOType::MULTIPLE_SEND_ZC_SOCKET:
      case IOType::MULTIPLE_SEND_SOCKET:
      {
        // 取出req_id对应的cqe信息
        ConnectionTaskHandler handler = connections_.at(info.fd).handler;
        cqe_status &target_cqe = handler.promise().multiple_send_cqes[info.req_id];

        // zero-copy的第一个cqe，只拷贝cqe数据，不标记为已接收
        if (io_type == MULTIPLE_SEND_ZC_SOCKET && cqe->flags & IORING_CQE_F_MORE)
        {
          copy_cqe(target_cqe.cqe, *cqe);
          Log::debug("recv cqe, IORING_CQE_F_MORE is set, do not resume, wait for notification cqe, sock_fd_idx=", info.fd);
          assert(!target_cqe.has_recv);
        }
        // zero-copy的第二个cqe，标记为已接收
        else if (io_type == MULTIPLE_SEND_ZC_SOCKET && cqe->flags & IORING_CQE_F_NOTIF)
        {
          target_cqe.has_recv = true;
          Log::debug("recv cqe, IORING_CQE_F_NOTIF is set|sock_fd_idx=", info.fd);
        }
        // 非zero-copy的一个sqe只对应一个cqe，同时拷贝与标记
        else if (io_type == MULTIPLE_SEND_SOCKET)
        {
          copy_cqe(target_cqe.cqe, *cqe);
          target_cqe.has_recv = true;
        }

        // 判断是否全部cqe都处理完毕，可以恢复协程
        bool recv_all = true;
        for (const cqe_status &cqe : handler.promise().multiple_send_cqes)
        {
          if (!cqe.has_recv)
          {
            recv_all = false;
            break;
          }
        }
        if (recv_all)
        {
          Log::debug("all cqe received, preapre to resume coroutine|sock_fd_idx=", info.fd);
          handler.resume();
        }
        break;
      }
      // zero copy的send都有相同行为：需要两个cqe才能恢复
      case IOType::SENDMSG_ZC_SOCKET:
      {
        // 接收到sendzc的第一个cqe，IORING_CQE_F_MORE表示还会有一个cqe通知表示结束
        // 此时还不能恢复，需要等待收到下一个cqe通知才可以resume
        // 但此时的cqe.res是代表IO结果的，因此在这里拷贝
        if (cqe->flags & IORING_CQE_F_MORE)
        {
          ConnectionTaskHandler handler = connections_.at(info.fd).handler;
          copy_cqe(handler.promise().cqe, *cqe);
          Log::debug("recv cqe, IORING_CQE_F_MORE is set, do not resume, wait for notification cqe, sock_fd_idx=", info.fd);
          break;
        }
        // 第二个cqe进行resume
        else
        {
          ConnectionTaskHandler handler = connections_.at(info.fd).handler;
          // 此处不能copy
          // copy_cqe(handler.promise().cqe, *cqe);
          Log::debug("process send cqe, need to resume, cqe->res=", cqe->res);
          handler.resume();
          break;
        }
      }
      case IOType::SEND_FILE:
      {
        ConnectionTaskHandler handler = connections_.at(info.fd).handler;
        std::map<int, bool> &sendfile_sqe_complete = handler.promise().sendfile_sqe_complete;

        // 成功，只有当全部sqe完成时再resume
        if (cqe->res > 0)
        {
          sendfile_sqe_complete[info.req_id] = true;
          // 检查是否全部cqe都已经收到
        }
        // 出错
        else
        {
          sendfile_sqe_complete[info.req_id] = true;
          Log::error("senfile failed, need resume, sock_fd_idx=", info.fd,
                     "|cqe->res=", cqe->res,
                     "|req_id=", info.req_id);
        }

        bool resume = true;
        for (auto &it : sendfile_sqe_complete)
        {
          if (!it.second)
          {
            resume = false;
            break;
          }
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
      case IOType::SENDMSG_SOCKET:
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
    io_uring_cq_advance(ring, count);
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

    // 设置tcp_nodelay
    if (!config::force_get_int("USE_DIRECT_FILE") && config::force_get_int("TCP_NODELAY"))
    {
      int flag = 1;
      if (setsockopt(sock_fd_idx, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) != 0)
        UtilError::error_exit("set tcp nodelay failed", true);
    }

    // 创建协程、添加连接
    connections_.emplace(sock_fd_idx,
                         handle_http_request(sock_fd_idx, processor_));
    Log::debug("accept new connection, sock_fd_idx=", sock_fd_idx);

    // 设置worker
    // 后续该协程的所有IO操作都通过此worker完成，使用fixed file加速
    connections_.at(sock_fd_idx).handler.promise().io_worker = this;
    // add_io_resume_task(sock_fd_idx);
    connections_.at(sock_fd_idx).handler.resume();
  }
  // accept错误
  else
  {
    Log::error("accept failed cqe->res=", cqe->res);
    // fixed file槽位不足
    if (cqe->res == -ENFILE)
    {
      UtilError::error_exit(
          "fixed file not enough, please set a higher fixed file num, current connections num=" + std::to_string(connections_.size()) +
              " max_fixed_file_num=" + std::to_string(io_uring_instance.get_max_fixed_file_num()),
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
      io_uring_instance.add_multishot_accept(listen_fd_);
#else
      UtilError::error_exit("multishot-accept is terminated", false);
#endif
    }
  }
}