#ifndef __UTIL_HPP__
#define __UTIL_HPP__

#include <fcntl.h>
#include <signal.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cmath>

#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>

namespace UtilEnv
{
  enum Mode
  {
    PROD,
    DEBUG
  };

#ifdef PRODUCTION
  constexpr Mode BuildMode = Mode::PROD;
#else
  constexpr Mode BuildMode = Mode::DEBUG;
#endif

  static bool is_debug_mode()
  {
    return BuildMode == Mode::DEBUG;
  }

  static bool is_production_mode()
  {
    return BuildMode == Mode::PROD;
  }

} // namespace UtilEnv

namespace UtilFile
{
  static bool dir_exists(const std::string &path)
  {
    struct stat file_stat;
    bool res =
        (stat(path.c_str(), &file_stat) == 0) && (S_ISDIR(file_stat.st_mode));
    return res;
  }

  static bool dir_create(const std::string &path)
  {
    if (mkdir(path.c_str(), 0777) != 0)
    {
      perror("create_dir");
      return false;
    }
    return true;
  }

  static bool dir_remove(const std::string &path)
  {
    return rmdir(path.c_str()) == 0;
  }

  static bool file_exists(const std::string &path)
  {
    return access(path.c_str(), F_OK) != -1;
  }

  static bool file_remove(const std::string &path)
  {
    return remove(path.c_str()) == 0;
  }

  static int is_valid_fd(int fd)
  {
    return fcntl(fd, F_GETFL) != -1 || errno != EBADF;
  }

  static bool fd_is_invalid_or_closed(int fd)
  {
    return fcntl(fd, F_GETFL) < 0 && errno == EBADF;
  }

} // namespace UtilFile

namespace UtilError
{
  static void error_exit(std::string msg, bool print_perror)
  {
    if (print_perror)
      perror(msg.c_str());
    else
      std::cerr << msg << std::endl;

    if (UtilEnv::is_production_mode())
      exit(EXIT_FAILURE);
    else if (UtilEnv::is_debug_mode())
      std::terminate();
  }

#define FORCE_ASSERT(expr)              \
  if (!(expr))                          \
  {                                     \
    std::cerr << "Assertion failed: "   \
              << #expr << ", file "     \
              << __FILE__ << ", line "  \
              << __LINE__ << std::endl; \
    std::terminate();                   \
  }

} // namespace UtilError

namespace UtilSystem
{
  // 守护进程
  static int init_daemon()
  {
    int pid;
    int i;
    /*忽略终端I/O信号，STOP信号*/
    signal(SIGTTOU, SIG_IGN);
    signal(SIGTTIN, SIG_IGN);
    signal(SIGTSTP, SIG_IGN);
    signal(SIGHUP, SIG_IGN);

    pid = fork();

    // 结束父进程，使得子进程成为后台进程
    if (pid > 0)
      exit(0);
    else if (pid < 0)
      UtilError::error_exit("fork fail while initializing deamon", true);

    // 建立一个新的进程组，在这个新的进程组中，子进程成为这个进程组的首进程，以使该进程脱离所有终端
    setsid();

    // 再次新建一个子进程，退出父进程，保证该进程不是进程组长，同时让该进程无法再打开一个新的终端
    pid = fork();
    if (pid > 0)
      exit(0);
    else if (pid < 0)
      UtilError::error_exit("fork fail while initializing deamon", true);

    // 关闭所有从父进程继承的不再需要的文件描述符
    for (i = 0; i < NOFILE; i++)
      close(i);

    // 改变工作目录，使得进程不与任何文件系统联系
    // chdir("/");

    // 将文件当时创建屏蔽字设置为0
    umask(0);

    // 忽略SIGCHLD信号
    signal(SIGCHLD, SIG_IGN);

    return 0;
  }
} // namespace UtilSystem

namespace UtilString
{
  // 去掉input前后的字符c
  static std::string strip(const std::string &input, char c = ' ')
  {
    int start_pos = input.find_first_not_of(c);
    if (start_pos == std::string::npos)
      start_pos = 0;

    int end_pos = input.find_last_not_of(c);
    if (end_pos == std::string::npos)
      end_pos = input.size() - 1;

    return input.substr(start_pos, end_pos - start_pos + 1);
  }
} // namespace UtilString

namespace UtilMath
{
  // 下一个大于n的2的幂次
  static int next_power_of_two(int n)
  {
    // n已经是2的幂次
    if ((n & (n - 1)) == 0)
    {
      return n;
    }

    int result = pow(2, ceil(log2(n)));
    FORCE_ASSERT(result >= 0);
    return result;
  }
} // namespace UtilMath

#endif // __UTIL_HPP__