#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <filesystem>
#include <string>

#include "worker.h"

const std::string root = "/var/www/html/";

bool is_valid_path(const std::string &path, const std::string &root)
{
  std::error_code ec;
  std::filesystem::path fs_path = std::filesystem::canonical(path, ec);
  if (ec)
  {
    std::cout << "request path invalid:" << ec.message() << std::endl;
    return false;
  }

  std::filesystem::path fs_root = std::filesystem::canonical(root, ec);
  if (ec)
  {
    std::cout << "root path invalid:" << ec.message() << std::endl;
    return false;
  }

  if (std::filesystem::is_directory(path))
  {
    std::cout << "path is directory, not file" << ec.message() << std::endl;
    return false;
  }

  return std::equal(fs_root.begin(), fs_root.end(), fs_path.begin());
}

int web_handler(http::request<http::string_body> &req, ResponseType &res,
                process_func_args &args)
{
  args.use_web_server = true;

  std::string path = root + std::string(req.target());
  if (!is_valid_path(path, root))
  {
    std::cout << "invalid path: " << args.file_path << std::endl;
    args.file_path = "/var/www/html/404.html";
    return 0;
  }

  args.file_path = path;
  return 0;
}

int main()
{
  const int worker_num = 2;
  Service server(worker_num, web_handler);
  server.start();
}