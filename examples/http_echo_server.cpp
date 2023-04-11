#include "worker.h"

int echo_handler(http::request<http::string_body> &req,
                 http::response<http::string_body> &res,
                 process_func_args &args)
{
  res.result(http::status::ok);
  res.set(http::field::content_type, "text/html");
  res.body() = std::move(req.body());
  res.prepare_payload();
  return 0;
}

int main()
{
  Service server(echo_handler);
  server.start();
}