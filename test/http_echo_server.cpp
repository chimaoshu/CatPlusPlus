#include "worker.h"

int echo_handler(http::request<http::string_body> &req,
                 http::response<http::string_body> &res) {
  res.result(http::status::ok);
  res.set(http::field::content_type, "text/html");
  res.keep_alive(false);
  res.body() = req.body();
  res.prepare_payload();
  return 0;
}

int main() {
  Service server(2, 32, "0.0.0.0", 8080, 16, 32, echo_handler);
  server.start();
}