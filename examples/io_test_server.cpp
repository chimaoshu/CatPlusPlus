#include "worker.h"

#include <string>

static int body_size = 10240;
static char *body = new char[body_size];

int echo_handler(http::request<http::string_body> &req,
                 http::response<http::buffer_body> &res,
                 process_func_args &args)
{
    res.result(http::status::ok);
    res.set(http::field::content_type, "text/html");

    res.body().data = body;
    res.body().size = body_size;
    res.body().more = false;

    res.prepare_payload();
    return 0;
}

int main()
{
    Service server(echo_handler);
    server.start();
}