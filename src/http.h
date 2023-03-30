#ifndef __HTTP_H__
#define __HTTP_H__

#include "worker.h"

void serialize(SerializerType &sr,
               std::list<boost::asio::const_buffer> &buffers, error_code &ec,
               int &data_to_consume);

ConnectionTask handle_http_request(int sock_fd_idx, ProcessFuncType processor);

#endif // __HTTP_H__