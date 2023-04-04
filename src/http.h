#ifndef __HTTP_H__
#define __HTTP_H__

#include "worker.h"

void serialize(ResponseType &res, SerializerType &sr, std::list<boost::asio::const_buffer> &buffers, error_code &ec, int &data_to_consume);
void internal_serialize(SerializerType &sr,
                        std::list<boost::asio::const_buffer> &buffers, error_code &ec,
                        int &data_to_consume);
void clear_serializer_data(SerializerType &sr, int data_to_consume);

bool is_response_keep_alive(ResponseType &res);

ConnectionTask handle_http_request(int sock_fd_idx, ProcessFuncType processor);

#endif // __HTTP_H__