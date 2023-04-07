#ifndef __HTTP_H__
#define __HTTP_H__

#include "worker.h"

#include <fcntl.h>

template <typename T>
void serialize(T *sr,
               std::list<boost::asio::const_buffer> &buffers,
               error_code &ec, int &data_to_consume, bool header_only)
{
    // using BodyType = typename std::decay_t<decltype(res)>::body_type;
    data_to_consume = 0;
    bool is_finish = false;

    sr->split(header_only);

    do
    {
        sr->next(ec, [&](error_code &ec, auto const &buffer)
                 {
                     // hack: 不consume的话，next会循环输出，遇到一样的说明结束了
                     // 通过这种方式避免一次数据拷贝，等到拷贝到write_buf发送完再consume
                     boost::asio::const_buffer buf = *buffer.begin();
                     if (buffers.front().data() == buf.data())
                     {
                         Log::debug("get same buf with next()",
                                    ", this means that the serialization is done.");
                         is_finish = true;
                         return;
                     }

                     ec.assign(0, ec.category());
                     for (auto it = buffer.begin(); it != buffer.end(); it++)
                     {
                         buffers.push_back(*it);
                     }
                     data_to_consume += boost::asio::buffer_size(buffer);
#ifndef PRODUCTION
                     std::cout << make_printable(buffer);
#endif
                     // sr->consume(boost::asio::buffer_size(buffer));
                 });
    } while (!ec && ((!header_only && !sr->is_done()) || (header_only && !sr->is_header_done())) && !is_finish);
}

ConnectionTask handle_http_request(int sock_fd_idx, ProcessFuncType processor);

#endif // __HTTP_H__