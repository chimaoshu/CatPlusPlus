#ifndef __HTTP_H__
#define __HTTP_H__

#include "worker.h"

#include <fcntl.h>

ConnectionTask handle_http_request(int sock_fd_idx, ProcessFuncType processor);

#endif // __HTTP_H__