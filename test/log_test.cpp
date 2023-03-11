#include <pthread.h>
#include <climits>

#include "src/log.h"

// #define NUM_TIMES INT_MAX
#define NUM_TIMES 5000

void *debug(void *ptr)
{
    for (int i = 0; i < NUM_TIMES; i++)
    {
        Log::debug("this is a log " + std::to_string(i) + " " + std::to_string(pthread_self()));
        Log::debug("this is a log ", i, " ", pthread_self());
    }
    // pthread_exit(NULL);
    return nullptr;
}

void *info(void *ptr)
{
    for (int i = 0; i < NUM_TIMES; i++)
    {
        Log::info("this is a log " + std::to_string(i) + " " + std::to_string(pthread_self()));
        Log::info("this is a log ", i, " ", pthread_self());
    }
    // pthread_exit(NULL);
    return nullptr;
}

void *warn(void *ptr)
{
    for (int i = 0; i < NUM_TIMES; i++)
    {
        Log::warn("this is a log " + std::to_string(i) + " " + std::to_string(pthread_self()));
        Log::warn("this is a log ", i, " ", pthread_self());
    }
    // pthread_exit(NULL);
    return nullptr;
}

void *error(void *ptr)
{
    for (int i = 0; i < NUM_TIMES; i++)
    {
        Log::error("this is a log " + std::to_string(i) + " " + std::to_string(pthread_self()));
        Log::warn("this is a log ", i, " ", pthread_self());
    }
    // pthread_exit(NULL);
    return nullptr;
}

void *all(void *ptr)
{
    debug(NULL);
    info(NULL);
    warn(NULL);
    error(NULL);
    return nullptr;
}

int main()
{
    int thread_num = 10;
    pthread_t threads[thread_num];
    for (int i = 0; i < thread_num; i++)
    {
        int ret = pthread_create(&threads[i], NULL, all, NULL);
        if (ret != 0)
        {
            std::cerr << "Error: Unable to create thread, " << ret << std::endl;
            return 1;
        }
    }
    for (int i = 0; i < thread_num; i++)
    {
        pthread_join(threads[i], NULL);
    }
    return 0;
}