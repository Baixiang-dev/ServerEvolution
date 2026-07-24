#pragma once
#include <signal.h>

extern bool g_quit;

using handler_t = void (*)(int);

/**
 * @brief 封装信号注册函数 sigaction
 *
 * @param signum 信号
 * @param handler 信号对应的处理函数
 *
 * @return 成功时返回指向前次处理程序的指针，否则返回 SIG_ERR (不设置errno)
 */
handler_t Signal(int signum, handler_t handler);

/**
 * @brief SIGINT 信号处理函数，设置 g_quit == true
 */
void SigintHandler(int signum);
