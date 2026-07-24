#include "Signal.h"

bool g_quit = false;

handler_t Signal(int signum, handler_t handler)
{
    struct sigaction act, old_act;
    act.sa_handler = handler;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    if (sigaction(signum, &act, &old_act) < 0)
    {
        return SIG_ERR;
    }
    return old_act.sa_handler;
}

void SigintHandler(int signum)
{
    (void)signum;   // unused
    g_quit = true;
}