实现主从Multi-Reactor(含线程池 Offload)

- 采用主从 Reactor（Main acceptor + N 个 SubReactor）架构，跨线程交接使用 eventfd 唤醒 + 线程安全队列

- 实现 SubReactor（N 个线程）：
    - 线程内资源：epoll fd、timerfd、eventfd、sessions(fd->HttpSession)
    - 支持 enqueueFd(fd)：主线程投递新连接 fd；SubReactor eventfd 分支接入并 epoll ADD
    - eventfd/timerfd/clientfd 全部 EPOLLET，必须读/写到 EAGAIN（drain）
- 实现 MainAcceptor（1 个线程）：
    - 只做 accept（到 EAGAIN）+ 轮询分发（round-robin）
    - 分发：sub.enqueueFd(fd) + eventfd_write(sub.efd, 1)

- 参数约束：
    - -t 指定 SubReactor 数（默认 std::thread::hardware_concurrency()，为 0 则回退 4）
    - -a/-p 沿用现有
- stop 语义：
    - running=false，close listen fd
    - 唤醒所有 SubReactor（eventfd 写入）
    - join acceptor + join 所有 SubReactor

- 线程池 Offload
- 复用 ThreadPool.h，新增 -w worker 线程数（默认可取 -t 或 CPU 核数）
- 关键约束：worker 线程绝不直接触碰 HttpSession
    - SubReactor 线程解析完请求后，把“业务处理任务”投递到线程池（携带 HttpRequest 数据副本）
    - worker 执行 route + handler 生成 HttpResponse
    - worker 完成后，通过 SubReactor 的 runInLoop(functor) 投递“写回响应”的闭包，再 eventfd 唤醒
- SubReactor 增加 pendingFunctors 队列：eventfd 分支同时 drain pending fds + pending functors