/**
 * 多 Reactor 模型
 *
 * 一个 main Reactor 接收、建立新连接，多个 Sub Reactor 处理连接上的事件
 */

#include <dirent.h>
#include <iostream>
#include <map>
#include <memory>
#include <stack>
#include <string>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/signal.h>

#include "http/HttpBuilder.h"
#include "http/handlers/HttpHandlers.h"
#include "http/router/HttpRouter.h"
#include "io/socket/Socket.h"

#include "ThreadPool.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"

/** 自定义 make_unique 方便使用 */
template<typename T, typename... Args> std::unique_ptr<T> make_unique(Args&&... args)
{
    return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

class Channel
{
public:
    Channel(std::unique_ptr<Socket> sock, int events)
        : sock_(std::move(sock))
        , events_(events)
        , revents_(0)
    {
    }

    // 临时支持listen fd：不拥有，只存 fd
    Channel(int fd, int events)
        : fd_(fd)
        , events_(events)
        , revents_(0)
    {
    }

    void setReadCallback(std::function<void()> cb) { readCallback_ = std::move(cb); }
    void setWriteCallback(std::function<void()> cb) { writeCallback_ = std::move(cb); }
    void setErrorCallback(std::function<void()> cb) { errorCallback_ = std::move(cb); }

    int  fd() const { return sock_ ? sock_->fd() : fd_; }
    int  events() const { return events_; }
    void set_revents(int revents) { revents_ = revents; }
    void setEvents(int events) { events_ = events; }

    void handleEvent()
    {
        if (revents_ & EPOLLIN)
        {
            if (readCallback_)
                readCallback_();
        }
        if (revents_ & EPOLLOUT)
        {
            if (writeCallback_)
                writeCallback_();
        }
        if (revents_ & EPOLLERR)
        {
            if (errorCallback_)
                errorCallback_();
        }
    }

private:
    std::unique_ptr<Socket> sock_;
    int                     fd_ = -1;   // 临时支持 listen fd
    int                     events_;    // interested events
    int                     revents_;   // returned events

    // 事件发生时的回调函数
    std::function<void()> readCallback_;
    std::function<void()> writeCallback_;
    std::function<void()> errorCallback_;
};

/**
 * @brief 对 epoll 操作的封装，提供注册、修改、删除 fd 的接口
 */
class Epoller
{
public:
    Epoller()
    {
        epoll_fd_ = epoll_create1(0);
        if (epoll_fd_ == -1)
        {
            throw std::runtime_error("Failed to create epoll file descriptor");
        }
    }

    ~Epoller()
    {
        if (epoll_fd_ != -1)
        {
            close(epoll_fd_);
        }
    }

    /**
     * @brief 注册一个 channel 到 epoll
     */
    void addChannel(Channel* ch)
    {
        struct epoll_event ev;
        ev.data.ptr = ch;
        ev.events = ch->events();
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, ch->fd(), &ev);
        channels_[ch->fd()] = ch;
    }

    /**
     * @brief 修改一个已注册的 channel 的事件
     */
    void updateChannel(Channel* ch)
    {
        if (channels_.find(ch->fd()) == channels_.end())
        {
            throw std::runtime_error("Channel not found in Epoller");
        }

        struct epoll_event ev;
        ev.data.ptr = ch;
        ev.events = ch->events();
        epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, ch->fd(), &ev);
    }

    /**
     * @brief 从 epoll 中移除一个 channel
     */
    void removeChannel(Channel* ch)
    {
        if (channels_.find(ch->fd()) == channels_.end())
        {
            throw std::runtime_error("Channel not found in Epoller");
        }

        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, ch->fd(), nullptr);
        channels_.erase(ch->fd());
    }

    /**
     * @brief 在 epoll 上侦听事件并返回活跃Channel
     */
    std::vector<Channel*> wait(int timeout_ms)
    {
        const int             max_events = 64;
        std::vector<Channel*> ready;
        struct epoll_event    events[max_events];
        int                   n = epoll_wait(epoll_fd_, events, max_events, timeout_ms);
        for (int i = 0; i < n; ++i)
        {
            Channel* ch = static_cast<Channel*>(events[i].data.ptr);
            ch->set_revents(events[i].events);
            ready.push_back(ch);
        }
        return ready;
    }

private:
    int                     epoll_fd_;
    std::map<int, Channel*> channels_;   // 在 epoll 中注册的 channel 列表
};

/**
 * @brief 事件循环的封装，提供 loop() 接口，死循环监听事件、分发事件、处理事件
 */
class EventLoop
{
public:
    EventLoop()
        : epoller_()
    {
        thread_id_ = std::this_thread::get_id();
        int wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (wakeup_fd_ == -1)
        {
            std::runtime_error("Failed to create event fd");
        }
        wakeupChan_ = make_unique<Channel>(wakeup_fd_, EPOLLIN);
        wakeupChan_->setReadCallback([this]() { this->handleWakeup(); });
        epoller_.addChannel(wakeupChan_.get());
    }
    ~EventLoop() {}

    void loop()
    {

        while (!quit_)
        {
            auto activated_channels = epoller_.wait(-1);   // 无IO时永久阻塞，直到 eventfd 唤醒
            for (Channel* chan : activated_channels)
            {
                chan->handleEvent();
            }
            doPendingTasks();
        }
    }

    void quit()
    {
        quit_ = true;
        wakeUp();
    }

    void addChannel(Channel* ch) { epoller_.addChannel(ch); }
    void updateChannel(Channel* ch) { epoller_.updateChannel(ch); }
    void removeChannel(Channel* ch) { epoller_.removeChannel(ch); }

    bool isInLoopThread() const { return std::this_thread::get_id() == thread_id_; }

    void runInLoop(std::function<void()> cb)
    {
        if (isInLoopThread())
        {
            cb();
        }
        else
        {
            queueInLoop(std::move(cb));
        }
    }

    /**
     * @brief 供其他线程投递任务
     */
    void queueInLoop(std::function<void()> cb)
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            pendingTasks_.push_back(std::move(cb));
        }
        wakeUp();
    };

private:
    Epoller                            epoller_;
    bool                               quit_ = false;
    std::unique_ptr<Channel>           wakeupChan_;
    std::mutex                         mutex_;
    std::thread::id                    thread_id_;   // 判断当前线程是不是 loop 线程，以决定待执行的任务是立即执行，还是提交到loop_thread等待执行。
    std::vector<std::function<void()>> pendingTasks_;


    /**
     * @brief 执行其他线程投递的任务
     */
    void doPendingTasks()
    {
        std::vector<std::function<void()>> tasks;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            tasks = pendingTasks_;
            pendingTasks_.clear();
        }
        for (auto& task : tasks)
        {
            task();
        }
    }

    /**
     * @brief 往 eventfd 写 8 字节，唤醒阻塞在 epoll_wait 的 IO 线程
     */
    void wakeUp()
    {
        uint64_t one = 1;
        ::write(wakeupChan_->fd(), &one, sizeof(one));
    }

    /**
     * @brief 读空 eventfd 的计数器，消耗唤醒事件
     */
    void handleWakeup()
    {
        uint64_t val;
        ::read(wakeupChan_->fd(), &val, sizeof(val));
    }
};

/**
 * @brief 持有 EventLoop 的 IO 线程的封装
 */
class EventLoopThread
{
public:
    explicit EventLoopThread(const std::string& threadName = "EventLoopThread")
        : loop_(nullptr)
        , loopThreadname_(threadName)
    {
    }

    ~EventLoopThread()
    {
        if (loop_)
        {
            loop_->quit();
        }
        if (thread_.joinable())
        {
            thread_.join();
        }
    }

    /**
     * @brief 启动IO线程，开始事件循环
     *
     * @note 此方法会阻塞调用它的线程，直到事件循环已就绪
     *
     * @return EventLoop* 返回指向事件循环的裸指针，供外部投递任务
     */
    EventLoop* run()
    {
        std::mutex              mtx;
        std::condition_variable cv;
        bool                    started = false;

        thread_ = std::thread(
            [this, &mtx, &cv, &started]()
            {
                loop_ = make_unique<EventLoop>();
                {
                    std::lock_guard<std::mutex> lock(mtx);
                    started = true;
                }
                cv.notify_one();   // 通知调用者，loop已就绪

                loop_->loop();
            });

        {
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait(lock, [&started]() { return started; });
        }

        return loop_.get();
    }

    /**
     * @brief 等待事件循环退出
     *
     * @note 此方法会阻塞调用它的线程，直到事件循环退出
     */
    void wait()
    {
        if (loop_)
        {
            loop_->quit();
        }
        if (thread_.joinable())
        {
            thread_.join();
        }
    }

    /**
     * @brief 或许指向事件循环的裸指针
     */
    EventLoop* getLoop() const { return loop_.get(); }

private:
    std::unique_ptr<EventLoop> loop_;
    std::string                loopThreadname_;
    std::thread                thread_;
};

/**
 * @brief 对TCP连接的抽象
 *
 * @details Server accept 时创建Connection，然后分发Connection到某个EventLoop，应用层业务使用 Connectoin
 * 提供的接口读写数据。在出现错误或连接关闭时，在所属的EventLoop执行清理(因为EventLoop可能还在使用Channel，在其他地方关闭会影响EventLoop中的使用)。
 */
class Connection
{
public:
    /**
     * @note Connection 在构造时即归属于loop
     */
    Connection(std::unique_ptr<Socket> sock, EventLoop* loop)
        : owner_loop_(loop)
    {
        channel_ = make_unique<Channel>(std::move(sock), EPOLLIN | EPOLLET);
        channel_->setReadCallback([this]() { this->handleRead(); });
        channel_->setWriteCallback([this]() { this->handleWrite(); });
        channel_->setErrorCallback([this]() { this->handleError(); });
        owner_loop_->runInLoop([this]() { this->owner_loop_->addChannel(this->channel_.get()); });
    }

    void setReadCallback(std::function<void(const char*, size_t)> cb) { read_cb_ = std::move(cb); }
    void setCloseCallback(std::function<void()> cb) { close_cb_ = std::move(cb); }
    void setErrorCallback(std::function<void()> cb) { error_cb_ = std::move(cb); }

    // 上层发送接口
    void sendResponse(HttpResponse&& resp)
    {
        std::string data = "HTTP/1.1 " + std::to_string(resp.status_code) + " " + resp.status_message + "\r\n";
        for (auto& h : resp.headers) data += h.first + ": " + h.second + "\r\n";
        data += "\r\n";
        data += resp.body;

        output_buffer_ += data;

        // 启用写监听
        channel_->setEvents(EPOLLIN | EPOLLOUT | EPOLLET);
        owner_loop_->runInLoop([this]() { owner_loop_->updateChannel(channel_.get()); });
    }

    // Channel 回调
    void handleRead()
    {
        while (true)
        {
            ssize_t n = ::read(channel_->fd(), input_buffer_, sizeof(input_buffer_));
            if (n > 0)
            {
                if (read_cb_)
                    read_cb_(input_buffer_, static_cast<size_t>(n));
            }
            else if (n == 0)
            {
                // 对端关闭
                if (close_cb_)
                    close_cb_();
                close();   // 从 epoll 移除（延迟到 doPendingTasks 执行）
                return;
            }
            else   // n < 0
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    break;   // 读完
                // 真正的错误
                if (error_cb_)
                    error_cb_();
                close();
                return;
            }
        }
    }

    void handleWrite()
    {
        while (!output_buffer_.empty())
        {
            ssize_t n = ::write(channel_->fd(), output_buffer_.data(), output_buffer_.size());
            if (n > 0)
            {
                output_buffer_.erase(0, n);
            }
            else if (n < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    break;   // 内核缓冲区满，等下次 EPOLLOUT
                if (error_cb_)
                    error_cb_();
                close();
                return;
            }
        }

        // 全部发完，关闭写监听
        if (output_buffer_.empty())
        {
            channel_->setEvents(EPOLLIN | EPOLLET);   // 去掉 EPOLLOUT
            owner_loop_->runInLoop([this]() { owner_loop_->updateChannel(channel_.get()); });
        }
    }
    void handleError()
    {
        if (error_cb_)
            error_cb_();
        close();
    }

    void close()
    {
        if (closed_)
            return;
        closed_ = true;
        // 使用 queueInLoop 而非 runInLoop：
        // 如果 close() 是在 handleRead 回调链中（如 onResponse → close）被调用，
        // 同步执行 doClose() 会销毁 channel_，而 handleRead 的 while 循环还在使用它。
        // 延迟到 doPendingTasks 执行，确保当前回调栈完全退出。
        owner_loop_->queueInLoop([this]() { doClose(); });
    }

    int fd() const { return channel_->fd(); }

private:
    std::unique_ptr<Channel> channel_;
    char                     input_buffer_[4096];
    std::string              output_buffer_;
    EventLoop*               owner_loop_;   // Connection 所属的 Eventloop_;

    std::function<void(const char*, size_t)> read_cb_;
    std::function<void()>                    close_cb_;
    std::function<void()>                    error_cb_;
    bool                                     closed_ = false;

    void doClose()
    {
        owner_loop_->removeChannel(channel_.get());
        channel_.reset();
    }
};

class HttpSession
{
public:
    HttpSession(Connection* conn, Router& router, std::shared_ptr<spdlog::logger> logger)
        : connection_(conn)
        , logger_(logger)
    {
        // HttpReqBuilder 的响应回调：序列化后交给 Connection 发送
        auto onResponse = [this](HttpResponse&& resp)
        {
            connection_->sendResponse(std::move(resp));
            if (!builder_->shouldKeepAlive())
            {
                connection_->close();   // Connection 会在 EventLoop 中安全关闭
            }
            else
            {
                // Keep-Alive: 重置 parser 和 builder 复用
                builder_->reset();
                parser_->reset();
                parser_->feed("", 0);   // 触发处理 buffer 中剩余数据
            }
        };

        builder_ = make_unique<HttpReqBuilder>(router, std::move(onResponse), logger);
        parser_ = make_unique<HttpParser>(builder_.get());

        // 设置 Connection 的回调
        connection_->setReadCallback([this](const char* data, size_t len) { this->onData(data, len); });
        connection_->setCloseCallback([this]() { this->onClose(); });
        connection_->setErrorCallback([this]() { this->onClose(); });
    }

private:
    void onData(const char* data, size_t len) { parser_->feed(data, len); }

    void onClose()
    {
        // 通知 Server 清理此 Session
        if (close_cb_)
            close_cb_(connection_->fd());
    }

public:
    void setCloseCallback(std::function<void(int)> cb) { close_cb_ = std::move(cb); }

private:
    Connection*                     connection_;   // 不拥有
    std::unique_ptr<HttpReqBuilder> builder_;
    std::unique_ptr<HttpParser>     parser_;
    std::shared_ptr<spdlog::logger> logger_;
    std::function<void(int)>        close_cb_;
};

class Acceptor
{
public:
    explicit Acceptor(std::string& listenAddr, int port, EventLoop* loop)
        : listAddr_(listenAddr)
        , port_(port)
        , loop_(loop)
    {
        std::unique_ptr<Socket> socket_ = make_unique<Socket>(::socket(AF_INET, SOCK_STREAM, 0));
        if (!socket_ || socket_->fd() < 0)
        {
            std::runtime_error("Failed to create socket: " + std::string(strerror(errno)));
        }

        socket_->setReuseAddr();
        socket_->setNonBlocking();

        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        if (inet_pton(AF_INET, listenAddr.c_str(), &addr.sin_addr) <= 0)
        {
            std::runtime_error("Invalid address or inet_pton error: " + listenAddr);
        }
        addr.sin_port = htons(port);
        if (!socket_->bind(addr))
        {
            std::runtime_error("socket bind failed; address: " + listAddr_ + ", port: " + std::to_string(port) + ", error code: " + std::to_string(socket_->getSocketError()) +
                               ", errmsg: " + std::string(strerror(errno)));
        }
        if (!socket_->listen(128))
        {
            std::runtime_error("Failed to listen on socket");
        }
        accept_channel_ = make_unique<Channel>(std::move(socket_), EPOLLIN | EPOLLET);
    }

    /**
     * @brief 在 Acceptor 的事件循环里面监听
     */
    void listen()
    {
        // set read callback to accept new Connection
        accept_channel_->setReadCallback(
            [this]()
            {
                struct sockaddr_in client_addr;
                socklen_t          client_len = sizeof(client_addr);
                int                fd = ::accept(accept_channel_->fd(), (struct sockaddr*)&client_addr, &client_len);
                if (fd < 0)
                {
                    if (errno != EAGAIN && errno != EWOULDBLOCK)
                        std::runtime_error("accept failed: " + std::string(strerror(errno)));
                    return;
                }

                int flags = fcntl(fd, F_GETFL, 0);
                fcntl(fd, F_SETFL, flags | O_NONBLOCK);

                if (newConnectionCallback_)
                {
                    newConnectionCallback_(fd);
                }
                else
                {
                    ::close(fd);
                }
            });

        // 将 accept channel 注册到主 EventLoop 的 epoll
        loop_->addChannel(accept_channel_.get());
    }

    void close() { ::shutdown(accept_channel_->fd(), SHUT_WR); }
    void setNewConnectionCallback(std::function<void(int)> cb) { newConnectionCallback_ = std::move(cb); }

    std::string getListenAddr() const { return listAddr_; }
    int         getPort() const { return port_; }

private:
    std::string              listAddr_;
    int                      port_;
    EventLoop*               loop_;
    std::unique_ptr<Channel> accept_channel_;
    std::function<void(int)> newConnectionCallback_;
};

class Server
{
public:
    Server(std::string addr, int port, std::string static_dir = "web_root", int pool_size = 4, int sub_reactor_number = 4)
        : static_dir_(std::move(static_dir))
        , running_(false)
    {
        logger_ = spdlog::basic_logger_mt("single_reactor_Server_Logger", "logs/single_reactor_server.log");
        router_ = register_router(static_dir_);
        thread_pool_ = make_unique<ThreadPool>(pool_size);
        main_loop_ = make_unique<EventLoop>();
        acceptor_ = make_unique<Acceptor>(addr, port, main_loop_.get());
        for (int i = 0; i < sub_reactor_number; i++)
        {
            io_threads_.push_back(make_unique<EventLoopThread>("EventLoopThread " + std::to_string(i)));
        }
    }

    ~Server() { stop(); }

    bool start();
    void stop();

private:
    std::string                                   static_dir_;
    bool                                          running_;
    std::unique_ptr<Router>                       router_;
    std::shared_ptr<spdlog::logger>               logger_;
    std::unique_ptr<ThreadPool>                   thread_pool_;
    std::thread                                   server_thread;   // 服务器主线程
    std::unique_ptr<EventLoop>                    main_loop_;
    std::vector<std::unique_ptr<EventLoopThread>> io_threads_;
    std::unique_ptr<Acceptor>                     acceptor_;
    std::map<int, std::unique_ptr<Connection>>    connections_;   // fd -> connection
    std::map<int, std::unique_ptr<HttpSession>>   sessions_;      // fd -> session

    void       run();
    bool       setup_socket();   // 创建并设置 socket
    void       new_connection(int fd);
    void       handle_client(int client_fd);   // 处理新连接上的请求
    void       remove_connection();            // 清理连接
    EventLoop* getNextEventloop();

    std::unique_ptr<Router>  register_router(std::string& dir);                    // 注册路由
    std::vector<std::string> get_html_files_recursively(const std::string& dir);   // 注册路由的辅助函数
};

bool Server::start()
{
    running_ = true;
    server_thread = std::thread(&Server::run, this);
    logger_->info("Server started on {}:{}", acceptor_->getListenAddr(), acceptor_->getPort());
    return true;
}

void Server::stop()
{
    if (running_)
    {
        running_ = false;
        main_loop_->quit();
        for (auto& reactor : io_threads_)
        {
            reactor->wait();
        }


        acceptor_->close();

        // 等待 server_thread 线程退出
        if (server_thread.joinable())
        {
            server_thread.join();
        }

        logger_->info("Server stopped");
        spdlog::shutdown();
    }
}

void Server::run()
{
    // 1. 启动所有 Sub Reactor 线程，主线程阻塞直到每个 EventLoop 就绪
    for (auto& t : io_threads_)
    {
        t->run();
    }

    // 2. 将 Acceptor 的 listen channel 注册到 main_loop，启动主事件循环
    acceptor_->setNewConnectionCallback([this](int fd) { this->new_connection(fd); });
    acceptor_->listen();
    main_loop_->loop();   // 阻塞直到 quit
}

void Server::new_connection(int fd)
{
    EventLoop* loop = getNextEventloop();
    auto       client_sock = make_unique<Socket>(fd);
    auto       conn = make_unique<Connection>(std::move(client_sock), loop);        // Connection 创建时自动注册到 EventLoop
    auto       session = make_unique<HttpSession>(conn.get(), *router_, logger_);   // 把Connection绑定到Session
    session->setCloseCallback(
        [this](int fd)
        {
            // 关闭回调：在 main_loop 中清理连接和会话
            main_loop_->queueInLoop(
                [this, fd]()
                {
                    connections_.erase(fd);
                    sessions_.erase(fd);
                });
        });

    connections_[fd] = std::move(conn);
    sessions_[fd] = std::move(session);
}

EventLoop* Server::getNextEventloop()
{
    static int i = 0;
    EventLoop* next = io_threads_[i]->getLoop();
    i = (i + 1) % io_threads_.size();
    return next;
}

/**
 * 注册路由
 *
 * @details
 *  扫描指定目录下的所有html文件，注册为静态路由
 *
 * @return 路由对象指针
 */
std::unique_ptr<Router> Server::register_router(std::string& dir)
{
    std::vector<std::string> html_files = get_html_files_recursively(dir);
    std::unique_ptr<Router>  router(new Router());
    router->addRoute(HttpMethod::GET, "/", [dir, this]() { return std::unique_ptr<RequestHandler>(new HtmlFileHandler(dir + "/index.html", logger_)); });
    for (const auto& file_path : html_files)
    {
        router->addRoute(HttpMethod::GET,
                         file_path.substr(dir.size()),   // 去掉前缀目录
                         [file_path, this]() { return std::unique_ptr<RequestHandler>(new HtmlFileHandler(file_path, logger_)); });
    }
    return router;
}

/**
 * 注册路由的辅助函数
 *
 * @details 扫描指定目录及其子目录，获取所有HTML文件的路径
 *
 * @param dir 目录路径
 *
 * @return HTML文件路径列表
 */
std::vector<std::string> Server::get_html_files_recursively(const std::string& dir)
{
    std::vector<std::string> html_files;
    std::stack<std::string>  dirs;
    dirs.push(dir);

    while (!dirs.empty())
    {
        std::string cur_dir = dirs.top();
        dirs.pop();

        DIR* dir = opendir(cur_dir.c_str());
        if (dir == nullptr)
        {
            logger_->error("Failed to open directory: {}", cur_dir);
            continue;
        }

        struct dirent* entry;
        while ((entry = readdir(dir)) != nullptr)
        {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            {
                continue;   // Skip . and ..
            }
            std::string full_path = cur_dir + "/" + entry->d_name;
            if (entry->d_type == DT_DIR)
            {
                dirs.push(full_path);   // Push subdirectory to stack
            }
            else if (entry->d_type == DT_REG)
            {
                if (full_path.size() >= 5 && full_path.substr(full_path.size() - 5) == ".html")
                {
                    html_files.push_back(full_path);
                }
            }
        }
        closedir(dir);
    }
    return html_files;
}


bool g_quit = false;   // 全局退出标志

/**
 * sigint 的信号处理函数
 */
void sigint_handler(int signum)
{
    (void)signum;   // unused
    g_quit = true;
}

using handler_t = void (*)(int);
/** TODO: 提取为公共函数 */
/**
 * 为指定信号注册信号处理函数
 */
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

void usage(const char* prog)
{
    std::cout << "Usage: " << prog << " [options]\n"
              << "Options:\n"
              << "  -a <address>          bind address (default 127.0.0.1)\n"
              << "  -p <port>             target port (default 7788)\n"
              << "  -t <thread_pool_size> set thread pool size (default 4)\n"
              << "  -r <sub_reactor_number> set sub Reactor number (default 4)\n"
              << "  -h <help>             display this help message\n";
}

int main(int argc, char* argv[])
{
    spdlog::set_level(spdlog::level::debug);
    Signal(SIGINT, sigint_handler);

    std::string address = "127.0.0.1";
    int         port = 7788;
    int         pool_size = 4;
    int         reactor_number = 4;

    int opt;
    while ((opt = getopt(argc, argv, "a:p:t:h")) != -1)
    {
        switch (opt)
        {
            case 'a':
                address = optarg;
                break;
            case 'p':
                port = std::stoi(optarg);
                break;
            case 't':
                pool_size = std::stoi(optarg);
                break;
            case 'r':
                reactor_number = std::stoi(optarg);
                break;
            case 'h':
                usage(argv[0]);
                return 0;
            default:
                usage(argv[0]);
                return 1;
        }
    }

    Server server(address, port, "web_root", pool_size, reactor_number);
    if (!server.start())
    {
        std::cout << "Failed to start server" << std::endl;
        return 1;
    }
    std::cout << "Press Ctrl+C to stop the server..." << std::endl;

    while (!g_quit)
    {
        pause();   // 等待信号
    }
    std::cout << "Received SIGINT, stopping server..." << std::endl;
    server.stop();
    return 0;
}