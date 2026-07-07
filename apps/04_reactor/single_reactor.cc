/**
 * 单线程单 Reactor 模型
 *
 * 在Reactor模型里面需要做的几件事：
 * 1. 事件分发，把IO事件分发给对应的处理者
 */
#include <dirent.h>
#include <iostream>
#include <map>
#include <memory>
#include <stack>
#include <string>
#include <sys/epoll.h>
#include <sys/signal.h>

#include "http/HttpBuilder.h"
#include "http/handlers/HttpHandlers.h"
#include "http/router/HttpRouter.h"
#include "io/socket/Socket.h"

#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"

/** 自定义 make_unique 方便使用 */
template<typename T, typename... Args> std::unique_ptr<T> make_unique(Args&&... args)
{
    return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

/**
 * @brief 对单个 fd 状态的封装，由 Connection 持有
 */
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

struct Connection
{
    // Socket                          client_sock;
    std::unique_ptr<HttpReqBuilder> http_builder;
    std::unique_ptr<HttpParser>     http_parser;
    std::unique_ptr<Channel>        channel;   // channel 由 Connection 持有，在 Epoller 和 EventLoop 中使用 raw pointer 做非拥有访问

    Connection(std::unique_ptr<Channel> chan, Router& r, std::shared_ptr<spdlog::logger> logger)
        : channel(std::move(chan))
    {
        /** TODO:
         * 对 Socket 的 发送/读取， 也就是对 IO 的处理应该统一在一个地方，而不是分散各处
         */
        http_builder = make_unique<HttpReqBuilder>(r, channel->fd(), logger);

        http_parser = make_unique<HttpParser>(http_builder.get());
    }
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
    }
    ~EventLoop() {}

    void loop()
    {
        while (!quit_)
        {
            auto activated_channels = epoller_.wait(1000);
            for (Channel* chan : activated_channels)
            {
                chan->handleEvent();
            }
            afterLoopTask_();
        }
    }

    void quit() { quit_ = true; }

    void addChannel(Channel* ch) { epoller_.addChannel(ch); }
    void updateChannel(Channel* ch) { epoller_.updateChannel(ch); }
    void removeChannel(Channel* ch) { epoller_.removeChannel(ch); }
    void pendingTask(std::function<void()> cb) { afterLoopTask_ = cb; }

private:
    Epoller               epoller_;
    bool                  quit_ = false;
    std::function<void()> afterLoopTask_;   // 在每轮事件循环结尾需要执行的操作。
};

class Server
{
public:
    Server(std::string addr, int port, std::string static_dir = "web_root")
        : address_(std::move(addr))
        , port_(port)
        , static_dir_(std::move(static_dir))
        , running_(false)
    {
        logger_ = spdlog::basic_logger_mt("single_reactor_Server_Logger", "logs/single_reactor_server.log");
        router_ = register_router(static_dir_);
    }

    ~Server() { stop(); }

    bool start();
    void stop();

private:
    std::string                                address_;
    int                                        port_;
    std::string                                static_dir_;
    bool                                       running_;
    std::unique_ptr<Socket>                    socket_;
    std::unique_ptr<Router>                    router_;
    std::shared_ptr<spdlog::logger>            logger_;
    std::thread                                server_thread;   // 服务器主线程
    EventLoop                                  event_loop_;
    std::map<int, std::unique_ptr<Connection>> connections_;   // fd -> connection
    std::vector<int>                           to_remove;      // 需要移除的 fd 列表，依据列表移除对应的 connection 和 channel

    void run();
    bool setup_socket();   // 创建并设置 socket
    void accept_connection();
    void handle_client(int client_fd);   // 处理新连接上的请求
    void remove_connection();            // 清理连接

    std::unique_ptr<Router>  register_router(std::string& dir);                    // 注册路由
    std::vector<std::string> get_html_files_recursively(const std::string& dir);   // 注册路由的辅助函数
};

bool Server::start()
{
    if (!setup_socket())
    {
        logger_->error("Failed to setup socket");
        return false;
    }

    if (!socket_->listen(128))
    {
        logger_->error("Failed to listen on socket");
        return false;
    }

    running_ = true;
    server_thread = std::thread(&Server::run, this);
    logger_->info("Server started on {}:{}", address_, port_);
    return true;
}

void Server::stop()
{
    if (running_)
    {
        running_ = false;
        event_loop_.quit();

        ::shutdown(socket_->fd(), SHUT_WR);
        socket_->close();

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
    // 把 listen fd 封装成 Channel，统一到 EventLoop 中处理
    Channel listen_chan = Channel(socket_->fd(), EPOLLIN);
    listen_chan.setReadCallback([this]() { this->accept_connection(); });
    event_loop_.addChannel(&listen_chan);
    event_loop_.pendingTask([this]() { this->remove_connection(); });

    event_loop_.loop();
}

bool Server::setup_socket()
{
    socket_ = make_unique<Socket>(::socket(AF_INET, SOCK_STREAM, 0));
    if (!socket_ || socket_->fd() < 0)
    {
        logger_->error("Failed to create socket: {}", strerror(errno));
        return false;
    }

    socket_->setReuseAddr();
    socket_->setNonBlocking();

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, address_.c_str(), &addr.sin_addr) <= 0)
    {
        logger_->error("Invalid address or inet_pton error: {}", address_);
        return false;
    }
    addr.sin_port = htons(port_);
    if (!socket_->bind(addr))
    {
        logger_->error("socket bind failed; address: {}, port: {}, error code: {}, errmsg: {}", address_, port_, socket_->getSocketError(), strerror(errno));
        return false;
    }

    return true;
}

void Server::accept_connection()
{
    struct sockaddr_in      client_addr;
    socklen_t               client_len = sizeof(client_addr);
    std::unique_ptr<Socket> client_sock = make_unique<Socket>(::accept(socket_->fd(), (struct sockaddr*)&client_addr, &client_len));

    if (client_sock->fd() < 0)
    {
        if (errno != EAGAIN && errno != EWOULDBLOCK)
            logger_->error("accept failed: {}", strerror(errno));
        return;
    }

    client_sock->setNonBlocking();
    int client_fd = client_sock->fd();

    // 把 fd 和 fd 感兴趣的事件绑定到 channel 上，设置事件的回调函数
    auto chan = make_unique<Channel>(std::move(client_sock), EPOLLIN | EPOLLET);
    chan->setReadCallback([this, fd = client_fd]() { this->handle_client(fd); });

    // 创建 Connection，接管 channel 的所有权
    auto conn = make_unique<Connection>(std::move(chan), *router_, logger_);
    // 注册 channel 到 epoll
    event_loop_.addChannel(conn->channel.get());
    connections_[client_fd] = std::move(conn);
}

/** TODO: callback 里面如何访问 channel 的 fd
 * 答：channel 里面传递 fd 给 callback
 */
void Server::handle_client(int client_fd)
{
    auto it = connections_.find(client_fd);
    if (it == connections_.end())
    {
        logger_->error("Connection not found for fd: {}", client_fd);
        return;
    }

    auto& ctx = it->second;
    char  buffer[4096];
    while (true)
    {
        ssize_t count = read(client_fd, buffer, sizeof(buffer));

        if (count == -1)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                break;
            }
            logger_->error("Read error on {} : {}", client_fd, strerror(errno));
            event_loop_.removeChannel(it->second->channel.get());
            to_remove.push_back(client_fd);   // 标记需要移除的 fd
            return;
        }
        else if (count == 0)
        {
            logger_->debug("Client closed connection: fd {}", client_fd);
            // epoll_ctl(epfd_, EPOLL_CTL_DEL, client_fd, nullptr);
            event_loop_.removeChannel(it->second->channel.get());
            to_remove.push_back(client_fd);
            return;
        }
        else
        {
            ctx->http_parser->feed(buffer, static_cast<size_t>(count)); /** TODO: 参数风格统一，ssize */
            if (ctx->http_builder->isDone())
            {
                if (ctx->http_builder->shouldKeepAlive())
                {
                    logger_->debug("Keep-Alive for {}, resetting parser", client_fd);
                    ctx->http_builder->reset();
                    ctx->http_parser->reset();

                    // 触发 Parser 处理 buffer 中剩余的数据（如果有）
                    ctx->http_parser->feed("", 0);
                }
                else
                {
                    logger_->debug("Closing connection: fd {}", client_fd);
                    // epoll_ctl(epfd_, EPOLL_CTL_DEL, client_fd, nullptr);
                    event_loop_.removeChannel(it->second->channel.get());
                    to_remove.push_back(client_fd);
                    break;
                }
            }
        }
    }
}

/**
 * @brief 每轮事件循环的结束清理连接
 */
void Server::remove_connection()
{
    for (int fd : to_remove)
    {
        connections_.erase(fd);
    }
    to_remove.clear();
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
              << "  -a <address>     bind address (default 127.0.0.1)\n"
              << "  -p <port>        target port (default 7788)\n"
              << "  -h <help>        display this help message\n";
}

int main(int argc, char* argv[])
{
    spdlog::set_level(spdlog::level::debug);
    Signal(SIGINT, sigint_handler);

    std::string address = "127.0.0.1";
    int         port = 7788;

    int opt;
    while ((opt = getopt(argc, argv, "a:p:h")) != -1)
    {
        switch (opt)
        {
            case 'a':
                address = optarg;
                break;
            case 'p':
                port = std::stoi(optarg);
                break;
            case 'h':
                usage(argv[0]);
                return 0;
            default:
                usage(argv[0]);
                return 1;
        }
    }

    Server server(address, port);
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