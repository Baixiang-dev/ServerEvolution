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

struct Connection
{
    Socket                          client_sock;
    std::unique_ptr<HttpReqBuilder> http_builder;
    std::unique_ptr<HttpParser>     http_parser;

    Connection(Socket&& sock, Router& r, std::shared_ptr<spdlog::logger> logger)
        : client_sock(std::move(sock))
    {
        /** TODO:
         * 1. Socket 或 fd 的所有权问题
         *  更合理的应该是 Connection 持有 Socket(fd) 的唯一所有权，其他需要 Socket 的实际执行发送/读取的对象都通过Connection 来获取访问权限
         * 2. 对 Socket 的 发送/读取， 也就是对 IO 的处理应该统一在一个地方，而不是分散各处
         */
        http_builder = make_unique<HttpReqBuilder>(r, client_sock.fd(), logger);

        http_parser = make_unique<HttpParser>(http_builder.get());
    }
};

/**
 * @brief 绑定fd感兴趣的事件，和事件对应的处理函数，传递给 epoll，供 epoll 监听 fd 和 分发事件
 */
class Channel
{
public:
    Channel(int fd, int events)
        : fd_(fd)
        , events_(events)
        , revents_(0)
    {
    }

    void setReadCallback(std::function<void()> cb) { readCallback_ = std::move(cb); }
    void setWriteCallback(std::function<void()> cb) { writeCallback_ = std::move(cb); }
    void setErrorCallback(std::function<void()> cb) { errorCallback_ = std::move(cb); }

    int  fd() const { return fd_; }
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
    int fd_;

    // 用户感兴趣的事件，以及 epoll 返回的事件
    int events_;
    int revents_;

    // 事件发生时的回调函数
    std::function<void()> readCallback_;
    std::function<void()> writeCallback_;
    std::function<void()> errorCallback_;
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
    std::map<int, std::unique_ptr<Connection>> connections_;    // fd -> connection
    std::map<int, std::unique_ptr<Channel>>    channels_;       // fd -> channel
    std::vector<int>                           to_remove;       // 需要移除的 fd 列表，依据列表移除对应的 connection 和 channel
    int                                        epfd_;

    void run();
    bool setup_socket();   // 创建并设置 socket
    void accept_connection();
    void handle_client(int client_fd);   // 处理新连接上的请求

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

    epfd_ = epoll_create1(0);
    if (epfd_ == -1)
    {
        logger_->error("epoll_create1 failed: {}", strerror(errno));
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
    struct epoll_event ev;
    ev.data.fd = socket_->fd();
    ev.events = EPOLLIN;


    if (epoll_ctl(epfd_, EPOLL_CTL_ADD, socket_->fd(), &ev) == -1)
    {
        logger_->error("epoll_ctl failed: {}", strerror(errno));
        return;
    }


    struct epoll_event events[64];
    while (running_)
    {
        int n = epoll_wait(epfd_, events, 64, 1000);

        for (int i = 0; i < n; ++i)
        {
            int fd = events[i].data.fd;
            if (fd == socket_->fd())
            {
                accept_connection();
            }
            else
            {
                auto ch = channels_.find(fd);
                if (ch == channels_.end())
                {
                    logger_->error("Channel not found for fd: {}", fd);
                    continue;
                }
                else
                {
                    ch->second->set_revents(events[i].events);
                    ch->second->handleEvent();
                }
            }
        }
        // 释放已标记的 channel 和 connection
        for (int fd : to_remove)
        {
            channels_.erase(fd);
            connections_.erase(fd);
        }
        to_remove.clear();
    }
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
    struct sockaddr_in client_addr;
    socklen_t          client_len = sizeof(client_addr);
    Socket             client_sock(::accept(socket_->fd(), (struct sockaddr*)&client_addr, &client_len));

    if (client_sock.fd() < 0)
    {
        if (errno != EAGAIN && errno != EWOULDBLOCK)
            logger_->error("accept failed: {}", strerror(errno));
        return;
    }

    client_sock.setNonBlocking();
    int client_fd = client_sock.fd();

    // 创建 Connection，接管 client_sock 的所有权
    auto conn = make_unique<Connection>(std::move(client_sock), *router_, logger_);

    // 把 fd 和 fd 感兴趣的事件绑定到 channel 上，设置事件的回调函数
    auto chan = make_unique<Channel>(client_fd, EPOLLIN | EPOLLET);
    chan->setReadCallback([this, fd = client_fd]() { this->handle_client(fd); });

    // 注册 channel 到 epoll
    struct epoll_event ev;
    ev.data.fd = chan->fd();
    ev.events = chan->events();
    epoll_ctl(epfd_, EPOLL_CTL_ADD, chan->fd(), &ev);

    channels_[chan->fd()] = std::move(chan);
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
            epoll_ctl(epfd_, EPOLL_CTL_DEL, client_fd, nullptr);
            to_remove.push_back(client_fd);   // 标记需要移除的 fd
            return;
        }
        else if (count == 0)
        {
            logger_->debug("Client closed connection: fd {}", client_fd);
            epoll_ctl(epfd_, EPOLL_CTL_DEL, client_fd, nullptr);
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
                    epoll_ctl(epfd_, EPOLL_CTL_DEL, client_fd, nullptr);
                    to_remove.push_back(client_fd);
                    break;
                }
            }
        }
    }
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