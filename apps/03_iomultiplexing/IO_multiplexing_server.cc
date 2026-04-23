#include <dirent.h>
#include <iostream>
#include <map>
#include <memory>
#include <signal.h>
#include <stack>
#include <string>
#include <sys/epoll.h>

// Third-party code
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include "http/HttpBuilder.h"
#include "http/handlers/HttpHandlers.h"
#include "http/parser/HttpParser.h"
#include "http/router/HttpRouter.h"
#include "io/socket/Socket.h"

/**
 * Http连接的抽象, 用于IO多路复用中管理连接状态
 */
struct Connection
{
    Socket                          client_sock_;    // 与客户端建立连接的 socket
    std::unique_ptr<HttpReqBuilder> http_builder_;   // HTTP请求构建器
    std::unique_ptr<HttpParser>     http_parser_;    // HTTP请求解析器

    Connection(Socket&& sock, Router& r, std::shared_ptr<spdlog::logger> logger)
        : client_sock_(std::move(sock))
    {
        http_builder_ =
            std::unique_ptr<HttpReqBuilder>(new HttpReqBuilder(r, client_sock_.fd(), logger));
        http_parser_ = std::unique_ptr<HttpParser>(new HttpParser(http_builder_.get()));
    }
    ~Connection()
    {
        if (client_sock_.fd() >= 0)
        {
            client_sock_.close();
        }
    }
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
        logger_ = spdlog::basic_logger_mt("io_multiplexing_Server_Logger",
                                          "logs/io_multiplexing_server.log");
        router_ = register_router(static_dir_);
    }

    ~Server() { stop(); }

    bool start();
    void stop();

private:
    std::string                     address_;      // 服务器监听地址
    int                             port_;         // 监听端口
    std::string                     static_dir_;   // 静态资源目录
    bool                            running_;      // 服务器运行状态
    std::unique_ptr<Socket>         socket_;       // 服务器监听Socket
    std::unique_ptr<Router>         router_;       // 路由器
    std::shared_ptr<spdlog::logger> logger_;
    std::thread                     server_thread;   // 服务器主线程

    bool setup_socket();        // 创建并设置 socket
    void accept_connection();   // 侦听并接收连接

    void accept_connection_select();
    void accept_connection_poll();
    void accept_connection_epoll();

    std::unique_ptr<Router>  register_router(std::string& dir);   // 注册路由
    std::vector<std::string> get_html_files_recursively(
        const std::string& dir);   // 注册路由的辅助函数
};

/**
 * 启动服务器
 */
bool Server::start()
{
    if (!setup_socket())
    {
        logger_->error("Failed to setup socket");
        return false;
    }

    if (socket_->listen(128) == false)
    {
        logger_->error("socket listen failed: error code {}, errmsg {}",
                       socket_->getSocketError(),
                       strerror(errno));
        return false;
    }

    running_ = true;
    server_thread = std::thread(&Server::accept_connection, this);
    logger_->info("Server started on {}:{}", address_, port_);
    return true;
}

/**
 * 停止服务器
 */
void Server::stop()
{
    if (running_)
    {
        running_ = false;

        ::shutdown(socket_->fd(), SHUT_WR);
        socket_->close();

        // 等待 accept_thread 线程退出
        if (server_thread.joinable())
        {
            server_thread.join();
        }

        logger_->info("Server stopped");
        spdlog::shutdown();
    }
}

/**
 * 创建并设置Socket
 *
 * @details
 *  创建Socket，设置地址重用选项，绑定地址和端口
 */
bool Server::setup_socket()
{
    socket_ = std::unique_ptr<Socket>(new Socket(::socket(AF_INET, SOCK_STREAM, 0)));
    if (!socket_ || socket_->fd() < 0)
    {
        logger_->error("socket creation failed: error code {}, errmsg {}",
                       socket_->getSocketError(),
                       strerror(errno));
        return false;
    }

    socket_->setReuseAddr();

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, address_.c_str(), &addr.sin_addr) <= 0)
    {
        logger_->error("Invalid addr or inet_pton error: {}", address_);
        return false;
    }
    addr.sin_port = htons(port_);
    if (!socket_->bind(addr))
    {
        logger_->error("socket bind failed; address: {}, port: {}, error code: {}, errmsg: {}",
                       address_,
                       port_,
                       socket_->getSocketError(),
                       strerror(errno));
        return false;
    }
    return true;
}

/**
 * 接收并处理新连接
 */
void Server::accept_connection()
{
    /** 使用select / epoll / poll 循环处理 */
    accept_connection_epoll();
}

/**
 * 使用 select 处理连接
 */
void Server::accept_connection_select() {}

/**
 * 使用 poll 处理连接
 */
void Server::accept_connection_poll() {}

/**
 * 使用 epoll 处理连接
 */
void Server::accept_connection_epoll()
{
    int epoll_fd = epoll_create1(0);
    if (epoll_fd == -1)
    {
        logger_->error("epoll_create1 failed: {}", strerror(errno));
        return;
    }

    struct epoll_event event;
    event.events = EPOLLIN;
    event.data.fd = socket_->fd();

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socket_->fd(), &event) == -1)
    {
        logger_->error("epoll_ctl ADD failed: {}", strerror(errno));
        close(epoll_fd);
        return;
    }

    std::map<int, std::unique_ptr<Connection>> connections;
    struct epoll_event                         events[64];

    while (running_)
    {
        // 等待事件，超时时间， 以便检查 running_ 状态
        int n = epoll_wait(epoll_fd, events, 64, 1000);

        for (int i = 0; i < n; ++i)
        {
            int fd = events[i].data.fd;

            if (fd == socket_->fd())   // 处理新连接
            {
                struct sockaddr_in client_addr;
                socklen_t          client_len = sizeof(client_addr);
                Socket client_sock(::accept(fd, (struct sockaddr*)&client_addr, &client_len));

                if (client_sock.fd() < 0)
                {
                    if (errno != EAGAIN && errno != EWOULDBLOCK)
                        logger_->error("socket creation failed: error code {}, errmsg {}",
                                       client_sock.getSocketError(),
                                       strerror(errno));
                    continue;
                }

                // 设置客户端文件描述符非阻塞
                client_sock.setNonBlocking();

                event.events = EPOLLIN | EPOLLET;
                event.data.fd = client_sock.fd();
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_sock.fd(), &event) == -1)
                {
                    logger_->error("epoll_ctl ADD client failed: {}", strerror(errno));
                    client_sock.close();
                    continue;
                }

                logger_->debug("New Connection: {}", client_sock.fd());

                int client_fd = client_sock.fd();
                connections[client_fd] = std::unique_ptr<Connection>(
                    new Connection(std::move(client_sock), *router_, logger_));
            }
            else   // 处理连接中的数据
            {
                if (connections.find(fd) == connections.end())
                    continue;

                auto& ctx = connections[fd];
                char  buffer[4096];
                bool  close_conn = false;

                while (true)   // 边缘触发模式，循环处理直到 EAGAIN
                {
                    ssize_t count = read(fd, buffer, sizeof(buffer));

                    if (count == -1)
                    {
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                        {
                            break;
                        }
                        logger_->error("Read error on {} : {}", fd, strerror(errno));
                        close_conn = true;
                        break;
                    }
                    else if (count == 0)
                    {
                        close_conn = true;
                        break;
                    }

                    ctx->http_parser_->feed(buffer, count);
                    if (ctx->http_builder_->isDone())
                    {
                        if (ctx->http_builder_->shouldKeepAlive())
                        {
                            logger_->debug("Keep-Alve for {}, resetting parser", fd);
                            ctx->http_builder_->reset();
                            ctx->http_parser_->reset();

                            // 触发 Parser 处理 buffer 中剩余的数据（如果有）
                            ctx->http_parser_->feed("", 0);
                        }
                        else   // 不支持 Keep-Alive，关闭连接
                        {
                            close_conn = true;
                            break;
                        }
                    }
                }

                if (close_conn)
                {
                    logger_->debug("Closing Connection: {}", fd);
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                    connections.erase(fd);   // connection 析构时 close socket
                }
            }
        }
    }

    close(epoll_fd);
    connections.clear();   // 关闭所有连接
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
    router->addRoute(HttpMethod::GET,
                     "/",
                     [dir, this]() {
                         return std::unique_ptr<RequestHandler>(
                             new HtmlFileHandler(dir + "/index.html", logger_));
                     });
    for (const auto& file_path : html_files)
    {
        router->addRoute(
            HttpMethod::GET,
            file_path.substr(dir.size()),   // 去掉前缀目录
            [file_path, this]()
            { return std::unique_ptr<RequestHandler>(new HtmlFileHandler(file_path, logger_)); });
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

// 全局退出标识
std::atomic<bool> g_quit(false);

using handler_t = void (*)(int);

/* TODO: 提取为公共函数 */
/**
 * 为指定信号注册信号处理函数
 */
handler_t Singal(int signum, handler_t handler)
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

/**
 * sigint 的信号处理函数
 */
void sigint_handler(int signum)
{
    (void)signum;   // unused
    g_quit = true;
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
    Singal(SIGINT, sigint_handler);
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
        pause();
    }

    std::cout << "Received SIGINT, shutting down server..." << std::endl;
    server.stop();
    return 0;
}