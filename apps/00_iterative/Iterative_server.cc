#include <arpa/inet.h>
#include <dirent.h>
#include <getopt.h>
#include <iostream>
#include <signal.h>
#include <stack>
#include <vector>

// Third-party code
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include "http/HttpBuilder.h"
#include "http/handlers/HttpHandlers.h"
#include "http/parser/HttpParser.h"
#include "http/router/HttpRouter.h"
#include "io/socket/Socket.h"

/**
 * 迭代式HTTP服务器实现
 *
 * @details
 * 一个使用使用阻塞式I/O模型迭代处理请求的HTTP服务器，支持短连接，支持静态HTML文件的GET请求处理
 */

class Server
{
public:
    Server(std::string address, int port, std::string static_dir = "web_root")
        : address_(std::move(address))
        , port_(port)
        , static_dir_(std::move(static_dir))
        , running_(false)
    {
        logger_ = spdlog::basic_logger_mt("iterative_Server_Logger", "logs/iterative_server.log");
        router_ = RegisterRouter(static_dir_);
    }
    ~Server() { Stop(); }

    bool Start();
    void Stop();

private:
    std::string                     address_;         // 监听地址
    int                             port_;            // 监听端口
    std::string                     static_dir_;      // 静态文件目录
    bool                            running_;         // 服务器运行状态
    std::unique_ptr<Socket>         socket_;          // 服务器Socket
    std::unique_ptr<Router>         router_;          // 路由器
    std::shared_ptr<spdlog::logger> logger_;          // 日志器
    std::thread                     accept_thread_;   // 服务器主线程

    bool SetupSocket();                                       // 创建并设置socket
    void AcceptConnection();                                  // 侦听并接受连接
    void HandleNewConnection(std::unique_ptr<Socket> client_sock);   // 处理新连接

    std::unique_ptr<Router>  RegisterRouter(std::string& dir);                    // 注册路由
    std::vector<std::string> GetHtmlFilesRecursively(const std::string& dir);   // 注册路由的辅助函数
};

/**
 * 启动服务器
 */
bool Server::Start()
{
    if (!SetupSocket())
    {
        logger_->error("Failed to setup socket");
        return false;
    }

    if (socket_->listen(128) == false)
    {
        logger_->error("socket listen failed: error code {}, errmsg {}", socket_->getSocketError(), strerror(errno));
        return false;
    }

    running_ = true;
    accept_thread_ = std::thread(&Server::AcceptConnection, this);
    logger_->info("Server started on {}:{}", address_, port_);

    // unreached
    return true;
}

/**
 * 停止服务器
 */
void Server::Stop()
{
    if (running_)
    {
        running_ = false;

        // 关闭Server监听的文件描述符
        ::shutdown(socket_->fd(), SHUT_RDWR);
        socket_->close();

        // 等待线程退出
        if (accept_thread_.joinable())
        {
            accept_thread_.join();
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
bool Server::SetupSocket()
{
    socket_ = std::unique_ptr<Socket>(new Socket(::socket(AF_INET, SOCK_STREAM, 0)));
    if (!socket_ || socket_->fd() < 0)
    {
        logger_->error("socket creation failed: error code {}, errmsg {}", socket_->getSocketError(), strerror(errno));
        return false;
    }

    socket_->setReuseAddr();

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    if (inet_pton(AF_INET, address_.c_str(), &address.sin_addr) <= 0)
    {
        logger_->error("Invalid address or inet_pton error: {}", address_);
        return false;
    }
    address.sin_port = htons(port_);
    if (!socket_->bind(address))
    {
        logger_->error("socket bind failed; address: {}, port: {}, error code: {}, errmsg: {}", address_, port_, socket_->getSocketError(), strerror(errno));
        return false;
    }
    return true;
}

/**
 * 接收新连接并调用handle_client处理
 */
void Server::AcceptConnection()
{
    while (running_)
    {
        struct sockaddr_in      client_addr;
        socklen_t               addr_len = sizeof(client_addr);
        std::unique_ptr<Socket> client_sock = std::unique_ptr<Socket>(new Socket(socket_->accept(&client_addr, &addr_len)));
        if (!client_sock || client_sock->fd() < 0)
        {
            logger_->error("Accept failed: {}", strerror(errno));
            continue;
        }
        HandleNewConnection(std::move(client_sock));
    }
}

void Server::HandleNewConnection(std::unique_ptr<Socket> client_sock)
{
    char buffer[1024] = {0};

    while (true)
    {
        ssize_t nread = ::recv(client_sock->fd(), buffer, sizeof(buffer), 0);

        if (nread > 0)
        {
            std::string msg(buffer, static_cast<size_t>(nread));
            logger_->debug("Received message: [{}]", msg);
            HttpReqBuilder req_builder(*router_, client_sock->fd(), logger_);
            HttpParser     parser(&req_builder);

            parser.feed(msg.data(), msg.size());
            if (req_builder.isDone())
            {
                break;
            }
            continue;
        }
        else if (nread == 0)   // 对端有序关闭（FIN）
        {
            logger_->debug("Client closed connection (peer performed orderly shutdown)");
            break;
        }
        else   // nread < 0，异常关闭
        {
            // 遇到错误，记录并结束处理
            logger_->warn("recv failed {}", std::strerror(errno));
            break;
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
std::unique_ptr<Router> Server::RegisterRouter(std::string& dir)
{
    std::vector<std::string> html_files = GetHtmlFilesRecursively(dir);
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
std::vector<std::string> Server::GetHtmlFilesRecursively(const std::string& dir)
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

using handler_t = void (*)(int);
// 前向声明
handler_t Signal(int signum, handler_t handler);
void      SigintHandler(int signum);
void      Usage(const char* prog);
// 全局退出标志
std::atomic<bool> g_quit(false);

int main(int argc, char* argv[])
{
    Signal(SIGINT, SigintHandler);
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
                Usage(argv[0]);
                return 0;
            default:
                Usage(argv[0]);
                return 1;
        }
    }
    Server server(address, port);
    if (!server.Start())
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
    server.Stop();
    return 0;
}

/*TODO: 提取为公共函数 */
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

void Usage(const char* prog)
{
    std::cout << "Usage: " << prog << " [options]\n"
              << "Options:\n"
              << "  -a <address>     bind address (default 127.0.0.1)\n"
              << "  -p <port>        target port (default 7788)\n"
              << "  -h <help>        display this help message\n";
}