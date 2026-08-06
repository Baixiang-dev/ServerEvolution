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

// Third-party code
#include "ThreadPool.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"

#include "http/HttpBuilder.h"
#include "http/HttpSession.hh"
#include "http/handlers/HttpHandlers.h"
#include "http/router/HttpRouter.h"
#include "net/socket/Socket.h"
#include "net/Acceptor.hh"
#include "net/Channel.hh"
#include "net/Connection.hh"
#include "net/Epoller.hh"
#include "net/EventLoop.hh"
#include "net/EventLoopThread.hh"
#include "utils/Signal.h"

class Server
{
public:
    Server(std::string addr, int port, std::string static_dir = "web_root", int pool_size = 4, int sub_reactor_number = 4)
        : static_dir_(std::move(static_dir))
        , running_(false)
    {
        logger_ = spdlog::basic_logger_mt("multi_reactor_Server_Logger", "logs/multi_reactor_server.log");
        router_ = RegisterRouter(static_dir_);
        thread_pool_ = make_unique<ThreadPool>(pool_size);
        main_loop_ = make_unique<EventLoop>();
        acceptor_ = make_unique<Acceptor>(addr, port, main_loop_.get());
        for (int i = 0; i < sub_reactor_number; i++)
        {
            io_threads_.push_back(make_unique<EventLoopThread>("EventLoopThread " + std::to_string(i)));
        }
    }

    ~Server() { Stop(); }

    bool Start();
    void Stop();

    void ConnectionClosed(const std::shared_ptr<Connection> conn);
    void HandleCloseInLoop(const std::shared_ptr<Connection> conn);

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
    std::map<int, std::shared_ptr<Connection>>    connections_;   // fd -> connection
    std::map<int, std::unique_ptr<HttpSession>>   sessions_;      // fd -> session

    void       Run();
    void       HandleNewConnection(int fd);
    EventLoop* GetNextEventloop();

    std::unique_ptr<Router>  RegisterRouter(std::string& dir);                  // 注册路由
    std::vector<std::string> GetHtmlFilesRecursively(const std::string& dir);   // 注册路由的辅助函数
};

bool Server::Start()
{
    running_ = true;
    server_thread = std::thread(&Server::Run, this);
    logger_->info("Server started on {}:{}", acceptor_->GetListenAddr(), acceptor_->GetPort());
    return true;
}

void Server::Stop()
{
    if (running_)
    {
        running_ = false;
        main_loop_->quit();
        for (auto& reactor : io_threads_)
        {
            reactor->Wait();
        }


        acceptor_->Close();

        // 等待 server_thread 线程退出
        if (server_thread.joinable())
        {
            server_thread.join();
        }

        logger_->info("Server stopped");
        spdlog::shutdown();
    }
}

void Server::Run()
{
    // 1. 启动所有 Sub Reactor 线程，主线程阻塞直到每个 EventLoop 就绪
    for (auto& t : io_threads_)
    {
        t->Run();
    }

    // 2. 将 Acceptor 的 listen channel 注册到 main_loop，启动主事件循环
    acceptor_->SetNewConnectionCallback([this](int fd) { this->HandleNewConnection(fd); });
    acceptor_->Listen();
    main_loop_->loop();   // 阻塞直到 quit
}

void Server::HandleNewConnection(int fd)
{
    logger_->debug("Received a new Connection: {}", fd);
    EventLoop* loop = GetNextEventloop();
    auto       client_sock = make_unique<Socket>(fd);
    auto       conn = make_unique<Connection>(std::move(client_sock), loop);        // Connection 创建时自动注册到 EventLoop
    auto       session = make_unique<HttpSession>(conn.get(), *router_, logger_);   // 把Connection绑定到Session
    conn->SetCloseCallback(
        [this](std::shared_ptr<Connection> conn)
        {
            // 关闭回调：在 main_loop 中清理连接和会话
            ConnectionClosed(conn);
        });

    connections_[fd] = std::move(conn);
    sessions_[fd] = std::move(session);
    logger_->debug("Established a new Connection: {}", fd);
}

void Server::ConnectionClosed(const std::shared_ptr<Connection> conn)
{
    if (main_loop_->IsInLoopThread())
    {
        HandleCloseInLoop(conn);
    }
    else
    {
        main_loop_->QueueInLoop([this, conn]() { HandleCloseInLoop(conn); });
    }
}

void Server::HandleCloseInLoop(const std::shared_ptr<Connection> conn)
{
    logger_->debug("Earse Connection : {}", conn->Fd());
    // server loop 内 earse connection 避免竞态
    connections_.erase(conn->Fd());
    sessions_.erase(conn->Fd());
    // connection 对应的 loop 内执行清理操作，避免当前回调栈还没结束就清理掉channel，导致use-after-free
    auto conn_loop = conn->GetEventloop();
    conn_loop->QueueInLoop([conn]() { conn->ConnectionDestroyed(); });
}

EventLoop* Server::GetNextEventloop()
{
    static int i = 0;
    EventLoop* next = io_threads_[i]->GetLoop();
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

void Usage(const char* prog)
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
    Signal(SIGINT, SigintHandler);

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
                Usage(argv[0]);
                return 0;
            default:
                Usage(argv[0]);
                return 1;
        }
    }

    Server server(address, port, "web_root", pool_size, reactor_number);
    if (!server.Start())
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
    server.Stop();
    return 0;
}