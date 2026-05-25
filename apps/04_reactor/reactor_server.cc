#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <dirent.h>
#include <getopt.h>
#include <iostream>
#include <signal.h>
#include <stack>
#include <thread>
#include <unordered_map>
#include <vector>

#include <sys/epoll.h>
#include <sys/timerfd.h>

// Third-party code
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include "http/HttpSession.h"
#include "http/handlers/HttpHandlers.h"
#include "http/router/HttpRouter.h"
#include "io/socket/Socket.h"

/**
 * Reactor 架构的 IO 层 Server
 *
 * - Reactor: epoll_wait 获取事件，分派给 HttpSession
 * - HttpSession: 单连接生命周期/FSM + HTTP 解析/路由/响应 + 非阻塞写缓冲 + idle 超时
 */

class Server
{
public:
    Server(std::string address, int port, std::string static_dir = "web_root")
        : address_(std::move(address))
        , port_(port)
        , static_dir_(std::move(static_dir))
    {
        logger_ = spdlog::basic_logger_mt("reactor_Server_Logger", "logs/reactor_server.log");
        router_ = register_router(static_dir_);
    }

    ~Server() { stop(); }

    bool start();
    void stop();

private:
    std::string                     address_;
    int                             port_;
    std::string                     static_dir_;
    std::atomic<bool>               running_{false};
    std::unique_ptr<Socket>         listen_sock_;
    std::unique_ptr<Router>         router_;
    std::shared_ptr<spdlog::logger> logger_;
    std::thread                     server_thread_;

    bool setup_socket();
    void reactor_loop();

    std::unique_ptr<Router>  register_router(std::string& dir);
    std::vector<std::string> get_html_files_recursively(const std::string& dir);
};

bool Server::start()
{
    if (!setup_socket())
    {
        logger_->error("Failed to setup socket");
        return false;
    }

    if (!listen_sock_->listen(128))
    {
        logger_->error("socket listen failed: error code {}, errmsg {}",
                       listen_sock_->getSocketError(),
                       strerror(errno));
        return false;
    }

    listen_sock_->setNonBlocking();

    running_ = true;
    server_thread_ = std::thread(&Server::reactor_loop, this);
    logger_->info("Server started on {}:{}", address_, port_);
    return true;
}

void Server::stop()
{
    if (!running_)
        return;

    running_ = false;

    if (listen_sock_ && listen_sock_->fd() >= 0)
    {
        ::shutdown(listen_sock_->fd(), SHUT_WR);
        listen_sock_->close();
    }

    if (server_thread_.joinable())
        server_thread_.join();

    logger_->info("Server stopped");
    spdlog::shutdown();
}

bool Server::setup_socket()
{
    listen_sock_ = std::unique_ptr<Socket>(new Socket(::socket(AF_INET, SOCK_STREAM, 0)));
    if (!listen_sock_ || listen_sock_->fd() < 0)
    {
        logger_->error("socket creation failed: error code {}, errmsg {}",
                       listen_sock_->getSocketError(),
                       strerror(errno));
        return false;
    }

    listen_sock_->setReuseAddr();

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    if (inet_pton(AF_INET, address_.c_str(), &address.sin_addr) <= 0)
    {
        logger_->error("Invalid address or inet_pton error: {}", address_);
        return false;
    }
    address.sin_port = htons(port_);

    if (!listen_sock_->bind(address))
    {
        logger_->error("socket bind failed; address: {}, port: {}, error code: {}, errmsg: {}",
                       address_,
                       port_,
                       listen_sock_->getSocketError(),
                       strerror(errno));
        return false;
    }

    return true;
}

void Server::reactor_loop()
{
    int epfd = ::epoll_create1(0);
    if (epfd < 0)
    {
        logger_->error("epoll_create1 failed: {}", strerror(errno));
        return;
    }

    int tfd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (tfd < 0)
    {
        logger_->error("timerfd_create failed: {}", strerror(errno));
        ::close(epfd);
        return;
    }

    itimerspec its;
    memset(&its, 0, sizeof(its));
    its.it_interval.tv_sec = 1;
    its.it_value.tv_sec = 1;
    if (::timerfd_settime(tfd, 0, &its, nullptr) < 0)
    {
        logger_->error("timerfd_settime failed: {}", strerror(errno));
        ::close(tfd);
        ::close(epfd);
        return;
    }

    auto epoll_add = [&](int fd, uint32_t events)
    {
        epoll_event ev;
        memset(&ev, 0, sizeof(ev));
        ev.events = events;
        ev.data.fd = fd;
        return ::epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) == 0;
    };

    auto epoll_mod = [&](int fd, uint32_t events)
    {
        epoll_event ev;
        memset(&ev, 0, sizeof(ev));
        ev.events = events;
        ev.data.fd = fd;
        return ::epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev) == 0;
    };

    auto epoll_del = [&](int fd) { return ::epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr) == 0; };

    if (!epoll_add(listen_sock_->fd(), EPOLLIN | EPOLLET))
    {
        logger_->error("epoll_ctl ADD listen fd failed: {}", strerror(errno));
        ::close(tfd);
        ::close(epfd);
        return;
    }

    if (!epoll_add(tfd, EPOLLIN | EPOLLET))
    {
        logger_->error("epoll_ctl ADD timer fd failed: {}", strerror(errno));
        ::close(tfd);
        ::close(epfd);
        return;
    }

    std::unordered_map<int, std::unique_ptr<HttpSession>> sessions;
    epoll_event                                           events[128];

    while (running_)
    {
        int n = ::epoll_wait(epfd, events, 128, 1000);
        if (n < 0)
        {
            if (errno == EINTR)
                continue;
            logger_->error("epoll_wait failed: {}", strerror(errno));
            break;
        }

        for (int i = 0; i < n; ++i)
        {
            int      fd = events[i].data.fd;
            uint32_t ev = events[i].events;

            if (fd == listen_sock_->fd())
            {
                while (true)
                {
                    sockaddr_in client_addr;
                    socklen_t   client_len = sizeof(client_addr);
                    int         cfd = ::accept(fd, (sockaddr*)&client_addr, &client_len);
                    if (cfd < 0)
                    {
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                            break;
                        logger_->error("accept failed: {}", strerror(errno));
                        break;
                    }

                    Socket client_sock(cfd);
                    client_sock.setNonBlocking();
                    client_sock.setTcpNoDelay();

                    auto session = std::unique_ptr<HttpSession>(
                        new HttpSession(std::move(client_sock), *router_, logger_));
                    uint32_t interest = session->epollInterest() | EPOLLET;

                    if (!epoll_add(cfd, interest))
                    {
                        logger_->error("epoll_ctl ADD client failed: {}", strerror(errno));
                        continue;
                    }

                    logger_->debug("New Connection: {}", cfd);
                    sessions.emplace(cfd, std::move(session));
                }
                continue;
            }

            if (fd == tfd)
            {
                uint64_t exp;
                while (::read(tfd, &exp, sizeof(exp)) > 0)
                {
                    // drain
                }

                auto             now = std::chrono::steady_clock::now();
                std::vector<int> to_close;
                to_close.reserve(sessions.size());

                for (auto& item : sessions)
                {
                    item.second->onTick(now);
                    if (item.second->closed())
                        to_close.push_back(item.first);
                }

                for (int cfd : to_close)
                {
                    epoll_del(cfd);
                    sessions.erase(cfd);
                }
                continue;
            }

            auto it = sessions.find(fd);
            if (it == sessions.end())
                continue;

            HttpSession* session = it->second.get();
            session->onEpollEvent(ev);

            if (session->closed())
            {
                epoll_del(fd);
                sessions.erase(it);
                continue;
            }

            uint32_t interest = session->epollInterest() | EPOLLET;
            if (!epoll_mod(fd, interest))
            {
                logger_->error("epoll_ctl MOD failed: {}", strerror(errno));
                epoll_del(fd);
                sessions.erase(fd);
            }
        }
    }

    for (auto& item : sessions) epoll_del(item.first);
    sessions.clear();

    epoll_del(tfd);
    ::close(tfd);
    ::close(epfd);
}

std::unique_ptr<Router> Server::register_router(std::string& dir)
{
    std::vector<std::string> html_files = get_html_files_recursively(dir);
    std::unique_ptr<Router>  router(new Router());

    router->addRoute(HttpMethod::POST,
                     "/api/image/process",
                     [this]()
                     { return std::unique_ptr<RequestHandler>(new ImageProcessHandler(logger_)); });

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
            file_path.substr(dir.size()),
            [file_path, this]()
            { return std::unique_ptr<RequestHandler>(new HtmlFileHandler(file_path, logger_)); });
    }

    return router;
}

std::vector<std::string> Server::get_html_files_recursively(const std::string& dir)
{
    std::vector<std::string> html_files;
    std::stack<std::string>  dirs;
    dirs.push(dir);

    while (!dirs.empty())
    {
        std::string cur_dir = dirs.top();
        dirs.pop();

        DIR* dp = ::opendir(cur_dir.c_str());
        if (dp == nullptr)
        {
            logger_->error("Failed to open directory: {}", cur_dir);
            continue;
        }

        dirent* entry;
        while ((entry = ::readdir(dp)) != nullptr)
        {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                continue;

            std::string full_path = cur_dir + "/" + entry->d_name;
            if (entry->d_type == DT_DIR)
            {
                dirs.push(full_path);
            }
            else if (entry->d_type == DT_REG)
            {
                if (full_path.size() >= 5 && full_path.substr(full_path.size() - 5) == ".html")
                    html_files.push_back(full_path);
            }
        }
        ::closedir(dp);
    }

    return html_files;
}

using handler_t = void (*)(int);
static handler_t Signal(int signum, handler_t handler);
static void      sigint_handler(int signum);
static void      usage(const char* prog);

static std::atomic<bool> g_quit(false);

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
    while (!g_quit) pause();

    std::cout << "Received SIGINT, shutting down server..." << std::endl;
    server.stop();
    return 0;
}

static handler_t Signal(int signum, handler_t handler)
{
    struct sigaction act, old_act;
    act.sa_handler = handler;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    if (sigaction(signum, &act, &old_act) < 0)
        return SIG_ERR;
    return old_act.sa_handler;
}

static void sigint_handler(int)
{
    g_quit = true;
}

static void usage(const char* prog)
{
    std::cout << "Usage: " << prog << " [-a address] [-p port]" << std::endl;
}
