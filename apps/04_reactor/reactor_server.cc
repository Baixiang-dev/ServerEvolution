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
#include <sys/eventfd.h>
#include <sys/timerfd.h>

// Third-party code
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include "ThreadPool.h"
#include "http/HttpSession.h"
#include "http/handlers/HttpHandlers.h"
#include "http/router/HttpRouter.h"

/**
 * Reactor 架构的 IO 层 Server
 *
 * - Reactor: epoll_wait 获取事件，分派给 HttpSession
 * - HttpSession: 单连接生命周期/FSM + HTTP 解析/路由/响应 + 非阻塞写缓冲 + idle 超时
 */

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>

class SubReactor
{
public:
    SubReactor(Router& router, ThreadPool& workers, std::shared_ptr<spdlog::logger> logger)
        : router_(router)
        , workers_(workers)
        , logger_(logger)
    {
        running_ = false;
    }

    ~SubReactor() { stop(); }

    void start()
    {
        running_ = true;
        th_ = std::thread(&SubReactor::run, this);
    }

    void stop()
    {
        if (!running_)
            return;
        running_ = false;
        // wake up epoll loop
        uint64_t one = 1;
        if (efd_ >= 0)
            ::write(efd_, &one, sizeof(one));
        if (th_.joinable())
            th_.join();
    }

    void enqueueFd(int fd)
    {
        {
            std::lock_guard<std::mutex> lk(pending_fds_mutex_);
            pending_fds_.push(fd);
        }
        uint64_t one = 1;
        ::write(efd_, &one, sizeof(one));
    }

    // runInLoop: used by workers to schedule a functor to be executed in this reactor thread
    void runInLoop(std::function<void()> cb)
    {
        {
            std::lock_guard<std::mutex> lk(pending_functors_mutex_);
            pending_functors_.push(std::move(cb));
        }
        uint64_t one = 1;
        ::write(efd_, &one, sizeof(one));
    }

private:
    void run()
    {
        epfd_ = ::epoll_create1(0);
        if (epfd_ < 0)
        {
            logger_->error("SubReactor epoll_create1 failed: {}", strerror(errno));
            return;
        }

        tfd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        if (tfd_ < 0)
        {
            logger_->error("SubReactor timerfd_create failed: {}", strerror(errno));
            ::close(epfd_);
            return;
        }

        itimerspec its;
        memset(&its, 0, sizeof(its));
        its.it_interval.tv_sec = 1;
        its.it_value.tv_sec = 1;
        if (::timerfd_settime(tfd_, 0, &its, nullptr) < 0)
        {
            logger_->error("timerfd_settime failed: {}", strerror(errno));
            ::close(tfd_);
            ::close(epfd_);
            return;
        }

        efd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (efd_ < 0)
        {
            logger_->error("eventfd_create failed: {}", strerror(errno));
            ::close(tfd_);
            ::close(epfd_);
            return;
        }

        auto epoll_add = [&](int fd, uint32_t events)
        {
            epoll_event ev;
            memset(&ev, 0, sizeof(ev));
            ev.events = events;
            ev.data.fd = fd;
            return ::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) == 0;
        };

        auto epoll_mod = [&](int fd, uint32_t events)
        {
            epoll_event ev;
            memset(&ev, 0, sizeof(ev));
            ev.events = events;
            ev.data.fd = fd;
            return ::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) == 0;
        };

        auto epoll_del = [&](int fd)
        { return ::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr) == 0; };

        if (!epoll_add(tfd_, EPOLLIN | EPOLLET))
        {
            logger_->error("epoll_ctl ADD timer fd failed: {}", strerror(errno));
            ::close(tfd_);
            ::close(epfd_);
            ::close(efd_);
            return;
        }

        if (!epoll_add(efd_, EPOLLIN | EPOLLET))
        {
            logger_->error("epoll_ctl ADD event fd failed: {}", strerror(errno));
            ::close(tfd_);
            ::close(epfd_);
            ::close(efd_);
            return;
        }

        epoll_event events[128];

        while (running_)
        {
            int n = ::epoll_wait(epfd_, events, 128, 1000);
            if (n < 0)
            {
                if (errno == EINTR)
                    continue;
                logger_->error("SubReactor epoll_wait failed: {}", strerror(errno));
                break;
            }

            for (int i = 0; i < n; ++i)
            {
                int      fd = events[i].data.fd;
                uint32_t ev = events[i].events;

                if (fd == efd_)
                {
                    uint64_t cnt;
                    while (::read(efd_, &cnt, sizeof(cnt)) > 0)
                    {
                        // drain
                    }

                    // drain pending fds
                    std::vector<int> fds;
                    {
                        std::lock_guard<std::mutex> lk(pending_fds_mutex_);
                        while (!pending_fds_.empty())
                        {
                            fds.push_back(pending_fds_.front());
                            pending_fds_.pop();
                        }
                    }

                    for (int cfd : fds)
                    {
                        Socket client_sock(cfd);
                        client_sock.setNonBlocking();
                        client_sock.setTcpNoDelay();

                        auto session = std::make_shared<HttpSession>(
                            std::move(client_sock),
                            router_,
                            workers_,
                            [this](std::function<void()> fn) { runInLoop(std::move(fn)); },
                            logger_);

                        uint32_t interest = session->epollInterest() | EPOLLET;
                        if (!epoll_add(cfd, interest))
                        {
                            logger_->error("epoll_ctl ADD client failed: {}", strerror(errno));
                            continue;
                        }
                        sessions_.emplace(cfd, std::move(session));
                        logger_->debug("SubReactor new connection: {}", cfd);
                    }

                    // run pending functors
                    std::queue<std::function<void()>> functors;
                    {
                        std::lock_guard<std::mutex> lk(pending_functors_mutex_);
                        std::swap(functors, pending_functors_);
                    }
                    while (!functors.empty())
                    {
                        try
                        {
                            functors.front()();
                        }
                        catch (const std::exception& e)
                        {
                            logger_->error("SubReactor functor exception: {}", e.what());
                        }
                        functors.pop();
                    }

                    // After running functors (which may have changed session states),
                    // sync epoll interests for all sessions so EPOLLOUT/EPOLLIN changes take
                    // effect.
                    {
                        std::vector<int> to_remove;
                        to_remove.reserve(16);
                        for (auto& kv : sessions_)
                        {
                            int      cfd = kv.first;
                            uint32_t new_interest = kv.second->epollInterest() | EPOLLET;
                            if (!epoll_mod(cfd, new_interest))
                            {
                                logger_->error("epoll_ctl MOD failed after functors: {}",
                                               strerror(errno));
                                to_remove.push_back(cfd);
                            }
                        }
                        for (int cfd : to_remove)
                        {
                            epoll_del(cfd);
                            sessions_.erase(cfd);
                        }
                    }

                    continue;
                }

                if (fd == tfd_)
                {
                    uint64_t exp;
                    while (::read(tfd_, &exp, sizeof(exp)) > 0)
                    {
                    }

                    auto             now = std::chrono::steady_clock::now();
                    std::vector<int> to_close;
                    to_close.reserve(sessions_.size());
                    for (auto& it : sessions_)
                    {
                        it.second->onTick(now);
                        if (it.second->closed())
                            to_close.push_back(it.first);
                    }
                    for (int cfd : to_close)
                    {
                        epoll_del(cfd);
                        sessions_.erase(cfd);
                    }
                    continue;
                }

                auto it = sessions_.find(fd);
                if (it == sessions_.end())
                    continue;

                auto session = it->second;
                session->onEpollEvent(ev);

                if (session->closed())
                {
                    epoll_del(fd);
                    sessions_.erase(it);
                    continue;
                }

                uint32_t interest = session->epollInterest() | EPOLLET;
                if (!epoll_mod(fd, interest))
                {
                    logger_->error("epoll_ctl MOD failed: {}", strerror(errno));
                    epoll_del(fd);
                    sessions_.erase(fd);
                }
            }
        }

        for (auto& item : sessions_) epoll_del(item.first);
        sessions_.clear();

        epoll_del(tfd_);
        epoll_del(efd_);
        ::close(tfd_);
        ::close(efd_);
        ::close(epfd_);
    }

private:
    Router&                         router_;
    ThreadPool&                     workers_;
    std::shared_ptr<spdlog::logger> logger_;
    std::atomic<bool>               running_{false};
    int                             epfd_ = -1;
    int                             tfd_ = -1;
    int                             efd_ = -1;
    std::thread                     th_;

    std::unordered_map<int, std::shared_ptr<HttpSession>> sessions_;

    std::queue<int> pending_fds_;
    std::mutex      pending_fds_mutex_;

    std::queue<std::function<void()>> pending_functors_;
    std::mutex                        pending_functors_mutex_;
};

class Server
{
public:
    Server(std::string address, int port, std::string static_dir = "web_root", size_t sub_count = 0,
           size_t worker_count = 0)
        : address_(std::move(address))
        , port_(port)
        , static_dir_(std::move(static_dir))
        , sub_count_(sub_count)
        , worker_count_(worker_count)
    {
        logger_ = spdlog::basic_logger_mt("reactor_Server_Logger", "logs/reactor_server.log");
    }

    ~Server() { stop(); }

    bool start();
    void stop();

private:
    std::string                     address_;
    int                             port_;
    std::string                     static_dir_;
    size_t                          sub_count_ = 0;
    size_t                          worker_count_ = 0;
    std::atomic<bool>               running_{false};
    std::unique_ptr<Socket>         listen_sock_;
    std::unique_ptr<Router>         router_;
    std::shared_ptr<spdlog::logger> logger_;

    ThreadPool*                              workers_ = nullptr;
    std::vector<std::unique_ptr<SubReactor>> sub_reactors_;
    std::thread                              acceptor_thread_;

    bool                     setup_socket();
    std::unique_ptr<Router>  register_router(std::string& dir);
    std::vector<std::string> get_html_files_recursively(const std::string& dir);
};

bool Server::start()
{
    // create router early
    router_ = register_router(static_dir_);

    // determine sub reactor count
    if (sub_count_ == 0)
    {
        sub_count_ = std::thread::hardware_concurrency();
        if (sub_count_ == 0)
            sub_count_ = 4;
    }

    if (worker_count_ == 0)
        worker_count_ = sub_count_;

    // create thread pool
    try
    {
        workers_ = new ThreadPool(worker_count_);
    }
    catch (const std::exception& e)
    {
        logger_->error("Failed to create ThreadPool: {}", e.what());
        return false;
    }

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

    // start sub reactors
    sub_reactors_.reserve(sub_count_);
    for (size_t i = 0; i < sub_count_; ++i)
    {
        sub_reactors_.emplace_back(new SubReactor(*router_, *workers_, logger_));
        sub_reactors_.back()->start();
    }

    running_ = true;

    // acceptor thread
    acceptor_thread_ = std::thread(
        [this]()
        {
            size_t rr = 0;
            int    lfd = listen_sock_->fd();
            while (running_)
            {
                while (true)
                {
                    sockaddr_in client_addr;
                    socklen_t   client_len = sizeof(client_addr);
                    int         cfd = ::accept(lfd, (sockaddr*)&client_addr, &client_len);
                    if (cfd < 0)
                    {
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                            break;
                        logger_->error("accept failed: {}", strerror(errno));
                        break;
                    }

                    // dispatch to sub reactor
                    size_t idx = rr++ % sub_reactors_.size();
                    sub_reactors_[idx]->enqueueFd(cfd);
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        });

    logger_->info(
        "Server started on {}:{} (subs={} workers={})", address_, port_, sub_count_, worker_count_);
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

    // wake and stop sub reactors
    for (auto& sub : sub_reactors_)
    {
        sub->stop();
    }

    if (acceptor_thread_.joinable())
        acceptor_thread_.join();

    if (workers_)
    {
        delete workers_;
        workers_ = nullptr;
    }

    logger_->info("Server stopped");
    spdlog::shutdown();
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
    int sub_reactors = 0;
    int workers = 0;
    while ((opt = getopt(argc, argv, "a:p:t:w:h")) != -1)
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
                sub_reactors = std::stoi(optarg);
                break;
            case 'w':
                workers = std::stoi(optarg);
                break;
            case 'h':
                usage(argv[0]);
                return 0;
            default:
                usage(argv[0]);
                return 1;
        }
    }

    Server server(
        address, port, "web_root", static_cast<size_t>(sub_reactors), static_cast<size_t>(workers));
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
    std::cout << "Usage: " << prog << " [-a address] [-p port] [-t sub_reactors] [-w workers]"
              << std::endl;
}
