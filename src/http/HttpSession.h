#pragma once

#include <chrono>
#include <cstring>
#include <functional>
#include <memory>
#include <string>

#include <spdlog/spdlog.h>

#include <errno.h>
#include <strings.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "http/parser/HttpParser.h"
#include "http/router/HttpRouter.h"
#include "io/socket/Socket.h"

#include "ThreadPool.h"

/**
 * HttpSession: 单条 HTTP 连接的抽象。
 *
 * - 对上：承载 HTTP 消息解析、请求构建与业务分发（Router/Handler）
 * - 对下：封装事件订阅（epoll interest）与定时器（idle timeout）
 * - 连接生命周期管理单元：以有限状态机描述连接行为
 *
 * 说明：
 * - 该类不直接操作 epoll_ctl；通过 epollInterest() 暴露当前订阅。
 * - Reactor 在收到 epoll 事件后调用 onEpollEvent()，并根据 epollInterest() 更新订阅。
 */
class HttpSession final : public HttpParserCallback,
                          public std::enable_shared_from_this<HttpSession>
{
public:
    enum class State
    {
        READING,
        WAITING_WORKER,
        WRITING,
        CLOSING,
        CLOSED,
    };

    using RunInLoop = std::function<void(std::function<void()>)>;

    HttpSession(Socket&& sock, Router& router, ThreadPool& workers, RunInLoop run_in_loop,
                std::shared_ptr<spdlog::logger> logger)
        : client_sock_(std::move(sock))
        , router_(router)
        , workers_(workers)
        , run_in_loop_(std::move(run_in_loop))
        , logger_(std::move(logger))
        , parser_(this)
    {
        last_active_ = std::chrono::steady_clock::now();
    }

    ~HttpSession() override
    {
        if (client_sock_.fd() >= 0)
        {
            client_sock_.close();
        }
    }

    HttpSession(const HttpSession&) = delete;
    HttpSession& operator=(const HttpSession&) = delete;

    int   fd() const { return client_sock_.fd(); }
    State state() const { return state_; }
    bool  closed() const { return state_ == State::CLOSED; }

    void setIdleTimeout(std::chrono::milliseconds timeout) { idle_timeout_ = timeout; }

    // 仅在所属 SubReactor 线程调用：用于把 worker 计算出的响应写回连接。
    void applyResponse(HttpResponse resp)
    {
        if (state_ == State::CLOSED)
            return;

        // keep-alive 决策：若请求要求关闭，或响应显式 Connection: close，则关闭。
        bool response_close = false;
        for (const auto& kv : resp.headers)
        {
            if (strcasecmp(kv.first.c_str(), "Connection") == 0)
            {
                if (strcasecmp(kv.second.c_str(), "close") == 0)
                    response_close = true;
                break;
            }
        }

        close_after_write_ = close_after_write_ || (!keep_alive_) || response_close;
        queueResponse(std::move(resp));
        transitionTo(State::WRITING);
    }

    // Reactor 在 tick 时调用，用于处理 idle 超时等定时逻辑
    void onTick(std::chrono::steady_clock::time_point now)
    {
        if (state_ == State::CLOSED || state_ == State::CLOSING)
            return;
        if (idle_timeout_.count() <= 0)
            return;
        if (now - last_active_ >= idle_timeout_)
        {
            logger_->debug("[session {}] idle timeout, closing", fd());
            transitionTo(State::CLOSING);
            closeNow();
        }
    }

    // 返回当前 Session 希望订阅的 epoll events（不包含 EPOLLET，由 Reactor 决定）
    uint32_t epollInterest() const
    {
        if (state_ == State::CLOSED)
            return 0;

        uint32_t ev = EPOLLRDHUP | EPOLLHUP | EPOLLERR;
        switch (state_)
        {
            case State::READING:
                ev |= EPOLLIN;
                break;
            case State::WAITING_WORKER:
                // Backpressure: 业务处理中不继续读，避免无限堆积。
                break;
            case State::WRITING:
                ev |= EPOLLOUT;
                break;
            case State::CLOSING:
                ev |= EPOLLOUT;   // 允许尝试 flush 完成后关闭
                break;
            case State::CLOSED:
                break;
        }
        return ev;
    }

    // Reactor 收到 epoll 事件后调用
    void onEpollEvent(uint32_t events)
    {
        if (state_ == State::CLOSED)
            return;

        last_active_ = std::chrono::steady_clock::now();

        if (events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP))
        {
            transitionTo(State::CLOSING);
            closeNow();
            return;
        }

        if ((events & EPOLLIN) && (state_ == State::READING))
        {
            handleReadable();
        }

        if ((events & EPOLLOUT) && (state_ == State::WRITING || state_ == State::CLOSING))
        {
            handleWritable();
        }
    }

private:
    // ----- HttpParserCallback -----
    void onRequestLine(const std::string& method, const std::string& path,
                       const std::string& version) override
    {
        version_ = version;

        if (method == "GET")
            req_.method = HttpMethod::GET;
        else if (method == "POST")
            req_.method = HttpMethod::POST;
        else if (method == "PUT")
            req_.method = HttpMethod::PUT;
        else if (method == "DELETE")
            req_.method = HttpMethod::DELETE;
        else if (method == "PATCH")
            req_.method = HttpMethod::PATCH;
        else if (method == "OPTIONS")
            req_.method = HttpMethod::OPTIONS;
        else if (method == "HEAD")
            req_.method = HttpMethod::HEAD;
        else
            req_.method = HttpMethod::UNKNOWN;

        req_.path = path;
    }

    void onHeader(const std::string& name, const std::string& value) override
    {
        req_.headers[name] = value;
    }

    void onHeadersComplete() override
    {
        // 不在 IO 线程执行 route/handler，等 onMessageComplete 后投递到 worker。
    }

    void onBody(const char* data, size_t len) override { req_.body.append(data, len); }

    void onMessageComplete() override
    {
        // Keep-Alive 决策基于请求头/HTTP 版本
        keep_alive_ = shouldKeepAlive();
        close_after_write_ = false;
        if (!keep_alive_)
            close_after_write_ = true;

        // 业务处理交给 worker；IO 线程只负责读/写。
        HttpRequest                req_copy = req_;
        std::weak_ptr<HttpSession> weak_self = shared_from_this();

        Router*                         router = &router_;
        RunInLoop                       run_in_loop = run_in_loop_;
        std::shared_ptr<spdlog::logger> logger = logger_;
        int                             session_fd = fd();

        transitionTo(State::WAITING_WORKER);
        try
        {
            workers_.enqueue(
                [weak_self, req_copy, router, run_in_loop, logger, session_fd]() mutable
                {
                    RouteParams                     params;
                    std::unique_ptr<RequestHandler> handler =
                        router->route(req_copy.method, req_copy.path, params);
                    HttpResponse resp;

                    if (!handler)
                    {
                        resp = HttpResponse{404,
                                            "Not Found",
                                            {{"Content-Length", "0"}, {"Connection", "close"}},
                                            ""};
                    }
                    else
                    {
                        handler->onRequest(req_copy, params);
                        if (!req_copy.body.empty())
                            handler->onBody(req_copy.body.data(), req_copy.body.size());
                        handler->onEOM();
                        resp = std::move(handler->takeResponse());
                    }

                    run_in_loop(
                        [weak_self, resp = std::move(resp)]() mutable
                        {
                            if (auto self = weak_self.lock())
                            {
                                self->applyResponse(std::move(resp));
                            }
                        });
                });
        }
        catch (const std::exception& e)
        {
            logger_->error("[session {}] enqueue worker failed: {}", session_fd, e.what());
            queueResponse(HttpResponse{500,
                                       "Internal Server Error",
                                       {{"Content-Length", "0"}, {"Connection", "close"}},
                                       ""});
            close_after_write_ = true;
            transitionTo(State::WRITING);
        }
    }

    void onError(int code) override
    {
        logger_->error("[session {}] parser error: {}", fd(), code);
        queueResponse(HttpResponse{
            400, "Bad Request", {{"Content-Length", "0"}, {"Connection", "close"}}, ""});
        close_after_write_ = true;
        transitionTo(State::WRITING);
    }

    // ----- IO handling -----
    void handleReadable()
    {
        char buffer[4096];
        while (true)
        {
            ssize_t n = ::recv(fd(), buffer, sizeof(buffer), 0);
            if (n > 0)
            {
                parser_.feed(buffer, static_cast<size_t>(n));

                // 当解析完成并已投递到 worker，会切到 WAITING_WORKER
                if (state_ != State::READING)
                    return;

                // 继续 drain 直到 EAGAIN
                continue;
            }

            if (n == 0)
            {
                transitionTo(State::CLOSING);
                closeNow();
                return;
            }

            // n < 0
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                return;
            }
            logger_->error("[session {}] recv failed: {}", fd(), std::strerror(errno));
            transitionTo(State::CLOSING);
            closeNow();
            return;
        }
    }

    void handleWritable()
    {
        if (out_buffer_.empty())
        {
            if (state_ == State::CLOSING)
            {
                closeNow();
            }
            else
            {
                transitionTo(State::READING);
            }
            return;
        }

        while (out_offset_ < out_buffer_.size())
        {
            ssize_t n = ::send(fd(),
                               out_buffer_.data() + out_offset_,
                               out_buffer_.size() - out_offset_,
                               MSG_NOSIGNAL);
            if (n > 0)
            {
                out_offset_ += static_cast<size_t>(n);
                continue;
            }

            if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
            {
                // 等待下一次 EPOLLOUT
                return;
            }

            logger_->error("[session {}] send failed: {}", fd(), std::strerror(errno));
            transitionTo(State::CLOSING);
            closeNow();
            return;
        }

        // flush 完成
        out_buffer_.clear();
        out_offset_ = 0;

        if (close_after_write_)
        {
            transitionTo(State::CLOSING);
            closeNow();
            return;
        }

        // Keep-Alive: 为下一条请求重置状态，并尝试解析缓冲区剩余数据
        resetForNextRequest();
        transitionTo(State::READING);
        parser_.feed("", 0);
        // 若 pipeline 数据足够形成完整请求，这里会触发 onMessageComplete 并进入 WAITING_WORKER。
    }

    // ----- FSM / helpers -----
    void transitionTo(State next)
    {
        if (state_ == State::CLOSED)
            return;
        state_ = next;
    }

    void closeNow()
    {
        if (state_ == State::CLOSED)
            return;
        if (client_sock_.fd() >= 0)
        {
            client_sock_.close();
        }
        transitionTo(State::CLOSED);
    }

    bool shouldKeepAlive() const
    {
        std::string connection_val;
        for (const auto& header : req_.headers)
        {
            if (strcasecmp(header.first.c_str(), "Connection") == 0)
            {
                connection_val = header.second;
                break;
            }
        }

        if (!connection_val.empty())
        {
            if (connection_val == "close")
                return false;
            if (connection_val == "keep-alive")
                return true;
        }

        if (version_ == "HTTP/1.0")
            return false;
        return true;   // HTTP/1.1 默认 keep-alive
    }

    void resetForNextRequest()
    {
        close_after_write_ = false;
        keep_alive_ = false;
        req_ = HttpRequest();
        version_.clear();
        parser_.reset();
    }

    void queueResponse(HttpResponse resp)
    {
        // 补齐 Content-Length（如果缺失）
        if (resp.headers.find("Content-Length") == resp.headers.end())
        {
            resp.headers["Content-Length"] = std::to_string(resp.body.size());
        }

        // 如果决定关闭，显式告诉客户端
        if (close_after_write_)
        {
            resp.headers["Connection"] = "close";
        }

        std::string response_str =
            "HTTP/1.1 " + std::to_string(resp.status_code) + " " + resp.status_message + "\r\n";
        for (const auto& header : resp.headers)
        {
            response_str += header.first + ": " + header.second + "\r\n";
        }
        response_str += "\r\n";
        response_str += resp.body;

        out_buffer_ = std::move(response_str);
        out_offset_ = 0;
    }

private:
    Socket                          client_sock_;
    Router&                         router_;
    ThreadPool&                     workers_;
    RunInLoop                       run_in_loop_;
    std::shared_ptr<spdlog::logger> logger_;

    // 解析/业务状态
    HttpParser  parser_;
    HttpRequest req_;
    std::string version_;

    // IO 状态
    State       state_ = State::READING;
    bool        keep_alive_ = false;
    bool        close_after_write_ = false;
    std::string out_buffer_;
    size_t      out_offset_ = 0;

    // Timer（idle）
    std::chrono::milliseconds             idle_timeout_{30000};
    std::chrono::steady_clock::time_point last_active_;
};