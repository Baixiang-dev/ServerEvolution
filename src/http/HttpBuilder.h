#pragma once
#include <cstring>
#include <memory>
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <unistd.h>

#include "http/parser/HttpParser.h"
#include "http/router/HttpRouter.h"

/**
 * HttpReqBuilder: Parser 和 Router 的桥梁，在 Parser 解析过程中构造 Http 请求，在 Router 中匹配
 *  Handler，构造响应并发送
 */
class HttpReqBuilder : public HttpParserCallback
{
public:
    HttpReqBuilder(Router& r, int sock, std::shared_ptr<spdlog::logger> logger)
        : router_(r)
        , client_sock_(sock)
        , done_(false)
        , logger_(logger)
    {
    }

    void onRequestLine(const std::string& method, const std::string& path, const std::string& version) override
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

    void onHeader(const std::string& name, const std::string& value) override { req_.headers[name] = value; }

    void onHeadersComplete() override
    {
        handler_ = router_.route(req_.method, req_.path, params_);
        if (!handler_)
        {
            // 404 处理
            logger_->debug("[404] Not Found: {}", req_.path);
            send_response(client_sock_, HttpResponse{404, "Not Found", {{"Content-Length", "0"}, {"Connection", "close"}}, ""});
            done_ = true;   // 标记结束
            return;
        }
        handler_->onRequest(req_, params_);
    }

    void onBody(const char* data, size_t len) override
    {
        if (handler_)
        {
            handler_->onBody(data, len);
        }
    }

    void onMessageComplete() override
    {
        if (handler_)
        {
            handler_->onEOM();
            HttpResponse resp = std::move(handler_->takeResponse());
            send_response(client_sock_, resp);
        }
        done_ = true;   // 标记结束
    }

    void onError(int code) override
    {
        logger_->error("Parser Error: {}", code);
        send_response(client_sock_, HttpResponse{400, "Bad Request", {{"Content-Length", "0"}, {"Connection", "close"}}, ""});
        done_ = true;
    }

    // 提供给 Server 判断是否处理完一个请求
    bool isDone() const { return done_; }

    // 重置状态（支持 Keep-Alive 复用同一个 Builder）
    void reset()
    {
        done_ = false;
        handler_.reset();
        req_ = HttpRequest();
        params_.clear();
        version_.clear();
    }

    bool shouldKeepAlive() const
    {
        std::string connection_val = "";
        for (const auto& header : req_.headers)
        {
            std::string key = header.first;
            if (strcasecmp(key.c_str(), "Connection") == 0)
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

        // 如果没有 Connection 头，根据 HTTP 版本判断
        if (version_ == "HTTP/1.0")
            return false;

        // HTTP/1.1 默认为 keep-alive
        return true;
    }

private:
    std::string                     version_;
    HttpRequest                     req_;
    Router&                         router_;
    RouteParams                     params_;
    std::unique_ptr<RequestHandler> handler_;
    int                             client_sock_;
    bool                            done_;
    std::shared_ptr<spdlog::logger> logger_;   // 日志器，由外部注入

    void send_response(int client_sock, const HttpResponse& resp)
    {
        std::string response_str = "HTTP/1.1 " + std::to_string(resp.status_code) + " " + resp.status_message + "\r\n";
        for (const auto& header : resp.headers)
        {
            response_str += header.first + ": " + header.second + "\r\n";
        }
        response_str += "\r\n";
        response_str += resp.body;

        // 确保全部数据被发送（处理部分发送）
        /* send发送的字节数可能小于实际发送的字节数，如以下情况
         *  1. 内核发送缓冲区剩余空间不足；
         *  2. 消息很大，内核分片后多次复制到内核缓冲区；
         *  3. 非阻塞模式，send 会尽可能发送能立即写入的部分然后返回
         */
        ssize_t     total_sent = 0;
        ssize_t     to_send = response_str.size();
        const char* data = response_str.c_str();
        while (total_sent < to_send)
        {
            ssize_t sent = ::send(client_sock, data + total_sent, to_send - total_sent, 0);
            if (sent <= 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    logger_->warn("send would block, busy polling...");
                    continue;   // 内核缓冲区满，忙轮询等待可写
                }

                if (errno == EINTR)
                {
                    logger_->warn("send interrupted by signal, retrying...");
                    continue;   // 被信号中断，重试
                }

                logger_->error("send failed: {}", std::strerror(errno));
                return;
            }
            total_sent += sent;
        }
    }
};