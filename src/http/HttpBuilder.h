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
    using ResponseCallback = std::function<void(HttpResponse&&)>;
    HttpReqBuilder(Router& r, ResponseCallback onResponse, std::shared_ptr<spdlog::logger> logger)
        : router_(r)
        , onResponse_(std::move(onResponse))
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
            onResponse_(HttpResponse{404, "Not Found", {{"Content-Length", "0"}, {"Connection", "close"}}, ""});
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
            onResponse_(std::move(handler_->takeResponse()));
        }
        done_ = true;   // 标记结束
    }

    void onError(int code) override
    {
        logger_->error("Parser Error: {}", code);
        onResponse_(HttpResponse{400, "Bad Request", {{"Content-Length", "0"}, {"Connection", "close"}}, ""});
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
    bool                            done_;
    ResponseCallback                onResponse_;
    std::shared_ptr<spdlog::logger> logger_;   // 日志器，由外部注入
};