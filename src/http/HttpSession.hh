#pragma once

#include <net/Connection.hh>

class HttpSession
{
public:
    HttpSession(Connection* conn, Router& router, std::shared_ptr<spdlog::logger> logger)
        : connection_(conn)
        , logger_(logger)
    {
        // HttpReqBuilder 的响应回调：序列化后交给 Connection 发送
        auto onResponse = [this](HttpResponse&& resp)
        {
            connection_->SendResponse(std::move(resp));
            if (!builder_->shouldKeepAlive())
            {
                connection_->HandleClose();   // Connection 会在 EventLoop 中安全关闭
            }
            else
            {
                // Keep-Alive: 重置 parser 和 builder 复用
                builder_->reset();
                parser_->reset();
                parser_->feed("", 0);   // 触发处理 buffer 中剩余数据
            }
        };

        builder_ = make_unique<HttpReqBuilder>(router, std::move(onResponse), logger);
        parser_ = make_unique<HttpParser>(builder_.get());

        // 设置 Connection 的回调
        connection_->SetReadCallback([this](const char* data, size_t len) { this->OnData(data, len); });
    }

private:
    void OnData(const char* data, size_t len) { parser_->feed(data, len); }

    Connection*                     connection_;   // 不拥有
    std::unique_ptr<HttpReqBuilder> builder_;
    std::unique_ptr<HttpParser>     parser_;
    std::shared_ptr<spdlog::logger> logger_;
};