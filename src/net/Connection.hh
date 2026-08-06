#pragma once

#include <memory>
#include <net/EventLoop.hh>
#include <http/HttpBuilder.h>

/**
 * @brief 对TCP连接的抽象
 *
 * @details Server accept 时创建Connection，然后分发Connection到某个EventLoop，应用层业务使用 Connectoin
 * 提供的接口读写数据。在出现错误或连接关闭时，在所属的EventLoop执行清理(因为EventLoop可能还在使用Channel，在其他地方关闭会影响EventLoop中的使用)。
 */
class Connection : public std::enable_shared_from_this<Connection>
{
public:
    /**
     * @note Connection 在构造时即归属于loop
     */
    Connection(std::unique_ptr<Socket> sock, EventLoop* loop)
        : owner_loop_(loop)
    {
        channel_ = make_unique<Channel>(std::move(sock), EPOLLIN | EPOLLET);
        channel_->SetReadCallback([this]() { this->HandleRead(); });
        channel_->SetWriteCallback([this]() { this->HandleWrite(); });
        channel_->SetErrorCallback([this]() { this->HandleError(); });
        owner_loop_->RunInLoop([this]() { this->owner_loop_->AddChannel(this->channel_.get()); });
    }

    void SetReadCallback(std::function<void(const char*, size_t)> cb) { read_cb_ = std::move(cb); }
    void SetCloseCallback(std::function<void(std::shared_ptr<Connection>)> cb) { close_cb_ = std::move(cb); }
    void SetErrorCallback(std::function<void()> cb) { error_cb_ = std::move(cb); }

    // 上层发送接口
    void SendResponse(HttpResponse&& resp)
    {
        std::string data = "HTTP/1.1 " + std::to_string(resp.status_code) + " " + resp.status_message + "\r\n";
        for (auto& h : resp.headers) data += h.first + ": " + h.second + "\r\n";
        data += "\r\n";
        data += resp.body;

        output_buffer_ += data;

        // 启用写监听
        channel_->SetEvents(EPOLLIN | EPOLLOUT | EPOLLET);
        owner_loop_->RunInLoop([this]() { owner_loop_->UpdateChannel(channel_.get()); });
    }

    // Channel 回调
    void HandleRead()
    {
        while (true)
        {
            ssize_t n = ::read(channel_->Fd(), input_buffer_, sizeof(input_buffer_));
            if (n > 0)
            {
                if (read_cb_)
                    read_cb_(input_buffer_, static_cast<size_t>(n));
            }
            else if (n == 0)
            {
                // 对端关闭
                HandleClose();
                return;
            }
            else   // n < 0
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    break;   // 读完
                // 真正的错误
                HandleError();
                return;
            }
        }
    }

    void HandleWrite()
    {
        while (!output_buffer_.empty())
        {
            ssize_t n = ::write(channel_->Fd(), output_buffer_.data(), output_buffer_.size());
            if (n > 0)
            {
                output_buffer_.erase(0, n);
            }
            else if (n < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    break;   // 内核缓冲区满，等下次 EPOLLOUT
                HandleError();
                return;
            }
        }

        // 全部发完，关闭写监听
        if (output_buffer_.empty())
        {
            channel_->SetEvents(EPOLLIN | EPOLLET);   // 去掉 EPOLLOUT
            owner_loop_->RunInLoop([this]() { owner_loop_->UpdateChannel(channel_.get()); });
        }
    }
    void HandleError()
    {
        if (error_cb_)
            error_cb_();
        HandleClose();
    }

    void HandleClose()
    {
        if (closed_)
            return;
        closed_ = true;
        auto guard_this = shared_from_this();   // 避免 earse connection 后 直接释放导致的自释放问题
        if (close_cb_)
        {
            close_cb_(guard_this);
        }
    }

    int        Fd() const { return channel_->Fd(); }
    EventLoop* GetEventloop() const { return owner_loop_; }
    void       ConnectionDestroyed() { owner_loop_->RemoveChannel(channel_.get()); }

private:
    std::unique_ptr<Channel> channel_;
    char                     input_buffer_[4096];
    std::string              output_buffer_;
    EventLoop*               owner_loop_;   // Connection 所属的 Eventloop_;

    std::function<void(const char*, size_t)>         read_cb_;
    std::function<void(std::shared_ptr<Connection>)> close_cb_;
    std::function<void()>                            error_cb_;
    bool                                             closed_ = false;
};