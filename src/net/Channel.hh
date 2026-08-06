#pragma once

#include <memory>
#include <functional>
#include <sys/epoll.h>
#include <net/socket/Socket.h>

class Channel
{
public:
    Channel(std::unique_ptr<Socket> sock, int events)
        : sock_(std::move(sock))
        , events_(events)
        , revents_(0)
    {
    }

    // 临时支持listen fd：不拥有，只存 fd
    Channel(int fd, int events)
        : fd_(fd)
        , events_(events)
        , revents_(0)
    {
    }

    void SetReadCallback(std::function<void()> cb) { readCallback_ = std::move(cb); }
    void SetWriteCallback(std::function<void()> cb) { writeCallback_ = std::move(cb); }
    void SetErrorCallback(std::function<void()> cb) { errorCallback_ = std::move(cb); }

    int  Fd() const { return sock_ ? sock_->fd() : fd_; }
    int  Events() const { return events_; }
    void SetREvents(int revents) { revents_ = revents; }
    void SetEvents(int events) { events_ = events; }

    void HandleEvent()
    {
        if (revents_ & EPOLLIN)
        {
            if (readCallback_)
                readCallback_();
        }
        if (revents_ & EPOLLOUT)
        {
            if (writeCallback_)
                writeCallback_();
        }
        if (revents_ & EPOLLERR)
        {
            if (errorCallback_)
                errorCallback_();
        }
    }

private:
    std::unique_ptr<Socket> sock_;
    int                     fd_ = -1;   // 临时支持 listen fd
    int                     events_;    // interested events
    int                     revents_;   // returned events

    // 事件发生时的回调函数
    std::function<void()> readCallback_;
    std::function<void()> writeCallback_;
    std::function<void()> errorCallback_;
};