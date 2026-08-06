#pragma once

#include <net/Channel.hh>
#include <map>
#include <iostream>

/**
 * @brief 对 epoll 操作的封装，提供注册、修改、删除 fd 的接口
 */
class Epoller
{
public:
    Epoller()
    {
        epoll_fd_ = epoll_create1(0);
        if (epoll_fd_ == -1)
        {
            throw std::runtime_error("Failed to create epoll file descriptor");
        }
    }

    ~Epoller()
    {
        if (epoll_fd_ != -1)
        {
            ::close(epoll_fd_);
        }
    }

    /**
     * @brief 注册一个 channel 到 epoll
     */
    void AddChannel(Channel* ch)
    {
        struct epoll_event ev;
        ev.data.ptr = ch;
        ev.events = ch->Events();
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, ch->Fd(), &ev);
        channels_[ch->Fd()] = ch;
    }

    /**
     * @brief 修改一个已注册的 channel 的事件
     */
    void UpdateChannel(Channel* ch)
    {
        if (channels_.find(ch->Fd()) == channels_.end())
        {
            throw std::runtime_error("Channel not found in Epoller");
        }

        struct epoll_event ev;
        ev.data.ptr = ch;
        ev.events = ch->Events();
        epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, ch->Fd(), &ev);
    }

    /**
     * @brief 从 epoll 中移除一个 channel
     */
    void RemoveChannel(Channel* ch)
    {
        if (channels_.find(ch->Fd()) == channels_.end())
        {
            throw std::runtime_error("Channel not found in Epoller");
        }

        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, ch->Fd(), nullptr);
        channels_.erase(ch->Fd());
    }

    /**
     * @brief 在 epoll 上侦听事件并返回活跃Channel
     */
    std::vector<Channel*> Wait(int timeout_ms)
    {
        const int             max_events = 64;
        std::vector<Channel*> ready;
        struct epoll_event    events[max_events];
        int                   n = epoll_wait(epoll_fd_, events, max_events, timeout_ms);
        for (int i = 0; i < n; ++i)
        {
            Channel* ch = static_cast<Channel*>(events[i].data.ptr);
            ch->SetREvents(events[i].events);
            ready.push_back(ch);
        }
        return ready;
    }

private:
    int                     epoll_fd_;
    std::map<int, Channel*> channels_;   // 在 epoll 中注册的 channel 列表
};
