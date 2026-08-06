#pragma once

#include <net/Epoller.hh>
#include <sys/eventfd.h>
#include <thread>
#include <mutex>

/** 自定义 make_unique 方便使用 */
template<typename T, typename... Args> std::unique_ptr<T> make_unique(Args&&... args)
{
    return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

/**
 * @brief 事件循环的封装，提供 loop() 接口，死循环监听事件、分发事件、处理事件
 */
class EventLoop
{
public:
    EventLoop()
        : epoller_()
    {
        thread_id_ = std::this_thread::get_id();
        int wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (wakeup_fd_ == -1)
        {
            std::runtime_error("Failed to create event fd");
        }
        wakeupChan_ = make_unique<Channel>(wakeup_fd_, EPOLLIN);
        wakeupChan_->SetReadCallback([this]() { this->HandleWakeUp(); });
        epoller_.AddChannel(wakeupChan_.get());
    }
    ~EventLoop() {}

    void loop()
    {

        while (!quit_)
        {
            auto activated_channels = epoller_.Wait(-1);   // 无IO时永久阻塞，直到 eventfd 唤醒
            for (Channel* chan : activated_channels)
            {
                chan->HandleEvent();
            }
            DoPendingTasks();
        }
    }

    void quit()
    {
        quit_ = true;
        WakeUp();
    }

    void AddChannel(Channel* ch) { epoller_.AddChannel(ch); }
    void UpdateChannel(Channel* ch) { epoller_.UpdateChannel(ch); }
    void RemoveChannel(Channel* ch) { epoller_.RemoveChannel(ch); }

    bool IsInLoopThread() const { return std::this_thread::get_id() == thread_id_; }

    void RunInLoop(std::function<void()> cb)
    {
        if (IsInLoopThread())
        {
            cb();
        }
        else
        {
            QueueInLoop(std::move(cb));
        }
    }

    /**
     * @brief 供其他线程投递任务
     */
    void QueueInLoop(std::function<void()> cb)
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            pendingTasks_.push_back(std::move(cb));
        }
        WakeUp();
    };

private:
    Epoller                            epoller_;
    bool                               quit_ = false;
    std::unique_ptr<Channel>           wakeupChan_;
    std::mutex                         mutex_;
    std::thread::id                    thread_id_;   // 判断当前线程是不是 loop 线程，以决定待执行的任务是立即执行，还是提交到loop_thread等待执行。
    std::vector<std::function<void()>> pendingTasks_;


    /**
     * @brief 执行其他线程投递的任务
     */
    void DoPendingTasks()
    {
        std::vector<std::function<void()>> tasks;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            tasks = pendingTasks_;
            pendingTasks_.clear();
        }
        for (auto& task : tasks)
        {
            task();
        }
    }

    /**
     * @brief 往 eventfd 写 8 字节，唤醒阻塞在 epoll_wait 的 IO 线程
     */
    void WakeUp()
    {
        uint64_t one = 1;
        ::write(wakeupChan_->Fd(), &one, sizeof(one));
    }

    /**
     * @brief 读空 eventfd 的计数器，消耗唤醒事件
     */
    void HandleWakeUp()
    {
        uint64_t val;
        ::read(wakeupChan_->Fd(), &val, sizeof(val));
    }
};