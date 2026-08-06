#pragma once

#include <net/EventLoop.hh>
#include <condition_variable>

/**
 * @brief 持有 EventLoop 的 IO 线程的封装
 */
class EventLoopThread
{
public:
    explicit EventLoopThread(const std::string& threadName = "EventLoopThread")
        : loop_(nullptr)
        , loopThreadname_(threadName)
    {
    }

    ~EventLoopThread()
    {
        if (loop_)
        {
            loop_->quit();
        }
        if (thread_.joinable())
        {
            thread_.join();
        }
    }

    /**
     * @brief 启动IO线程，开始事件循环
     *
     * @note 此方法会阻塞调用它的线程，直到事件循环已就绪
     *
     * @return EventLoop* 返回指向事件循环的裸指针，供外部投递任务
     */
    EventLoop* Run()
    {
        std::mutex              mtx;
        std::condition_variable cv;
        bool                    started = false;

        thread_ = std::thread(
            [this, &mtx, &cv, &started]()
            {
                loop_ = make_unique<EventLoop>();
                {
                    std::lock_guard<std::mutex> lock(mtx);
                    started = true;
                }
                cv.notify_one();   // 通知调用者，loop已就绪

                loop_->loop();
            });

        {
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait(lock, [&started]() { return started; });
        }

        return loop_.get();
    }

    /**
     * @brief 等待事件循环退出
     *
     * @note 此方法会阻塞调用它的线程，直到事件循环退出
     */
    void Wait()
    {
        if (loop_)
        {
            loop_->quit();
        }
        if (thread_.joinable())
        {
            thread_.join();
        }
    }

    /**
     * @brief 或许指向事件循环的裸指针
     */
    EventLoop* GetLoop() const { return loop_.get(); }

private:
    std::unique_ptr<EventLoop> loop_;
    std::string                loopThreadname_;
    std::thread                thread_;
};