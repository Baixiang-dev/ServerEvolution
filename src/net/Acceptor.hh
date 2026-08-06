#pragma once

#include <net/EventLoop.hh>

class Acceptor
{
public:
    explicit Acceptor(std::string& listenAddr, int port, EventLoop* loop)
        : listAddr_(listenAddr)
        , port_(port)
        , loop_(loop)
    {
        std::unique_ptr<Socket> socket_ = make_unique<Socket>(::socket(AF_INET, SOCK_STREAM, 0));
        if (!socket_ || socket_->fd() < 0)
        {
            std::runtime_error("Failed to create socket: " + std::string(strerror(errno)));
        }

        socket_->setReuseAddr();
        socket_->setNonBlocking();

        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        if (inet_pton(AF_INET, listenAddr.c_str(), &addr.sin_addr) <= 0)
        {
            std::runtime_error("Invalid address or inet_pton error: " + listenAddr);
        }
        addr.sin_port = htons(port);
        if (!socket_->bind(addr))
        {
            std::runtime_error("socket bind failed; address: " + listAddr_ + ", port: " + std::to_string(port) + ", error code: " + std::to_string(socket_->getSocketError()) +
                               ", errmsg: " + std::string(strerror(errno)));
        }
        if (!socket_->listen(128))
        {
            std::runtime_error("Failed to listen on socket");
        }
        accept_channel_ = make_unique<Channel>(std::move(socket_), EPOLLIN | EPOLLET);
    }

    /**
     * @brief 在 Acceptor 的事件循环里面监听
     */
    void Listen()
    {
        // set read callback to accept new Connection
        accept_channel_->SetReadCallback(
            [this]()
            {
                struct sockaddr_in client_addr;
                socklen_t          client_len = sizeof(client_addr);
                int                fd = ::accept(accept_channel_->Fd(), (struct sockaddr*)&client_addr, &client_len);
                if (fd < 0)
                {
                    if (errno != EAGAIN && errno != EWOULDBLOCK)
                        std::runtime_error("accept failed: " + std::string(strerror(errno)));
                    return;
                }

                int flags = fcntl(fd, F_GETFL, 0);
                fcntl(fd, F_SETFL, flags | O_NONBLOCK);

                if (newConnectionCallback_)
                {
                    newConnectionCallback_(fd);
                }
                else
                {
                    ::close(fd);
                }
            });

        // 将 accept channel 注册到主 EventLoop 的 epoll
        loop_->AddChannel(accept_channel_.get());
    }

    void Close() { ::shutdown(accept_channel_->Fd(), SHUT_WR); }
    void SetNewConnectionCallback(std::function<void(int)> cb) { newConnectionCallback_ = std::move(cb); }

    std::string GetListenAddr() const { return listAddr_; }
    int         GetPort() const { return port_; }

private:
    std::string              listAddr_;
    int                      port_;
    EventLoop*               loop_;
    std::unique_ptr<Channel> accept_channel_;
    std::function<void(int)> newConnectionCallback_;
};