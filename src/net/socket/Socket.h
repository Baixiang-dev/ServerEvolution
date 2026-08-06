#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <unistd.h>

class Socket
{
public:
    explicit Socket(int fd);
    ~Socket();

    // 禁用拷贝构造和拷贝赋值
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    // 启用移动构造和移动赋值
    Socket(Socket&& other) noexcept;
    Socket& operator=(Socket&& other) noexcept;

    // 关闭
    void close();
    void shutdownWrite();

    // 网络选项设置
    void setNonBlocking();   // 设置非阻塞
    void setReuseAddr();     // 地址重用
    void setTcpNoDelay();    // 关闭Nagle算法
    void setReusePort();     // 端口重用
    // void setNoSigPipe();     // 关闭SIGPIPE信号 Linux不支持SO_NOSIGPIPE选项

    // 返回只读的文件描述符
    int fd() const;

    // 基础的socket操作
    bool bind(const struct sockaddr_in& address);
    bool listen(int backlog);
    int  accept(struct sockaddr_in* client_addr, socklen_t* addr_len);

    // 获取错误
    int getSocketError();

private:
    int fd_;
};

Socket::Socket(int fd)
    : fd_(fd)
{
}

Socket::~Socket()
{
    if (fd_ >= 0)
    {
        ::close(fd_);
    }
}

Socket::Socket(Socket&& other) noexcept
    : fd_(other.fd_)
{
    other.fd_ = -1;
}

Socket& Socket::operator=(Socket&& other) noexcept
{
    if (this != &other)
    {
        if (fd_ >= 0)
        {
            ::close(fd_);
        }
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

void Socket::close()
{
    if (fd_ >= 0)
    {
        ::close(fd_);
        fd_ = -1;
    }
}

void Socket::shutdownWrite()
{
    if (fd_ >= 0)
    {
        ::shutdown(fd_, SHUT_WR);
    }
}

void Socket::setNonBlocking()
{
    int flags = fcntl(fd_, F_GETFL, 0);
    fcntl(fd_, F_SETFL, flags | O_NONBLOCK);
}

void Socket::setReuseAddr()
{
    int opt = 1;
    setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
}

void Socket::setTcpNoDelay()
{
    int opt = 1;
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
}

void Socket::setReusePort()
{
    int opt = 1;
    setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
}

// void Socket::setNoSigPipe()
// {
//     int opt = 1;
//     setsockopt(fd_, SOL_SOCKET, SO_NOSIGPIPE, &opt, sizeof(opt));
// }

int Socket::fd() const
{
    return fd_;
}

bool Socket::bind(const struct sockaddr_in& address)
{
    return ::bind(fd_, (const struct sockaddr*)&address, sizeof(address)) == 0;
}

bool Socket::listen(int backlog)
{
    return ::listen(fd_, backlog) == 0;
}

int Socket::accept(struct sockaddr_in* client_addr, socklen_t* addr_len)
{
    return ::accept(fd_, (struct sockaddr*)client_addr, addr_len);
}

int Socket::getSocketError()
{
    int       optval;
    socklen_t optlen = sizeof(optval);
    if (getsockopt(fd_, SOL_SOCKET, SO_ERROR, &optval, &optlen) < 0)
    {
        return errno;
    }
    return optval;
}