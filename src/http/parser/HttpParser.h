#pragma once
#include <string>
#include <unordered_map>

using Headers = std::unordered_map<std::string, std::string>;

// 回调接口类
class HttpParserCallback
{
public:
    virtual void onRequestLine(const std::string& method, const std::string& path,
                               const std::string& version) = 0;   // 请求行解析完成时调用

    virtual void onHeader(const std::string& name,
                          const std::string& value) = 0;   // 请求头回调,每解析一行调用一次

    virtual void onHeadersComplete() = 0;   // 请求头解析完成回调

    virtual void onBody(const char* data, size_t len) = 0;   // 请求体回调

    virtual void onMessageComplete() = 0;   // 整个HTTP消息解析完成回调

    virtual void onError(int code) = 0;   // 解析错误回调

    virtual ~HttpParserCallback() = default;   // 虚析构函数
};

// 基于有限状态机的HTTP请求解析器
class HttpParser
{
public:
    void        feed(const char* data, size_t len);              // 接收数据进行解析
    std::string errorMessage() const { return error_message; }   // 返回错误信息
    void        reset();                                         // 重置解析器状态
    HttpParser(HttpParserCallback* callback);

private:
    enum class ParserState
    {
        REQUEST_LINE,          // 解析请求行
        HEADERS,               // 解析请求头
        BODY_CONTENT_LENGTH,   // 解析基于Content-Length的请求体
        BODY_CHUNKED,          // 解析基于Chunked的请求体 (未实现)
        COMPLETE,              // 解析完成
        ERROR                  // 解析出错
    };
    ParserState         state = ParserState::REQUEST_LINE;   // 解析状态, 初始状态为解析请求行
    std::string         buffer;                              // 存储接收到的数据
    size_t              contentLength = 0;                   // 请求体长度
    size_t              bodyBytesRead = 0;                   // 已读取的请求体字节数
    std::string         method;                              // HTTP方法
    std::string         uri;                                 // 请求URI
    std::string         version;                             // HTTP版本
    Headers             headers;                             // 请求头集合
    HttpParserCallback* callback_ = nullptr;                 // 回调接口指针
    std::string         error_message;                       // 错误信息
    int                 error_code = 0;                      // 错误代码
    bool                parseRequestLine();
    bool                parseHeaders();
    bool                parseBody();
};

HttpParser::HttpParser(HttpParserCallback* callback)
    : callback_(callback)
{
}

void HttpParser::reset()
{
    state = ParserState::REQUEST_LINE;
    contentLength = 0;
    bodyBytesRead = 0;
    method.clear();
    uri.clear();
    version.clear();
    headers.clear();
    error_message.clear();
    error_code = 0;
    // buffer 不清空，保留剩余数据供下一次解析
}

void HttpParser::feed(const char* data, size_t len)
{
    buffer.append(data, len);
    while (true)
    {
        switch (state)
        {
            case ParserState::REQUEST_LINE:
                if (!parseRequestLine() && error_code == 0)
                {
                    return;   // 请求行不完整，等待更多数据
                }
                else if (error_code != 0)
                {
                    callback_->onError(error_code);   // 请求行解析出错, 调用错误回调
                    return;
                }
                break;
            case ParserState::HEADERS:
                if (!parseHeaders() && error_code == 0)
                {
                    return;   // 请求头不完整，等待更多数据
                }
                else if (error_code != 0)
                {
                    callback_->onError(error_code);   // 请求头解析出错, 调用错误回调
                    return;
                }
                break;
            case ParserState::BODY_CONTENT_LENGTH:
                if (!parseBody() && error_code == 0)
                {
                    return;   // 请求体不完整，等待更多数据
                }
                else if (error_code != 0)
                {
                    callback_->onError(error_code);   // 请求体解析出错, 调用错误回调
                }
                break;
            case ParserState::BODY_CHUNKED:
                // 未实现Chunked传输编码的解析
                return;
            case ParserState::COMPLETE:
                return;
            case ParserState::ERROR:
                return;
        }
    }
}

bool HttpParser::parseRequestLine()
{
    //  Request-Line   = Method SP Request-URI SP HTTP-Version CRLF
    size_t pos = buffer.find("\r\n");
    if (pos == std::string::npos)
    {
        return false;   // 请求行未完整
    }

    std::string requestLine = buffer.substr(0, pos);
    buffer.erase(0, pos + 2);   // 移除请求行和CRLF

    size_t methodEnd = requestLine.find(' ');
    if (methodEnd == std::string::npos)
    {
        error_code = 400;   // 错误代码：400 Bad Request
        error_message = "Invalid request line, missing method";
        state = ParserState::ERROR;
        return false;   // 方法未找到
    }
    method = requestLine.substr(0, methodEnd);

    size_t uriEnd = requestLine.find(' ', methodEnd + 1);
    if (uriEnd == std::string::npos)
    {
        error_code = 400;   // 错误代码：400 Bad Request
        error_message = "Invalid request line, missing URI";
        state = ParserState::ERROR;
        return false;   // URI未找到
    }
    uri = requestLine.substr(methodEnd + 1, uriEnd - methodEnd - 1);

    version = requestLine.substr(uriEnd + 1);

    // 调用回调
    callback_->onRequestLine(method, uri, version);

    state = ParserState::HEADERS;
    return true;
}

/**
 * 解析HTTP请求头
 * Chunked传输编码未实现, 仅支持Content-Length
 */
bool HttpParser::parseHeaders()
{
    while (true)
    {
        size_t pos = buffer.find("\r\n");
        if (pos == std::string::npos)
        {
            return false;   // 当前行不完整，等待更多数据
        }

        if (pos == 0)   // 遇到空行，请求头结束
        {
            buffer.erase(0, 2);   // 移除最后的 CRLF

            callback_->onHeadersComplete();   // 请求头解析完成回调

            // 处理 Content-Length 和 Transfer-Encoding
            bool   isChunked = false;
            size_t contentLength = 0;
            auto   it = headers.find("Transfer-Encoding");
            if (it != headers.end() && it->second == "chunked")
            {
                isChunked = true;
            }

            it = headers.find("Content-Length");
            if (it != headers.end())
            {
                contentLength = std::stoul(it->second);
            }

            if (isChunked)
            {
                // 未实现Chunked传输编码的解析
                error_code = 501;   // 错误代码：501 Not Implemented
                error_message = "Chunked Transfer-Encoding not implemented";
                state = ParserState::ERROR;
                return false;
            }
            else if (contentLength > 0)
            {
                this->contentLength = contentLength;
                bodyBytesRead = 0;
                state = ParserState::BODY_CONTENT_LENGTH;   // 进入请求体解析状态
            }
            else
            {
                state = ParserState::COMPLETE;    // 如果没有请求体，直接进入完成状态
                callback_->onMessageComplete();   // 调用消息完成回调
            }
            return true;   // 状态已改变，返回 true 让 feed 循环继续处理后续数据
        }

        // 解析普通头部行
        std::string line = buffer.substr(0, pos);
        buffer.erase(0, pos + 2);   // 移除该行和 CRLF

        size_t colonPos = line.find(':');
        if (colonPos == std::string::npos)
        {
            error_code = 400;   // 错误代码：400 Bad Request
            error_message = "Invalid header line, missing colon";
            state = ParserState::ERROR;
            return false;   // 请求头行格式错误
        }

        std::string key = line.substr(0, colonPos);
        std::string value = line.substr(colonPos + 1);

        // 去除key和value的空格
        key.erase(key.find_last_not_of(" \t") + 1);
        value.erase(0, value.find_first_not_of(" \t"));

        headers[key] = value;              // 存储请求头
        callback_->onHeader(key, value);   // 调用请求头回调

        // 继续循环，处理 buffer 中可能存在的下一行
    }
}

bool HttpParser::parseBody()
{
    size_t bytesAvailable = buffer.size();
    size_t bytesToRead = std::min(contentLength - bodyBytesRead, bytesAvailable);

    if (bytesToRead > 0)
    {
        callback_->onBody(buffer.data(), bytesToRead);   // 调用请求体回调
        buffer.erase(0, bytesToRead);                    // 移除已读取的请求体数据
        bodyBytesRead += bytesToRead;
    }

    if (bodyBytesRead >= contentLength)
    {
        state = ParserState::COMPLETE;    // 请求体解析完成
        callback_->onMessageComplete();   // 调用消息完成回调
        return true;
    }

    return false;
}
