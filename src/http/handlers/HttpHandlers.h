#pragma once
#include <cstring>
#include <fstream>
#include <string>
#include <utility>

#include <spdlog/spdlog.h>
#include <strings.h>

#include "http/router/HttpRouter.h"

// html文件处理器
class HtmlFileHandler : public RequestHandler
{
public:
    HtmlFileHandler(const std::string& file_path, std::shared_ptr<spdlog::logger> logger)
        : file_path_(file_path)
        , logger_(logger)
    {
    }

    void onRequest(HttpRequest& request, RouteParams& params) override
    {
        (void)params;
        if (request.method != HttpMethod::GET)
        {
            response_.status_code = 405;
            response_.status_message = "Method Not Allowed";
            response_.headers["Content-Length"] = "0";
            logger_->warn("[405] Method Not Allowed: {}", request.path);
            return;
        }

        std::ifstream file(file_path_);
        if (!file.is_open())
        {
            response_.status_code = 500;
            response_.status_message = "Internal Server Error";
            response_.headers["Content-Length"] = "0";
            logger_->error("Failed to open file: {}", file_path_);
            return;
        }

        char buffer[1024];
        while (file.read(buffer, sizeof(buffer)) || file.gcount() > 0)
        {
            response_.body.append(buffer, file.gcount());
        }

        response_.status_code = 200;
        response_.status_message = "OK";
        response_.headers["Content-Type"] = "text/html";
        response_.headers["Content-Length"] = std::to_string(response_.body.size());
    }

    void           onBody(const char*, size_t) override {}
    void           onEOM() override {}
    HttpResponse&& takeResponse() override { return std::move(response_); }

private:
    std::string                     file_path_;
    std::shared_ptr<spdlog::logger> logger_;
};

class ImageProcessHandler : public RequestHandler
{
public:
    explicit ImageProcessHandler(std::shared_ptr<spdlog::logger> logger)
        : logger_(std::move(logger))
    {
    }

    void onRequest(HttpRequest& request, RouteParams& params) override
    {
        (void)params;
        request_ = &request;
        if (request.method != HttpMethod::POST)
        {
            response_.status_code = 405;
            response_.status_message = "Method Not Allowed";
            response_.headers["Content-Length"] = "0";
            response_.headers["Connection"] = "close";
            logger_->warn("[405] Method Not Allowed: {}", request.path);
            rejected_ = true;
            return;
        }
    }

    void onBody(const char* data, size_t len) override
    {
        if (rejected_)
            return;
        body_.append(data, len);
    }

    void onEOM() override
    {
        if (rejected_)
            return;
        if (body_.empty())
        {
            response_.status_code = 400;
            response_.status_message = "Bad Request";
            response_.headers["Content-Length"] = "0";
            response_.headers["Connection"] = "close";
            return;
        }

        std::string content_type =
            get_header_value_ci(request_ ? request_->headers : Headers{}, "Content-Type");
        if (!content_type.empty())
        {
            auto semi = content_type.find(';');
            if (semi != std::string::npos)
                content_type = content_type.substr(0, semi);
        }

        // 只处理原始图片字节，对使用multipart/form-data上传的文件返回415，因为为简单起见，目前不做
        // multipart/form-data 的解析。
        if (!content_type.empty() && content_type.find("multipart/form-data") != std::string::npos)
        {
            response_.status_code = 415;
            response_.status_message = "Unsupported Media Type";
            response_.headers["Content-Length"] = "0";
            response_.headers["Connection"] = "close";
            logger_->warn("[415] multipart/form-data not supported; send raw image bytes");
            return;
        }

        if (content_type.empty() || content_type.find("image/") != 0)
        {
            content_type = sniff_image_mime(body_);
        }

        // Minimal "processing": echo back the bytes.
        response_.status_code = 200;
        response_.status_message = "OK";
        response_.headers["Content-Type"] =
            content_type.empty() ? "application/octet-stream" : content_type;
        response_.headers["Cache-Control"] = "no-store";
        response_.headers["X-Image-Processed"] = "1";
        response_.headers["Connection"] = "close";
        response_.body = std::move(body_);
        response_.headers["Content-Length"] = std::to_string(response_.body.size());
    }

    HttpResponse&& takeResponse() override { return std::move(response_); }

private:
    std::shared_ptr<spdlog::logger> logger_;
    HttpRequest*                    request_ = nullptr;
    std::string                     body_;
    bool                            rejected_ = false;

    static std::string get_header_value_ci(const Headers& headers, const std::string& key)
    {
        for (const auto& kv : headers)
        {
            if (strcasecmp(kv.first.c_str(), key.c_str()) == 0)
                return kv.second;
        }
        return "";
    }

    static std::string sniff_image_mime(const std::string& bytes)
    {
        if (bytes.size() >= 8)
        {
            const unsigned char* p = reinterpret_cast<const unsigned char*>(bytes.data());
            // PNG: 89 50 4E 47 0D 0A 1A 0A
            if (p[0] == 0x89 && p[1] == 0x50 && p[2] == 0x4E && p[3] == 0x47 && p[4] == 0x0D &&
                p[5] == 0x0A && p[6] == 0x1A && p[7] == 0x0A)
                return "image/png";
        }
        if (bytes.size() >= 3)
        {
            const unsigned char* p = reinterpret_cast<const unsigned char*>(bytes.data());
            // JPEG: FF D8 FF
            if (p[0] == 0xFF && p[1] == 0xD8 && p[2] == 0xFF)
                return "image/jpeg";
        }
        if (bytes.size() >= 6)
        {
            // GIF: GIF87a / GIF89a
            if (bytes.rfind("GIF87a", 0) == 0 || bytes.rfind("GIF89a", 0) == 0)
                return "image/gif";
        }
        if (bytes.size() >= 12)
        {
            // WEBP: RIFF....WEBP
            if (bytes.rfind("RIFF", 0) == 0 && bytes.compare(8, 4, "WEBP") == 0)
                return "image/webp";
        }
        return "";
    }
};
