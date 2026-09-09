#pragma once
#include <algorithm>
#include <cstring>
#include <fstream>

#include "http/router/HttpRouter.h"

/**
 * @brief 处理静态文件请求
 */
class StaticFileHandler : public RequestHandler
{
public:
    StaticFileHandler(const std::string& file_path, std::shared_ptr<spdlog::logger> logger)
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

        std::string mime;

        auto ext = file_path_.substr(file_path_.find_last_of('.') + 1);
        std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);
        auto it = mime_map.find(ext);
        if (it != mime_map.end())
        {
            mime = it->second;
        }
        else
        {
            mime = "application/octet-stream";
        }

        response_.status_code = 200;
        response_.status_message = "OK";
        response_.headers["Content-Type"] = mime;
        response_.headers["Content-Length"] = std::to_string(response_.body.size());
    }

    void           onBody(const char*, size_t) override {}
    void           onEOM() override {}
    HttpResponse&& takeResponse() override { return std::move(response_); }

private:
    std::string                     file_path_;
    std::shared_ptr<spdlog::logger> logger_;

    static const std::unordered_map<std::string, std::string> mime_map;
};

const std::unordered_map<std::string, std::string> StaticFileHandler::mime_map = {
    {"html", "text/html"},
    {"css", "text/css"},
    {"js", "application/javascript"},
    {"json", "application/json"},
    {"png", "image/png"},
    {"jpg", "image/jpeg"},
    {"jpeg", "image/jpeg"},
    {"ico", "image/x-icon"},
    {"svg", "image/svg+xml"},
    {"webp", "image/webp"},
};
