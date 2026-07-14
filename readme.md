# ServerEvolution

这是一个从零开始构建的 C++ 服务器学习项目。

本项目旨在从一个最基本的迭代服务器出发，逐步完善至现代的 Reactor 模型。通过这一过程，探究现代服务器架构中各种技术（如线程池、Epoll、Reactor等）是为了解决什么具体问题。

## 项目背景

此项目起源于我之前学习实现的一个 HTTP Server。在过程中我虽然使用了 Epoll、Reactor 等技术，但对为什么要这么做以及性能提升究竟有多大还是比较模糊。因此，我希望以总结历史的视角，从最简单的阻塞式迭代服务器开始，通过代码迭代来回答这些问题。

## 项目架构

项目中的服务器主要包含以下几个阶段：
1. 循环处理连接的阻塞式服务器
2. 使用多线程实现的并发式服务器
3. 使用IO多路复用实现的并发式(并非真正的并发)服务器
4. 基于 Reactor 架构的现代式服务器模型

每一阶段对应一个独立的可编译、可运行的 Server 实现，你可以在 `apps/` 目录中找到它们的源码

现在，各个阶段的服务器已经基本实现，目前正在做文档以及代码的整理（依职责拆分文件，复用公共函数等），后续计划做对各阶段 Server 性能提升的量化衡量，和瓶颈点测试。
```

## 快速开始

### 环境要求
- Linux（依赖 epoll、eventfd 等 Linux 特有 API）
- G++ ≥ 11.4（支持 C++11）
- CMake ≥ 3.10

### 安装与运行

```sh
# 1. 克隆仓库
git clone https://github.com/Baixiang-dev/ServerEvolution.git
cd ServerEvolution

# 2. 编译
cmake -S . -B build
cmake --build build/

# 3. 运行服务器 (默认地址: http://127.0.0.1:7788/)
./build/bin/multi_reactor_server
```

启动后，你会看到类似下面的输出：
```text
Press Ctrl+C to stop the server...
```

此时在浏览器访问 `http://localhost:7788`，即可看到 `index.html` 的内容。

## Third-Party Code

本项目使用了以下开源仓库的代码，感谢原作者的贡献：

- [spdlog](https://github.com/gabime/spdlog)  
  A fast C++ logging library.  
  License: MIT

- [ThreadPool](https://github.com/progschj/ThreadPool)  
  A simple C++11 thread pool implementation.  
  License: Zlib
