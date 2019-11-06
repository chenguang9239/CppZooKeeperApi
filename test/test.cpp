//
// Created by admin on 2019-09-16.
//

#include "CppZooKeeper.h"
#include <iostream>
#include <unistd.h>
#include <sys/syscall.h>

using CppZooKeeper::logger;

bool getChildrenWatcherFunc(CppZooKeeper::ZookeeperManager &zkCli, int type, int state, const char *path);

bool getWatcherFunc(CppZooKeeper::ZookeeperManager &zkCli, int type, int state, const char *path);

bool existWatcherFunc(CppZooKeeper::ZookeeperManager &zkCli, int type, int state, const char *path);

std::string getNodeValue(CppZooKeeper::ZookeeperManager &zkClient, const std::string &path) {
    char *buffer = nullptr;
    Stat stat;
    int buf_len = 0;

    int ret = zkClient.Get(path, nullptr, &buf_len, &stat);
    if (ret != ZOK || stat.dataLength <= 0) {
//        M_LOG_ERROR << "ret: " << ret << ", dataLength: " << stat.dataLength;
        return "";
    }

    buffer = (char *) malloc(sizeof(char) * (stat.dataLength + 1));
    buf_len = stat.dataLength + 1;

    ret = zkClient.Get(path, buffer, &buf_len, &stat);
    if (ret != ZOK || buf_len <= 0) {
//        M_LOG_ERROR << "ret: " << ret << ", buf_len: " << buf_len;
        if (buffer != nullptr) {
            free(buffer);
            buffer = nullptr;
        }
        return "";
    }

    std::string return_str = std::string(buffer, buf_len);

    if (buffer != nullptr) {
        free(buffer);
        buffer = nullptr;
    }

//    boost::algorithm::trim_if(return_str, boost::algorithm::is_any_of("/"));
    return return_str;
}

void printChildChangeEvent(CppZooKeeper::ZookeeperManager &zkClient) {
    M_LOG_SPCL << "child change, but user will not register again";
}

void printChildList(CppZooKeeper::ZookeeperManager &zkClient) {
    std::string path("/test_CPPZKAPI/tree3");
    auto watcherFuncPtr = std::make_shared<CppZooKeeper::WatcherFuncType>(std::bind(&getChildrenWatcherFunc,
                                                                                    std::placeholders::_1,
                                                                                    std::placeholders::_2,
                                                                                    std::placeholders::_3,
                                                                                    std::placeholders::_4));
    CppZooKeeper::ScopedStringVector strings;
    zkClient.GetChildren(path, strings, watcherFuncPtr);
    if (strings.count == 0) M_LOG_SPCL << "no node in path: " << path;
    for (auto i = 0; i < strings.count; ++i) {
        std::string tmpValue = getNodeValue(zkClient, path + "/" + strings.data[i]);
        M_LOG_SPCL << "node path: " << strings.data[i] << ", node node value: " << tmpValue;
    }
}

void printDataChangeEvent(CppZooKeeper::ZookeeperManager &zkClient) {
    M_LOG_SPCL << "data change, but user will not register again";
}

void printData(CppZooKeeper::ZookeeperManager &zkClient) {
    std::string path("/test_CPPZKAPI/tree2/t1");
    auto watcherFuncPtr = std::make_shared<CppZooKeeper::WatcherFuncType>(std::bind(&getWatcherFunc,
                                                                                    std::placeholders::_1,
                                                                                    std::placeholders::_2,
                                                                                    std::placeholders::_3,
                                                                                    std::placeholders::_4));
    char buf[1024];
    int buflen = 1024;
    zkClient.Get(path.c_str(), buf, &buflen, NULL, watcherFuncPtr);
    M_LOG_SPCL << "node path: " << path << ", data: " << buf;
}

void printNodeExistEvent(CppZooKeeper::ZookeeperManager &zkClient) {
    M_LOG_SPCL << "node exist change, but user will not register again";
}

void printNodeExist(CppZooKeeper::ZookeeperManager &zkClient) {
    std::string path("/test_CPPZKAPI/tree1/t1");
    auto watcherFuncPtr = std::make_shared<CppZooKeeper::WatcherFuncType>(std::bind(&existWatcherFunc,
                                                                                    std::placeholders::_1,
                                                                                    std::placeholders::_2,
                                                                                    std::placeholders::_3,
                                                                                    std::placeholders::_4));
    auto ret = zkClient.Exists(path.c_str(), NULL, watcherFuncPtr);
    M_LOG_SPCL << "node path: " << path << ", exist? : " << ret;
}

bool globalWatcherFunc(CppZooKeeper::ZookeeperManager &zkCli, int type, int state, const char *path) {
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) { // 第一次连接成功与超时之后的重连成功，会触发ZOO_CONNECTED_STATE
            // 不超时的重连成功也会触发会触发ZOO_CONNECTED_STATE,此时needToInitValueList为false
            std::cout << "连接成功事件！！！" << std::endl;
            M_LOG_SPCL << "连接成功事件！！";
//            printChildList(zkCli);
        } else if (state == ZOO_EXPIRED_SESSION_STATE) { // 超时会触发ZOO_EXPIRED_SESSION_STATE
            std::cout << "连接超时事件！！！" << std::endl;
            M_LOG_SPCL << "连接超时事件！!";
        }
    }

    // 测试满足一定条件，自动取消重注册以及用户再次恢复不满足条件时的重注册
    static int count = 0;
    if (++count >= 3) {
        M_LOG_SPCL << "count >= 3, user do not want to auto re-register";
        return true;
    } else {
        M_LOG_SPCL << "count: " << count << " < 3, user want to auto re-register";
        return false;
    }
//    return false;
}

bool getChildrenWatcherFunc(CppZooKeeper::ZookeeperManager &zkCli, int type, int state, const char *path) {
    M_LOG_SPCL << "type: " << type << ", state: " << state << ", path: " << std::string(path);
    if ((type == CppZooKeeper::RESUME_EVENT && state == CppZooKeeper::RESUME_SUCC) ||
        type == 1 || type == 2 || type == 4) {

        // 判断是否为固定一个线程在处理event
        M_LOG_SPCL << "current thread id is: " << syscall(__NR_gettid);
        // 判断是否为串行处理event
//        M_LOG_SPCL << "now sleep 60s";
//        sleep(60);

        if (type == CppZooKeeper::RESUME_EVENT && state == CppZooKeeper::RESUME_SUCC) {
            M_LOG_WARN << "resume environment success, so user watcher func is calling now";
        }
//        printChildList(zkCli);
        printChildChangeEvent(zkCli);
    } else if (type == -1) {
        M_LOG_ERROR << "custom watcher should not triggered by ZOO_SESSION_EVENT";
    } else if (type == 3) {

    } else if (type == -2) {

    }

    // 测试满足一定条件，自动取消重注册以及用户再次恢复不满足条件时的重注册
    static int count = 0;
    if (++count >= 3) {
        M_LOG_SPCL << "count >= 3, user do not want to auto re-register";
        return true;
    } else {
        M_LOG_SPCL << "count: " << count << " < 3, user want to auto re-register";
        return false;
    }
//    return false;
}

bool getWatcherFunc(CppZooKeeper::ZookeeperManager &zkCli, int type, int state, const char *path) {
    M_LOG_SPCL << "type: " << type << ", state: " << state << ", path: " << std::string(path);
    if (type == 4) {

    } else if (type == -1) {
        M_LOG_ERROR << "custom watcher should not triggered by ZOO_SESSION_EVENT";
    } else if ((type == CppZooKeeper::RESUME_EVENT && state == CppZooKeeper::RESUME_SUCC) ||
               type == 1 || type == 2 || type == 3) { // node created, node deleted, node data changed

        // 判断是否为固定一个线程在处理event
        M_LOG_SPCL << "current thread id is: " << syscall(__NR_gettid);
        // 判断是否为串行处理event
//        M_LOG_SPCL << "now sleep 60s";
//        sleep(60);

        if (type == CppZooKeeper::RESUME_EVENT && state == CppZooKeeper::RESUME_SUCC) {
            M_LOG_WARN << "resume environment success, so user watcher func is calling now";
        }
        printDataChangeEvent(zkCli);
//        printData(zkCli);
    } else if (type == -2) {

    }

    // 测试满足一定条件，自动取消重注册以及用户再次恢复不满足条件时的重注册
    static int count = 0;
    if (++count >= 3) {
        M_LOG_SPCL << "count >= 3, user do not want to auto re-register";
        return true;
    } else {
        M_LOG_SPCL << "count: " << count << " < 3, user want to auto re-register";
        return false;
    }
//    return false;
}

bool existWatcherFunc(CppZooKeeper::ZookeeperManager &zkCli, int type, int state, const char *path) {
    M_LOG_SPCL << "type: " << type << ", state: " << state << ", path: " << std::string(path);
    if (type == 4) {

    } else if (type == -1) {
        M_LOG_ERROR << "custom watcher should not triggered by ZOO_SESSION_EVENT";
    } else if ((type == CppZooKeeper::RESUME_EVENT && state == CppZooKeeper::RESUME_SUCC) ||
               type == 1 || type == 2 || type == 3) {

        // 判断是否为固定一个线程在处理event
        M_LOG_SPCL << "current thread id is: " << syscall(__NR_gettid);
        // 判断是否为串行处理event
//        M_LOG_SPCL << "now sleep 60s";
//        sleep(60);

        if (type == CppZooKeeper::RESUME_EVENT && state == CppZooKeeper::RESUME_SUCC) {
            M_LOG_WARN << "resume environment success, so user watcher func is calling now";
        }
        printNodeExistEvent(zkCli);
//        printNodeExist(zkCli);
    } else if (type == -2) {

    }

    // 测试满足一定条件，自动取消重注册以及用户再次恢复不满足条件时的重注册
    static int count = 0;
    if (++count >= 3) {
        M_LOG_SPCL << "count >= 3, user do not want to auto re-register";
        return true;
    } else {
        M_LOG_SPCL << "count: " << count << " < 3, user want to auto re-register";
        return false;
    }
//    return false;
}

void InnerGetChirldren(CppZooKeeper::ZookeeperManager &zkClient) {
    M_LOG_SPCL << "InnerGetChirldren";
    printChildList(zkClient);
}

void InnerGet(CppZooKeeper::ZookeeperManager &zkClient) {
    M_LOG_SPCL << "InnerGet";
    printData(zkClient);
}

void InnerExist(CppZooKeeper::ZookeeperManager &zkClient) {
    M_LOG_SPCL << "InnerExist";
    printNodeExist(zkClient);
}

void reconnectNotifier() {
    M_LOG_ERROR << "reconnection!!!";
}

void resumeGlobalWatcherNotifier() {
    M_LOG_ERROR << "resume Global watcher!!!";
}

void resumeCustomWatcherNotifier() {
    M_LOG_ERROR << "resume custom watcher!!!";
}

void resumeTmpNodeNotifier() {
    M_LOG_ERROR << "resume tmp node!!!";
}

int main() {
    auto globalWatherPtr = std::make_shared<CppZooKeeper::WatcherFuncType>(std::bind(globalWatcherFunc,
                                                                                     std::placeholders::_1,
                                                                                     std::placeholders::_2,
                                                                                     std::placeholders::_3,
                                                                                     std::placeholders::_4));
    CppZooKeeper::ZookeeperManager zkClient;

    std::string zkAddr = "10.126.11.26:2181,10.136.132.13:2181,10.126.127.23:2181";
//    std::string zkAddr = "10.126.174.16:2181,10.126.174.11:2181,10.126.173.4:2181";

    CppZooKeeper::InitCPPAPILogger("./logs", "ZKCPPAPI", logger::L_DEBUG, 5);

    CppZooKeeper::InitCAPILogger("./logs/ZKCAPI.log", ZOO_LOG_LEVEL_DEBUG);

    zkClient.Init(zkAddr);

    // 测试重连以及恢复机制
    zkClient.SetReconnectOptions(reconnectNotifier, 1);
    zkClient.SetResumeOptions(resumeTmpNodeNotifier, resumeCustomWatcherNotifier, resumeGlobalWatcherNotifier, 1);
    zkClient.SetCallWatcherFuncOnResume(true);

    int i = 0;
    while (++i <= 3) {
        if (ZOK == zkClient.Connect(globalWatherPtr, 3000, 3000)) {
            break;
        } else {
            M_LOG_ERROR << "connect to zk server error, time(s): " << i
                      << ", zk addr: " << zkAddr << std::endl;
        }
    }

    if (i == 4) {
        M_LOG_ERROR << "can not connect to zk server, quit";
        return 0;
    }

    // 测试创建结点、删除结点、删除不存在的结点、递归删除结点
    {
        std::string returnedPath;
        returnedPath.resize(1024);
        zkClient.CreateRecursively("/test_CPPZKAPI/tree/10.136.132.13:1234", "empty value", &returnedPath,
                                   &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL);

        zkClient.DeletePathRecursion("/test_CPPZKAPI/tree/10.136.132.13:1234");
        zkClient.DeletePathRecursion("/test_CPPZKAPI/tree/10.136.132.13:1234");
    }

    {
        std::string returnedPath;
        returnedPath.resize(1024);
        zkClient.CreateRecursively("/test_CPPZKAPI/tree/10.136.132.13:5678", "empty value", &returnedPath,
                                   &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL);

        zkClient.Delete("/test_CPPZKAPI/tree/10.136.132.13:5678");
        zkClient.Delete("/test_CPPZKAPI/tree/10.136.132.13:5678");
        zkClient.Delete("/test_CPPZKAPI/tree2");
    }

    {
        std::string returnedPath;
        returnedPath.resize(1024);
        zkClient.CreateRecursively("/test_CPPZKAPI/tree/10.136.132.13:123", "empty value", &returnedPath,
                                   &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL);
        zkClient.CreateRecursively("/test_CPPZKAPI/tree/10.136.132.13:234", "empty value", &returnedPath,
                                   &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL);
        zkClient.CreateRecursively("/test_CPPZKAPI/tree/10.136.132.13:345", "empty value", &returnedPath,
                                   &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL);
        zkClient.CreateRecursively("/test_CPPZKAPI/tree/10.136.132.13:456", "empty value", &returnedPath,
                                   &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL);

        zkClient.DeletePathRecursion("/test_CPPZKAPI/tree");
        zkClient.DeletePathRecursion("/test_CPPZKAPI/tree");
    }

    {
        std::string returnedPath;
        returnedPath.resize(1024);
        zkClient.CreateRecursively("/test_CPPZKAPI/tree/10.136.132.13:1234567", "empty value", &returnedPath,
                                   &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL);
        if (returnedPath.find("/test_CPPZKAPI/tree/10.136.132.13:1234567") == 0) {
            M_LOG_SPCL << "create node success";
        }
    }

    M_LOG_SPCL << "current thread id is: " << syscall(__NR_gettid);

    // 测试自动重注册，custom watcher
    InnerGetChirldren(zkClient);
    InnerGet(zkClient);
    InnerExist(zkClient);

    // 测试自动重注册，global watcher
//    ScopedStringVector children;
//    zkClient.GetChildren("/test_CPPZKAPI/tree3", children, 1);
//
//    char buf[1024];
//    int buflen = sizeof(buf);
//    ret = zkClient.Get("/test_CPPZKAPI/tree2/t1", &buf, &buflen, NULL, 1);
//
//    zkClient.Exists("/test_CPPZKAPI/tree1/t1", NULL, 1);

    while (true) {
        usleep(5000);
        std::cout << "input 1, and will set watcher again, "
                     "and cpp zk client will auto re-register watcher under some condition";
        int a;
        std::cin >> a;
        if (a == 1) {
            InnerGetChirldren(zkClient);
            InnerGet(zkClient);
            InnerExist(zkClient);
        }
    }

    return 0;
}
