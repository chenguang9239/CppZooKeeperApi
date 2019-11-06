//
// Created by admin on 2019-09-16.
//

#include "../zookeeper/include/zookeeper/zookeeper.h"
#include <iostream>
#include "../boost-log/log.h"
#include <unistd.h>

using CppZooKeeper::logger;

void getChildrenWatcherFunc(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx);

void getWatcherFunc(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx);

void existWatcherFunc(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx);

void printChildList(zhandle_t *zh, const char *path) {
    String_vector children;
    auto ret = zoo_wget_children(zh, path, getChildrenWatcherFunc, NULL, &children);
    M_LOG_INFO << "ret: " << ret << ", children count: " << children.count;
}

void printData(zhandle_t *zh, const char *path) {
    char buf[1024];
    int buflen = sizeof(buf);
    auto ret = zoo_wget(zh, path, getWatcherFunc, NULL, buf, &buflen, NULL);
    M_LOG_INFO << "ret: " << ret << ", data: " << buf;
}

void printNodeExist(zhandle_t *zh, const char *path) {
    auto ret = zoo_wexists(zh, path, existWatcherFunc, NULL, NULL);
    M_LOG_INFO << "ret: " << ret << ", node created or deleted";
}

void getChildrenWatcherFunc(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
    M_LOG_SPCL << "type: " << type << ", state: " << state << ", path: " << std::string(path);
    if (type == 2 || type == 4) {
        printChildList(zh, path);
    } else if (type == -1) {

    } else if (type == 1) {

    } else if (type == 2) {

    } else if (type == 3) {

    } else if (type == -2) {

    }
}

void getWatcherFunc(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
    M_LOG_SPCL << "type: " << type << ", state: " << state << ", path: " << std::string(path);
    if (type == 4) {

    } else if (type == -1) {

    } else if (type == 2 || type == 3) { // node data changed, node deleted
        printData(zh, path);
    } else if (type == -2) {

    }
}

void existWatcherFunc(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
    M_LOG_SPCL << "type: " << type << ", state: " << state << ", path: " << std::string(path);
    if (type == 4) {

    } else if (type == -1) {

    } else if (type == 1 || type == 2 || type == 3) { // node created, node data changed, node deleted
        printNodeExist(zh, path);
    } else if (type == -2) {

    }
}

void InnerGetChirldren(zhandle_t *zh) {
    M_LOG_SPCL << "InnerGetChirldren";
    std::string path = "/test_CZKAPI/tree3";
    printChildList(zh, path.c_str());

}

void InnerGet(zhandle_t *zh) {
    M_LOG_SPCL << "InnerGet";
    std::string path = "/test_CZKAPI/tree2/t1";
    printData(zh, path.c_str());
}

void InnerExist(zhandle_t *zh) {
    M_LOG_SPCL << "InnerExist";
    std::string path = "/test_CZKAPI/tree1/t1";
    printNodeExist(zh, path.c_str());
}

int main() {
    std::string zkAddr = "10.126.174.16:2181,10.126.174.11:2181,10.126.173.4:2181";
    zhandle_t *m_zhandle = zookeeper_init(zkAddr.c_str(), NULL, 3000, NULL, NULL, 0);

    logger::getInstance()->initLogger("./logs", "test_capi", logger::L_DEBUG, 5);

    InnerExist(m_zhandle);
    InnerGet(m_zhandle);
    InnerGetChirldren(m_zhandle);

    while (true) {
        usleep(5000);
    }

    return 0;
}
