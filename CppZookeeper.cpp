#include "CppZooKeeper.h"

#include <stdio.h>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/syscall.h>


//#include <boost/asio.hpp>
//#include <boost/algorithm/string/trim.hpp>
//#include <boost/algorithm/string/split.hpp>

#ifdef CPP_ZK_USE_BOOST
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#endif

// hashtable_search需要包含
#include "zookeeper/hashtable/hashtable_private.h"
#include "zookeeper/zk_adaptor.h"

// 这个宏必须加上, 因为封装API基于多线程, 多线程和单线程的对象是不一样的, 二者不能共用
#define THREADED

// 用于删除注册的Watcher, 数据结构从zk_hashtable.c中获得
typedef struct _watcher_object {
    watcher_fn watcher;
    void *context;
    struct _watcher_object *next;
} watcher_object_t;

struct _zk_hashtable {
    struct hashtable *ht;
};

struct watcher_object_list {
    watcher_object_t *head;
};

using namespace std;

namespace CppZooKeeper {

void InitCPPAPILogger(const std::string &path, const std::string &name,
                      logger::LEVEL level, unsigned int flushPeriod) {
    logger::getInstance()->initLogger(path, name, level, flushPeriod);
    M_LOG_SPCL << "set zkCPPAPI log path: " << path << ", log name: " << name << ", flush period: " << flushPeriod;
}

void InitCAPILogger(const std::string &path, ZooLogLevel level) {
    zoo_set_debug_level(level);
    if (!path.empty()) {
        FILE *fp = nullptr;
        fp = fopen(path.c_str(), "a+");
        if (nullptr == fp) M_LOG_ERROR << "can not create zkCAPI log: " << path;
        zoo_set_log_stream(fp);
        M_LOG_SPCL << "set zkCAPI log: " << path;
    }
}

void SplitStr(string str, const vector<string> &splitStr,
              vector<string> &result, bool removeEmptyElm = true, size_t maxCount = 0) {
    result.clear();
    // 当前已获得段数
    size_t currCount = 0;

    // 从所有分割字符串中查找最小的索引
    size_t index = string::npos;
    size_t splitLen = 0;
    size_t currIndex;
    for (vector<string>::const_iterator it = splitStr.begin(); it != splitStr.end(); ++it) {
        if (it->length() == 0) { continue; }

        currIndex = str.find(*it);
        if (currIndex != string::npos && currIndex < index) {
            index = currIndex;
            splitLen = it->length();
        }
    }

    while (index != string::npos) {
        if (index != 0 || !removeEmptyElm) {
            // 将找到的字符串放入结果中
            ++currCount;
            if (maxCount > 0 && currCount >= maxCount) { break; }
            result.push_back(str.substr(0, index));
        }

        // 将之前的字符和分割符都删除
        str.erase(str.begin(), str.begin() + index + splitLen);

        // 继续查找下一个
        index = string::npos;
        for (vector<string>::const_iterator it = splitStr.begin(); it != splitStr.end(); ++it) {
            if (it->length() == 0) { continue; }

            currIndex = str.find(*it);
            if (currIndex != string::npos && currIndex < index) {
                index = currIndex;
                splitLen = it->length();
            }
        }
    }

    // 把剩下的放进去
    if (str.length() > 0 || !removeEmptyElm) {
        result.push_back(str);
    }
}

void SplitStr(string str, const string &splitStr,
              vector<string> &result, bool removeEmptyElm = true, size_t maxCount = 0) {
    SplitStr(str, vector<string>(1, splitStr), result, removeEmptyElm, maxCount);
}

enum GlobalWatcherType {
    WATCHER_GET = 1, // 00000001
    WATCHER_EXISTS = 2, // 00000010
    WATCHER_GET_CHILDREN = 4 // 00000100
};

class ZookeeperCtx {
public:
    enum WatcherType {
        NOT_WATCH,
        GLOBAL,
        EXISTS,
        GET,
        GET_CHILDREN,
    };

    ZookeeperCtx(ZookeeperManager &zookeeper_manager,
                 WatcherType watcher_type = NOT_WATCH,
                 bool need_reg_watcher = true) :
            m_is_stop(false),
            m_zookeeper_manager(zookeeper_manager),
            m_auto_reg_watcher(need_reg_watcher),
            m_watcher_type(watcher_type),
            m_global_watcher_add_type(0) {
    }

    virtual ~ZookeeperCtx() {
    }

    bool m_is_stop;

    ZookeeperManager &m_zookeeper_manager;

    shared_ptr<WatcherFuncType> m_watcher_func;
    shared_ptr<AclCompletionFuncType> m_acl_completion_func;
    shared_ptr<VoidCompletionFuncType> m_void_completion_func;
    shared_ptr<StatCompletionFuncType> m_stat_completion_func;
    shared_ptr<DataCompletionFuncType> m_data_completion_func;
    shared_ptr<MultiCompletionFuncType> m_multi_completion_func;
    shared_ptr<StringCompletionFuncType> m_string_completion_func;
    shared_ptr<StringsStatCompletionFuncType> m_strings_stat_completion_func;

    // 是否自动重新注册Watcher, 重新注册的话, 会使用原有的Context
    bool m_auto_reg_watcher;

    // 当前ctx是由什么操作触发的
    WatcherType m_watcher_type;

    // 临时节点信息, 不为空时用于异步操作成功后写入临时节点信息, 
    // 如果m_ephemeral_path不为空, m_ephemeral_info为NULL, 表示临时节点信息被删除, 在VoidCompletionFuncType中需要处理
    string m_ephemeral_path;
    // 临时节点信息, 不为NULL时用于异步操作成功后写入临时节点信息
    shared_ptr<EphemeralNodeInfo> m_ephemeral_info;

    // Watch的Path, 用于异步操作成功后添加Watcher信息
    string m_watch_path;
    // 自定义Watcher的Context, 用于异步操作成功后添加Watcher信息
    shared_ptr<ZookeeperCtx> m_custom_watcher_context;
    // 全局Watcher要添加的类型, 用于异步操作成功后添加全局Watcher信息
    uint8_t m_global_watcher_add_type;

    // 批量操作相关数据
    // 批量操作请求
    shared_ptr<MultiOps> m_multi_ops;
    // 批量操作结果
    shared_ptr<vector<zoo_op_result_t>> m_multi_results;

private:
    ZookeeperCtx(ZookeeperCtx &&right) = delete;

    ZookeeperCtx(const ZookeeperCtx &right) = delete;

    ZookeeperCtx &operator=(ZookeeperCtx &&right) = delete;

    ZookeeperCtx &operator=(const ZookeeperCtx &right) = delete;
};

ZookeeperManager::ZookeeperManager()
        : reconnecting(false), m_dont_close(false), m_zhandle(NULL), m_zk_tid(0) {
//        : m_dont_close(false), m_zhandle(NULL), m_zk_tid(0), m_need_resume_env(false) {
    m_zk_client_id.client_id = 0;
}

#ifdef CPP_ZK_USE_BOOST
int32_t ZookeeperManager::InitFromFile(const string &config_file_path, const clientid_t *client_id/*= NULL*/)
{
    try{
        boost::property_tree::ptree zk_conf_pt;
        read_xml(config_file_path, zk_conf_pt);

        string hosts = zk_conf_pt.get<string>("ZkConf.Hosts", "");
        string root_path = zk_conf_pt.get<string>("ZkConf.Root", "/");

        return Init(hosts, root_path, client_id);
    }catch (const boost::property_tree::xml_parser_error &e){
        M_LOG_ERROR << "read zookeeper path failed from : " << config_file_path << ", exception: " << e.what());
        return ZSYSTEMERROR;
    }
}
#endif

int32_t ZookeeperManager::Init(const string &hosts,
                               const string &root_path /*= "/"*/,
                               const clientid_t *client_id/*= NULL*/) {
    m_hosts = hosts;
    m_root_path = root_path;
//    m_need_resume_env = false;

    if (m_root_path.empty() || m_root_path.at(0) != '/') {
        M_LOG_ERROR << "invalid API root path: " << m_root_path;
        return ZBADARGUMENTS;
    }

    if (client_id != NULL) { m_zk_client_id = *client_id; }

    // default value
    reconnectMaxCount = INT_MAX;
    resumeMaxCount = 5;
    reconnectAlertCount = 3;
    resumeAlertCount = 3;
    callWatcherFuncOnResume = true;

    M_LOG_SPCL << "zookeeper config: m_hosts[" << m_hosts
               << "], m_root_path[" << m_root_path
               << "], client_id[" << m_zk_client_id.client_id
               << "], default reconnectMaxCount[" << reconnectMaxCount
               << "], default resumeMaxCount[" << resumeMaxCount
               << "], default reconnectAlertCount[" << reconnectAlertCount
               << "], default resumeAlertCount[" << resumeAlertCount
               << "], default callWatcherFuncOnResume[" << callWatcherFuncOnResume << "]";

    return ZOK;
}

void ZookeeperManager::SetReconnectOptions(std::function<void()> userReconnectAlertNotifier,
                                           int userReconnectAlertCount, int userReconnectMaxCount) {
    if (userReconnectAlertNotifier) {
        reconnectAlertNotifier = userReconnectAlertNotifier;
        M_LOG_SPCL << "set user specified reconnectAlertNotifier ok";
    } else { M_LOG_ERROR << "user specified reconnectAlertNotifier is nullptr"; }

    if (userReconnectMaxCount > 0) reconnectMaxCount = userReconnectMaxCount;
    else { M_LOG_ERROR << "user specified reconnectMaxCount is invalid: " << userReconnectMaxCount; }
    if (userReconnectAlertCount > 0) reconnectAlertCount = userReconnectAlertCount;
    else { M_LOG_ERROR << "user specified reconnectAlertCount is invalid: " << userReconnectAlertCount; }
    M_LOG_SPCL << "set reconnectMaxCount: " << reconnectMaxCount << ", reconnectAlertCount: " << reconnectAlertCount;
}

void ZookeeperManager::SetResumeOptions(std::function<void()> userResumeEphemeralNodeAlertNotifier,
                                        std::function<void()> userResumeCustomWatcherAlertNotifier,
                                        std::function<void()> userResumeGlobalWatcherAlertNotifier,
                                        int userResumeAlertCount, int userResumeMaxCount) {
    if (userResumeEphemeralNodeAlertNotifier) {
        resumeEphemeralNodeAlertNotifier = userResumeEphemeralNodeAlertNotifier;
        M_LOG_SPCL << "set user specified resumeEphemeralNodeAlertNotifier ok";
    } else { M_LOG_ERROR << "user specified reconnectAlertNotifier is nullptr"; }

    if (userResumeCustomWatcherAlertNotifier) {
        resumeCustomWatcherAlertNotifier = userResumeCustomWatcherAlertNotifier;
        M_LOG_SPCL << "set user specified resumeCustomWatcherAlertNotifier ok";
    } else { M_LOG_ERROR << "user specified resumeCustomWatcherAlertNotifier is nullptr"; }

    if (userResumeGlobalWatcherAlertNotifier) {
        resumeGlobalWatcherAlertNotifier = userResumeGlobalWatcherAlertNotifier;
        M_LOG_SPCL << "set user specified resumeGlobalWatcherAlertNotifier ok";
    } else { M_LOG_ERROR << "user specified resumeGlobalWatcherAlertNotifier is nullptr"; }

    if (userResumeMaxCount > 0) resumeMaxCount = userResumeMaxCount;
    else { M_LOG_ERROR << "user specified resumeMaxCount is invalid: " << userResumeMaxCount; }
    if (userResumeAlertCount > 0) resumeAlertCount = userResumeAlertCount;
    else { M_LOG_ERROR << "user specified resumeAlertCount is invalid: " << userResumeAlertCount; }
    M_LOG_SPCL << "set resumeMaxCount: " << resumeMaxCount << ", resumeAlertCount: " << resumeAlertCount;
}

void ZookeeperManager::SetCallWatcherFuncOnResume(bool flag) {
    callWatcherFuncOnResume = flag;
    M_LOG_SPCL << "set callWatcherFuncOnResume: " << callWatcherFuncOnResume;
}

int32_t ZookeeperManager::Connect(shared_ptr<WatcherFuncType> global_watcher_fun,
                                  int32_t recv_timeout_ms, uint32_t conn_timeout_ms /*= 3000*/) {
    m_zk_tid = 0;
//    m_need_resume_env = true;
    if (m_zhandle != NULL) {
        M_LOG_SPCL << "close none NULL m_zhandle!";
        zookeeper_close(m_zhandle);
        m_zhandle = NULL;
    }

    // Watcher已经变了, 重置
    if (m_global_watcher_context == NULL || m_global_watcher_context->m_watcher_func != global_watcher_fun) {
        m_global_watcher_context = make_shared<ZookeeperCtx>(*this, ZookeeperCtx::GLOBAL);
        m_global_watcher_context->m_watcher_func = global_watcher_fun;
    }

    M_LOG_SPCL << "start connecting...";
    m_zhandle = zookeeper_init(m_hosts.c_str(), &ZookeeperManager::InnerWatcherCbFunc, recv_timeout_ms,
                               m_zk_client_id.client_id != 0 ? &m_zk_client_id : NULL,
                               m_global_watcher_context.get(), 0);

    if (m_zhandle == NULL) {
        M_LOG_ERROR << "zookeeper_init error, return NULL zhandle, host[" << m_hosts
                    << "], errno[" << errno << "], error[" << ZError(errno) << "]";
        if (errno == ZOK) { return ZSYSTEMERROR; }
        return errno;
    }

    // 等待连接建立
    if (syscall(__NR_gettid) != m_zk_tid) {
        M_LOG_SPCL << "wait for connected event...";
        unique_lock<mutex> conn_lock(m_connect_lock);
        while (GetStatus() != ZOO_CONNECTED_STATE) {
            if (conn_timeout_ms > 0) {
                m_connect_cond.wait_for(conn_lock, chrono::milliseconds(conn_timeout_ms));
                if (GetStatus() != ZOO_CONNECTED_STATE) {
                    M_LOG_ERROR << "connect timeout!";
                    return ZOPERATIONTIMEOUT;
                }
            } else {
                m_connect_cond.wait(conn_lock);
            }
        }

        const clientid_t *p_curr_client_id = zoo_client_id(m_zhandle);
        if (p_curr_client_id != NULL) {
            m_zk_client_id = *p_curr_client_id;
        }
        M_LOG_SPCL << "connected, client_id[" << m_zk_client_id.client_id << "]";
    }

    return ZOK;
}

int32_t ZookeeperManager::Reconnect() {
    M_LOG_SPCL << "start reconnecting...";

    // 清空ClientID, 因为session过期才会进行重连, 此时ClinetID已经无效了
    m_zk_client_id.client_id = 0;
    ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(
            reinterpret_cast<const ZookeeperCtx *>(zoo_get_context(m_zhandle)));
    if (p_context == NULL) {
        M_LOG_ERROR << "context is NULL!";
        return ZSYSTEMERROR;
    }

    return Connect(p_context->m_watcher_func, zoo_recv_timeout(m_zhandle));
}

ZookeeperManager::~ZookeeperManager() {
    if (m_zhandle != NULL && !m_dont_close) {
        zookeeper_close(m_zhandle);
        m_zhandle = NULL;
    }
}

int32_t ZookeeperManager::AExists(const string &path,
                                  shared_ptr<StatCompletionFuncType> stat_completion_fun, int watch /*= 0*/) {
    int32_t ret = ZOK;
    ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
    p_zookeeper_context->m_stat_completion_func = stat_completion_fun;

    string abs_path = ChangeToAbsPath(path);
    if (watch != 0) {
        p_zookeeper_context->m_watch_path = abs_path;
        p_zookeeper_context->m_global_watcher_add_type = WATCHER_EXISTS;
    }

    ret = zoo_aexists(m_zhandle, abs_path.c_str(), watch,
                      &ZookeeperManager::InnerStatCompletion, p_zookeeper_context);
    if (ret != ZOK) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
        delete p_zookeeper_context;
    }

    return ret;
}

int32_t ZookeeperManager::AExists(const string &path,
                                  shared_ptr<StatCompletionFuncType> stat_completion_fun,
                                  shared_ptr<WatcherFuncType> watcher_fun) {
    int32_t ret = ZOK;
    ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
    p_zookeeper_context->m_stat_completion_func = stat_completion_fun;

    shared_ptr<ZookeeperCtx> p_zookeeper_watcher_context = make_shared<ZookeeperCtx>(*this, ZookeeperCtx::EXISTS);
    p_zookeeper_watcher_context->m_watcher_func = watcher_fun;

    string abs_path = ChangeToAbsPath(path);
    p_zookeeper_context->m_watch_path = abs_path;
    p_zookeeper_context->m_custom_watcher_context = p_zookeeper_watcher_context;

    ret = zoo_awexists(m_zhandle, abs_path.c_str(),
                       &ZookeeperManager::InnerWatcherCbFunc,
                       p_zookeeper_watcher_context.get(),
                       &ZookeeperManager::InnerStatCompletion, p_zookeeper_context);

    if (ret != ZOK) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
        delete p_zookeeper_context;
    }

    return ret;
}

int32_t ZookeeperManager::Exists(const string &path, Stat *stat, int watch /*= 0*/) {
    string abs_path = ChangeToAbsPath(path);
    int32_t ret = zoo_exists(m_zhandle, abs_path.c_str(), watch, stat);
    if (ret == ZOK || ret == ZNONODE) {
        if (watch != 0) {
            if ((m_global_watcher_path_type[abs_path] & WATCHER_EXISTS) != WATCHER_EXISTS) {
                std::unique_lock<std::mutex> lock(m_global_watcher_path_type_mutex);
                m_global_watcher_path_type[abs_path] |= WATCHER_EXISTS;
            }
        }
    } else {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
    }

    return ret;
}

int32_t ZookeeperManager::Exists(const string &path, Stat *stat, shared_ptr<WatcherFuncType> watcher_fun) {
    int32_t ret = ZOK;
    string abs_path = ChangeToAbsPath(path);
    shared_ptr<ZookeeperCtx> p_zookeeper_watcher_context =
            GetCustomWatcherCtx(abs_path, ZookeeperCtx::EXISTS, watcher_fun);
    ret = zoo_wexists(m_zhandle, abs_path.c_str(),
                      &ZookeeperManager::InnerWatcherCbFunc, p_zookeeper_watcher_context.get(), stat);

    if ((ret != ZOK && ret != ZNONODE) || watcher_fun == nullptr || watcher_fun.get() == nullptr) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret)
                    << "], try to delete custom watcher(context), watcher_fun is nullptr: " << (watcher_fun == nullptr)
                    << ", watcher_fun.get() is nullptr: " << (watcher_fun.get() == nullptr);
        DelCustomWatcherCtx(abs_path, p_zookeeper_watcher_context.get());
    }

    return ret;
}

int32_t ZookeeperManager::AGet(const string &path,
                               shared_ptr<DataCompletionFuncType> data_completion_fun, int watch /*= 0*/) {
    int32_t ret = ZOK;
    ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
    p_zookeeper_context->m_data_completion_func = data_completion_fun;

    string abs_path = ChangeToAbsPath(path);
    if (watch != 0) {
        p_zookeeper_context->m_watch_path = abs_path;
        p_zookeeper_context->m_global_watcher_add_type = WATCHER_GET;
    }

    ret = zoo_aget(m_zhandle, abs_path.c_str(), watch,
                   &ZookeeperManager::InnerDataCompletion, p_zookeeper_context);

    if (ret != ZOK) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
        delete p_zookeeper_context;
    }

    return ret;
}

int32_t ZookeeperManager::AGet(const string &path,
                               shared_ptr<DataCompletionFuncType> data_completion_fun,
                               shared_ptr<WatcherFuncType> watcher_fun) {
    int32_t ret = ZOK;
    ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
    p_zookeeper_context->m_data_completion_func = data_completion_fun;

    shared_ptr<ZookeeperCtx> p_zookeeper_watcher_context = make_shared<ZookeeperCtx>(*this, ZookeeperCtx::GET);
    p_zookeeper_watcher_context->m_watcher_func = watcher_fun;

    string abs_path = ChangeToAbsPath(path);
    p_zookeeper_context->m_watch_path = abs_path;
    p_zookeeper_context->m_custom_watcher_context = p_zookeeper_watcher_context;

    ret = zoo_awget(m_zhandle, abs_path.c_str(), &ZookeeperManager::InnerWatcherCbFunc,
                    p_zookeeper_watcher_context.get(), &ZookeeperManager::InnerDataCompletion, p_zookeeper_context);
    if (ret != ZOK) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
        delete p_zookeeper_context;
    }

    return ret;
}

int32_t ZookeeperManager::Get(const string &path, char *buffer, int *buflen, Stat *stat, int watch /*= 0*/) {
    string abs_path = ChangeToAbsPath(path);
    int32_t ret = zoo_get(m_zhandle, abs_path.c_str(), watch, buffer, buflen, stat);
    if (ret != ZOK) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
    } else if (watch != 0) {
        if ((m_global_watcher_path_type[abs_path] & WATCHER_GET) != WATCHER_GET) {
            std::unique_lock<std::mutex> lock(m_global_watcher_path_type_mutex);
            m_global_watcher_path_type[abs_path] |= WATCHER_GET;
        }
    } else {
        // Noting
    }

    return ret;
}

int32_t ZookeeperManager::Get(const string &path, char *buffer,
                              int *buflen, Stat *stat, shared_ptr<WatcherFuncType> watcher_fun) {
    int32_t ret = ZOK;
    string abs_path = ChangeToAbsPath(path);
    shared_ptr<ZookeeperCtx> p_zookeeper_watcher_context = GetCustomWatcherCtx(abs_path, ZookeeperCtx::GET,
                                                                               watcher_fun);

    ret = zoo_wget(m_zhandle, abs_path.c_str(), &ZookeeperManager::InnerWatcherCbFunc,
                   p_zookeeper_watcher_context.get(), buffer, buflen, stat);

    if (ret != ZOK || watcher_fun == nullptr || watcher_fun.get() == nullptr) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret)
                    << "], try to delete custom watcher(context), watcher_fun is nullptr: " << (watcher_fun == nullptr)
                    << ", watcher_fun.get() is nullptr: " << (watcher_fun.get() == nullptr);
        DelCustomWatcherCtx(abs_path, p_zookeeper_watcher_context.get());
    }

    return ret;
}

int32_t ZookeeperManager::AGetChildren(const string &path,
                                       shared_ptr<StringsStatCompletionFuncType> strings_stat_completion_fun,
                                       int watch /*= 0*/, bool need_stat /*= false*/) {
    int32_t ret = ZOK;
    ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
    p_zookeeper_context->m_strings_stat_completion_func = strings_stat_completion_fun;
    string abs_path = ChangeToAbsPath(path);
    if (watch != 0) {
        p_zookeeper_context->m_watch_path = abs_path;
        p_zookeeper_context->m_global_watcher_add_type = WATCHER_GET_CHILDREN;
    }

    if (need_stat) {
        ret = zoo_aget_children2(m_zhandle, abs_path.c_str(), watch,
                                 &ZookeeperManager::InnerStringsStatCompletion, p_zookeeper_context);

    } else {
        ret = zoo_aget_children(m_zhandle, abs_path.c_str(), watch,
                                &ZookeeperManager::InnerStringsCompletion, p_zookeeper_context);
    }

    if (ret != ZOK) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
        delete p_zookeeper_context;
    }

    return ret;
}

int32_t ZookeeperManager::AGetChildren(const string &path,
                                       shared_ptr<StringsStatCompletionFuncType> strings_stat_completion_fun,
                                       shared_ptr<WatcherFuncType> watcher_fun, bool need_stat /*= false*/) {
    // 此处需要创建2个context
    int32_t ret = ZOK;

    ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
    p_zookeeper_context->m_strings_stat_completion_func = strings_stat_completion_fun;

    shared_ptr<ZookeeperCtx> p_zookeeper_watcher_context =
            make_shared<ZookeeperCtx>(*this, ZookeeperCtx::GET_CHILDREN);
    p_zookeeper_watcher_context->m_watcher_func = watcher_fun;

    string abs_path = ChangeToAbsPath(path);
    p_zookeeper_context->m_watch_path = abs_path;
    p_zookeeper_context->m_custom_watcher_context = p_zookeeper_watcher_context;

    if (need_stat) {
        ret = zoo_awget_children2(m_zhandle, abs_path.c_str(),
                                  &ZookeeperManager::InnerWatcherCbFunc, p_zookeeper_watcher_context.get(),
                                  &ZookeeperManager::InnerStringsStatCompletion, p_zookeeper_context);
    } else {
        ret = zoo_awget_children(m_zhandle, abs_path.c_str(),
                                 &ZookeeperManager::InnerWatcherCbFunc, p_zookeeper_watcher_context.get(),
                                 &ZookeeperManager::InnerStringsCompletion, p_zookeeper_context);
    }

    if (ret != ZOK) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
        delete p_zookeeper_context;
    }

    return ret;
}

int32_t ZookeeperManager::GetChildren(const string &path, ScopedStringVector &strings,
                                      int watch /*= 0*/, Stat *stat /*= NULL*/) {
    // 这里要Clear掉它, 避免内部还有数据时导致内存泄露
    strings.Clear();
    string abs_path = ChangeToAbsPath(path);
    int32_t ret = ZOK;
    if (stat == NULL) {
        ret = zoo_get_children(m_zhandle, abs_path.c_str(), watch, &strings);
    } else {
        ret = zoo_get_children2(m_zhandle, abs_path.c_str(), watch, &strings, stat);
    }

    if (ret != ZOK) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
    } else if (watch != 0) {
        if ((m_global_watcher_path_type[abs_path] & WATCHER_GET_CHILDREN) != WATCHER_GET_CHILDREN) {
            std::unique_lock<std::mutex> lock(m_global_watcher_path_type_mutex);
            m_global_watcher_path_type[abs_path] |= WATCHER_GET_CHILDREN;
        }
    } else {
        // Nothing
    }

    return ret;
}

int32_t ZookeeperManager::GetChildren(const string &path, ScopedStringVector &strings,
                                      shared_ptr<WatcherFuncType> watcher_fun, Stat *stat /*= NULL*/) {
    strings.Clear();
    int32_t ret = ZOK;
    string abs_path = ChangeToAbsPath(path);
    shared_ptr<ZookeeperCtx> p_zookeeper_watcher_context =
            GetCustomWatcherCtx(abs_path, ZookeeperCtx::GET_CHILDREN, watcher_fun);

    if (stat == NULL) {
        ret = zoo_wget_children(m_zhandle, abs_path.c_str(),
                                &ZookeeperManager::InnerWatcherCbFunc, p_zookeeper_watcher_context.get(), &strings);
    } else {
        ret = zoo_wget_children2(m_zhandle, abs_path.c_str(),
                                 &ZookeeperManager::InnerWatcherCbFunc,
                                 p_zookeeper_watcher_context.get(), &strings, stat);
    }

    if (ret != ZOK || watcher_fun == nullptr || watcher_fun.get() == nullptr) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret)
                    << "], try to delete custom watcher(context), watcher_fun is nullptr: " << (watcher_fun == nullptr)
                    << ", watcher_fun.get() is nullptr: " << (watcher_fun.get() == nullptr);
        DelCustomWatcherCtx(abs_path, p_zookeeper_watcher_context.get());
    }

    return ret;
}

int32_t ZookeeperManager::ACreate(const string &path, const char *value, int valuelen,
                                  shared_ptr<StringCompletionFuncType> string_completion_fun,
                                  const ACL_vector *acl /*= &ZOO_OPEN_ACL_UNSAFE*/, int flags /*= 0*/) {
    int32_t ret = ZOK;
    ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
    string abs_path = ChangeToAbsPath(path);

    p_zookeeper_context->m_string_completion_func = string_completion_fun;
    if (flags & ZOO_EPHEMERAL) {
        p_zookeeper_context->m_ephemeral_path = abs_path;
        p_zookeeper_context->m_ephemeral_info = make_shared<EphemeralNodeInfo>();
        p_zookeeper_context->m_ephemeral_info->Acl = *acl;
        p_zookeeper_context->m_ephemeral_info->Data.assign(value, valuelen);
        p_zookeeper_context->m_ephemeral_info->Flags = flags;
    }

    ret = zoo_acreate(m_zhandle, abs_path.c_str(), value, valuelen, acl, flags,
                      &ZookeeperManager::InnerStringCompletion, p_zookeeper_context);

    if (ret != ZOK) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
        delete p_zookeeper_context;
    }

    return ret;
}

int32_t ZookeeperManager::ACreate(const string &path, const string &value,
                                  shared_ptr<StringCompletionFuncType> string_completion_fun,
                                  const ACL_vector *acl /*= &ZOO_OPEN_ACL_UNSAFE*/, int flags /*= 0*/) {
    return ACreate(path, value.data(), value.size(), string_completion_fun, acl, flags);
}

int32_t ZookeeperManager::Create(const string &path, const char *value,
                                 int valuelen, string *p_real_path /*= NULL*/,
                                 const ACL_vector *acl /*= &ZOO_OPEN_ACL_UNSAFE*/,
                                 int flags /*= 0*/, bool ephemeral_exist_skip /*= false*/) {
    string abs_path = ChangeToAbsPath(path);
    int32_t ret;
    string exist_value;             // 节点存在的话, 保存其Value
    string exist_path;              // 节点存在的话, 保存其路径, 不为空, 表示节点存在
    Stat exist_stat;                // 节点存在的话, 保存其stat
    bzero(&exist_stat, sizeof(exist_stat));

    // 重连恢复API临时节点状态步骤
    if (ephemeral_exist_skip && (flags & ZOO_EPHEMERAL)) {
        exist_value.resize(valuelen);
        if (flags & ZOO_SEQUENCE) {
            // 如果是序列节点, 获得当前父节点所有的子节点, 判断有没有正则表达式为"[节点名]\w{10}"的节点
            // 有的话, 获得他们的owner信息, 如果找到了, 则将此节点加入到m_ephemeral_node_info中
            // 这个只能适用于一个同名节点的情况, 如果有超过1个以上的同名节点, 则不支持, 目前也没有这样的需求, 
            // 比如创建2个名为node, flag为ZOO_EPHEMERAL|ZOO_SEQUENCE的节点

            // 获得节点名和父路径
            size_t index = abs_path.rfind('/');
            if (index == string::npos) {
                M_LOG_ERROR << "can not get parent path of: " << abs_path;
                return ZBADARGUMENTS;
            }

            string parent_path = abs_path.substr(0, index);
            string node_name = abs_path.substr(index + 1);

            // 获得所有子节点
            ScopedStringVector children;
            ret = GetChildren(parent_path, children);
            if (ret != ZOK) {
                M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
                return ret;
            }

            static const uint32_t SEQUENCE_LEN = 10;            // 序号长度, 全是数字
            list<string> match_children;                        // 符合条件的children
            for (int32_t ci = 0; ci < children.count; ++ci) {
                char *child = children.GetData(ci);
                uint32_t children_len = strlen(child);

                // 序号节点名长度 = 原节点名长度 + SEQUENCE_LEN, 不符合的跳过
                if (node_name.size() + SEQUENCE_LEN != children_len) {
                    continue;
                }

                // 如果节点名前node_name.size()不一样, 跳过
                if (memcmp(child, node_name.c_str(), node_name.size()) != 0) {
                    continue;
                }

                // 判断children后SEQUENCE_LEN个字符是不是都是数字, 如果不是, 跳过
                uint32_t i = node_name.size();
                for (; i < node_name.size() + SEQUENCE_LEN; ++i) {
                    if (!isdigit(child[i])) {
                        break;
                    }
                }

                if (i == node_name.size() + SEQUENCE_LEN) {
                    // 符合条件
                    match_children.push_back(child);
                }
            }

            // 获得所有符合条件的节点Stat, 判断owner是否是自己
            for (auto child_it = match_children.begin(); child_it != match_children.end(); ++child_it) {
                int buflen = valuelen;
                string child_path = parent_path + "/" + *child_it;
                ret = Get(child_path, const_cast<char *>(exist_value.data()), &buflen, &exist_stat);
                if (ret != ZOK) {
                    M_LOG_ERROR << "child_path[" << child_path
                                << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
                    return ret;
                }

                if (exist_stat.ephemeralOwner == m_zk_client_id.client_id) {
                    // 找到了, 返回
                    exist_path = child_path;
                    break;
                }
            }
        } else {
            // 如果是普通临时节点, 直接Get出Stat判断Owner即可
            char child_buf[1];
            int buflen = sizeof(child_buf);
            ret = Get(path, child_buf, &buflen, &exist_stat);
            if (ret != ZOK) {
                M_LOG_ERROR << "child_path[" << path
                            << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
                return ret;
            }

            if (exist_stat.ephemeralOwner == m_zk_client_id.client_id) {
                exist_path = path;
            }
        }
    }

    if (!exist_path.empty()) {
        if (p_real_path != NULL) {
            strncpy(const_cast<char *>(p_real_path->data()), exist_path.c_str(), p_real_path->size());
        }

        // 已经存在, 处理value, 如果Value不同, 则重新写入
        if (memcmp(exist_value.c_str(), value, valuelen) != 0) {
            ret = Set(exist_path, value, valuelen, exist_stat.version);
            if (ret != ZOK) {
                M_LOG_ERROR << "exist_path[" << exist_path
                            << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
                return ret;
            }
        }

        // TODO(moon)：这里没有判断ACL, 目前没需求, 后面有的话, 要加上判断
    } else {
        ret = zoo_create(m_zhandle, abs_path.c_str(), value, valuelen, acl, flags,
                         p_real_path != NULL ? const_cast<char *>(p_real_path->data()) : NULL,
                         p_real_path != NULL ? p_real_path->size() : 0);

        if (ret != ZOK) {
            M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
            return ret;
        }
    }

    // 调用成功
    if (flags & ZOO_EPHEMERAL) {
        // 如果是临时节点, 添加到临时节点列表中。
        std::unique_lock<std::mutex> phemeral_node_info_lock(m_ephemeral_node_info_lock);
        if (m_ephemeral_node_info.count(abs_path) == 0) {
            m_ephemeral_node_info[abs_path].Acl = *acl;
            m_ephemeral_node_info[abs_path].Data.assign(value, valuelen);
            m_ephemeral_node_info[abs_path].Flags = flags;
            M_LOG_SPCL << "add ephemeral node info: " << abs_path
                       << ", ephemeral node number: " << m_ephemeral_node_info.size();
        }
    }

    return ZOK;
}

int32_t ZookeeperManager::Create(const string &path,
                                 const string &value,
                                 string *p_real_path /*= NULL*/,
                                 const ACL_vector *acl /*= &ZOO_OPEN_ACL_UNSAFE*/,
                                 int flags /*= 0*/, bool ephemeral_exist_skip /*= false*/) {
    return Create(path, value.data(), value.size(), p_real_path, acl, flags, ephemeral_exist_skip);
}

///// 递归创建结点, 即其父结点不存在时创建父结点
int32_t ZookeeperManager::CreateRecursively(const std::string &path,
                                            const std::string &value,
                                            std::string *p_real_path /*= NULL*/,
                                            const ACL_vector *acl /*= &ZOO_OPEN_ACL_UNSAFE*/,
                                            int flags /*= 0*/, bool ephemeral_exist_skip /*= false*/) {

    std::string zkPath = path;
    if (zkPath.front() != '/') zkPath = "/" + zkPath;
    while (!zkPath.empty() && zkPath.back() == '/') zkPath.pop_back();
    if (zkPath.size() > 1) {
        size_t parentNodeEndIndex = zkPath.find_last_of('/');
        if (parentNodeEndIndex != std::string::npos) {
            std::string parentNodePath = zkPath.substr(0, parentNodeEndIndex);
            int ret = CreatePathRecursion(parentNodePath);
            if (ZOK == ret) {
                M_LOG_SPCL << "create parent node ok: path[" << parentNodePath
                           << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
            } else {
                M_LOG_ERROR << "create parent node error: path[" << parentNodePath
                            << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
            }
        }

        int ret = Create(zkPath, value, p_real_path, acl, flags, ephemeral_exist_skip);
        if (ZOK == ret) {
            M_LOG_SPCL << "create node ok: path[" << zkPath
                       << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
        } else if (ZNODEEXISTS == ret) {
            M_LOG_WARN << "create node warning: path[" << zkPath
                       << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
        } else {
            M_LOG_ERROR << "create node error: path[" << zkPath
                        << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
        }

        return ret;
    } else {
        M_LOG_ERROR << "illegal path: " << path;
        return ILLEGAL_PATH;
    }
}

int32_t ZookeeperManager::ASet(const string &path, const char *buffer, int buflen,
                               int version, shared_ptr<StatCompletionFuncType> stat_completion_fun) {
    int32_t ret = ZOK;
    ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
    p_zookeeper_context->m_stat_completion_func = stat_completion_fun;
    string abs_path = ChangeToAbsPath(path);

    {
        std::unique_lock<std::mutex> phemeral_node_info_lock(m_ephemeral_node_info_lock);
        auto ephemeral_it = m_ephemeral_node_info.find(abs_path);
        if (ephemeral_it != m_ephemeral_node_info.end()) {
            p_zookeeper_context->m_ephemeral_path = abs_path;
            p_zookeeper_context->m_ephemeral_info = make_shared<EphemeralNodeInfo>();
            *p_zookeeper_context->m_ephemeral_info = ephemeral_it->second;
            p_zookeeper_context->m_ephemeral_info->Data.assign(buffer, buflen);
        }
    }

    ret = zoo_aset(m_zhandle, abs_path.c_str(), buffer, buflen, version,
                   &ZookeeperManager::InnerStatCompletion, p_zookeeper_context);

    if (ret != ZOK) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
        delete p_zookeeper_context;
    }

    return ret;
}

int32_t ZookeeperManager::ASet(const string &path, const string &buffer, int version,
                               shared_ptr<StatCompletionFuncType> stat_completion_fun) {
    return ASet(path, buffer.data(), buffer.size(), version, stat_completion_fun);
}

int32_t ZookeeperManager::Set(const string &path, const char *buffer, int buflen, int version, Stat *stat /*= NULL*/) {
    int32_t ret = ZOK;
    string abs_path = ChangeToAbsPath(path);
    if (stat == NULL) {
        ret = zoo_set(m_zhandle, abs_path.c_str(), buffer, buflen, version);
    } else {
        ret = zoo_set2(m_zhandle, abs_path.c_str(), buffer, buflen, version, stat);
    }

    if (ret != ZOK) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
    } else {
        // 调用成功
        std::unique_lock<std::mutex> phemeral_node_info_lock(m_ephemeral_node_info_lock);
        auto ephemeral_it = m_ephemeral_node_info.find(abs_path);
        if (ephemeral_it != m_ephemeral_node_info.end()) {
            // 如果在临时节点列表中找到, 修改数据
            ephemeral_it->second.Data.assign(buffer, buflen);
        }
    }

    return ret;
}

int32_t ZookeeperManager::Set(const string &path, const string &buffer, int version, Stat *stat /*= NULL*/) {
    return Set(path, buffer.data(), buffer.size(), version, stat);
}

int32_t ZookeeperManager::ADelete(const string &path, int version,
                                  shared_ptr<VoidCompletionFuncType> void_completion_fun) {
    int32_t ret = ZOK;
    ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
    p_zookeeper_context->m_void_completion_func = void_completion_fun;
    string abs_path = ChangeToAbsPath(path);

    {
        std::unique_lock<std::mutex> phemeral_node_info_lock(m_ephemeral_node_info_lock);
        auto ephemeral_it = m_ephemeral_node_info.find(abs_path);
        if (ephemeral_it != m_ephemeral_node_info.end()) {
            p_zookeeper_context->m_ephemeral_path = abs_path;
        }
    }

    ret = zoo_adelete(m_zhandle, abs_path.c_str(), version,
                      &ZookeeperManager::InnerVoidCompletion, p_zookeeper_context);

    if (ret != ZOK) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
        delete p_zookeeper_context;
    }

    return ret;
}

int32_t ZookeeperManager::Delete(const string &path, int version /*= -1*/) {
    string abs_path = ChangeToAbsPath(path);
    int32_t ret = zoo_delete(m_zhandle, abs_path.c_str(), version);
    if (ret != ZOK) {
        M_LOG_ERROR << "delete failed, abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
    } else {
        // 调用成功
        M_LOG_SPCL << "delete succeed, abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
        std::unique_lock<std::mutex> phemeral_node_info_lock(m_ephemeral_node_info_lock);
        if (m_ephemeral_node_info.find(abs_path) != m_ephemeral_node_info.end()) {
            // 如果在临时节点列表中找到, 删除它
            m_ephemeral_node_info.erase(abs_path);
            M_LOG_SPCL << "delete ephemeral node info: " << abs_path
                       << ", ephemeral node number: " << m_ephemeral_node_info.size();
        }
    }

    return ret;
}

int32_t ZookeeperManager::AGetAcl(const string &path, shared_ptr<AclCompletionFuncType> acl_completion_fun) {
    int32_t ret = ZOK;
    ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
    p_zookeeper_context->m_acl_completion_func = acl_completion_fun;
    string abs_path = ChangeToAbsPath(path);
    ret = zoo_aget_acl(m_zhandle, abs_path.c_str(), &ZookeeperManager::InnerAclCompletion, p_zookeeper_context);

    if (ret != ZOK) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
        delete p_zookeeper_context;
    }

    return ret;
}

int32_t ZookeeperManager::GetAcl(const string &path, ScopedAclVector &acl, Stat *stat) {
    acl.Clear();
    string abs_path = ChangeToAbsPath(path);
    int32_t ret = zoo_get_acl(m_zhandle, abs_path.c_str(), &acl, stat);
    if (ret != ZOK) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
    }

    return ret;
}

int32_t ZookeeperManager::ASetAcl(const string &path, int version, ACL_vector *acl,
                                  shared_ptr<VoidCompletionFuncType> void_completion_fun) {
    int32_t ret = ZOK;
    ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
    p_zookeeper_context->m_void_completion_func = void_completion_fun;
    string abs_path = ChangeToAbsPath(path);

    {
        std::unique_lock<std::mutex> phemeral_node_info_lock(m_ephemeral_node_info_lock);
        auto ephemeral_it = m_ephemeral_node_info.find(abs_path);
        if (ephemeral_it != m_ephemeral_node_info.end()) {
            p_zookeeper_context->m_ephemeral_path = abs_path;
            p_zookeeper_context->m_ephemeral_info = make_shared<EphemeralNodeInfo>();
            *p_zookeeper_context->m_ephemeral_info = ephemeral_it->second;
            p_zookeeper_context->m_ephemeral_info->Acl = *acl;
        }
    }

    ret = zoo_aset_acl(m_zhandle, abs_path.c_str(), version, acl,
                       &ZookeeperManager::InnerVoidCompletion, p_zookeeper_context);

    if (ret != ZOK) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
        delete p_zookeeper_context;
    }

    return ret;
}

int32_t ZookeeperManager::SetAcl(const string &path, int version, ACL_vector *acl) {
    string abs_path = ChangeToAbsPath(path);
    int32_t ret = zoo_set_acl(m_zhandle, abs_path.c_str(), version, acl);
    if (ret != ZOK) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
    } else {
        // 调用成功
        std::unique_lock<std::mutex> phemeral_node_info_lock(m_ephemeral_node_info_lock);
        auto ephemeral_it = m_ephemeral_node_info.find(abs_path);
        if (ephemeral_it != m_ephemeral_node_info.end()) {
            // 如果在临时节点列表中找到, 修改数据
            ephemeral_it->second.Acl = *acl;
        }
    }

    return ret;
}

MultiOps ZookeeperManager::CreateMultiOps() {
    return MultiOps(this);
}

int32_t ZookeeperManager::AMulti(shared_ptr<MultiOps> &multi_ops,
                                 shared_ptr<MultiCompletionFuncType> multi_completion_fun) {
    if (multi_ops->m_multi_ops.empty()) {
        M_LOG_WARN << "multiple operations count is 0!";
        return ZBADARGUMENTS;
    }

    int32_t ret = ZOK;
    ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
    p_zookeeper_context->m_multi_completion_func = multi_completion_fun;
    p_zookeeper_context->m_multi_ops = multi_ops;
    p_zookeeper_context->m_multi_results.reset(new vector<zoo_op_result_t>());
    p_zookeeper_context->m_multi_results->resize(multi_ops->m_multi_ops.size());
    ret = zoo_amulti(m_zhandle, multi_ops->m_multi_ops.size(), &multi_ops->m_multi_ops[0],
                     &(*p_zookeeper_context->m_multi_results)[0],
                     &ZookeeperManager::InnerMultiCompletion, p_zookeeper_context);

    if (ret != ZOK) {
        M_LOG_ERROR << "multiple operations count[" << multi_ops->m_multi_ops.size()
                    << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
        delete p_zookeeper_context;
    }

    return ret;
}

int32_t ZookeeperManager::Multi(MultiOps &multi_ops, vector<zoo_op_result_t> &results) {
    // 为了保证没有之前使用的脏数据, 这里必须clear掉
    results.clear();
    results.resize(multi_ops.m_multi_ops.size());

    // TODO(moon)：官方API中如果操作数量为0怎么办？
    if (multi_ops.m_multi_ops.empty()) {
        M_LOG_WARN << "multiple operations count is 0!";
        return ZBADARGUMENTS;
    }

    // TODO(moon)：注意包量总大小限制1M
    int32_t ret = zoo_multi(m_zhandle, multi_ops.m_multi_ops.size(), &multi_ops.m_multi_ops[0], &results[0]);
    if (ret != ZOK) {
        M_LOG_ERROR << "multiple operations count[" << multi_ops.m_multi_ops.size()
                    << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
    }

    // 处理临时节点, 这里可能部分成功, 部分失败
    ProcMultiEphemeralNode(multi_ops.m_multi_ops, results);

    return ret;
}

const string ZookeeperManager::ChangeToAbsPath(const string &path) {
    // 为空, 返回根目录
    if (path.empty()) {
        return m_root_path;
    }

    // 本来就是绝对路径, 直接返回
    if (path.at(0) == '/') {
        return path;
    }

    // 相对路径的处理
    if (!m_root_path.empty() && m_root_path.back() == '/') {
        // 如果是绝对根目录, 根目录后不加'/'
        return m_root_path + path;
    }

    return m_root_path + "/" + path;
}

///// 递归创建路径, 即检查不存在的结点, 依次创建不存在的结点
int32_t ZookeeperManager::CreatePathRecursion(const string &path) {
    int32_t ret;

    // 创建节点, 忽略节点已存在的错误
    string abs_path = ChangeToAbsPath(path);
    vector<string> dirs;
    SplitStr(abs_path, "/", dirs);
    string curr_path;

    // 先使用批量check接口逐步判断节点是否存在, 对不存在的节点进行批量创建
    MultiOps multi_check_ops(this);
    for (auto dir_it = dirs.begin(); dir_it != dirs.end(); ++dir_it) {
        curr_path += string("/") + *dir_it;
        multi_check_ops.AddCheckOp(curr_path, -1);
    }

    vector<zoo_op_result_t> results;
    ret = Multi(multi_check_ops, results);
    if (ret == ZNONODE) {
        // 如果某一级节点不存在, 则将此级及以后的节点全部批量创建
        MultiOps multi_create_ops(this);
        bool start_no_node = false;

        auto check_op_it = multi_check_ops.m_multi_ops.begin();
        for (auto result_it = results.begin();
             result_it != results.end() && check_op_it != multi_check_ops.m_multi_ops.end();
             ++result_it, ++check_op_it) {
            // 跳过前面已经存在的节点
            if (result_it->err == ZOK && !start_no_node) {
                continue;
            }

            start_no_node = true;

            multi_create_ops.AddCreateOp(check_op_it->check_op.path, "");
        }

        // 执行批量创建
        ret = Multi(multi_create_ops, results);
        if (ret != ZOK) {
            M_LOG_ERROR << "multiple creation failed, ret[" << ret << "], zerror[" << ZError(ret) << "]";
            return ret;
        }
    }

    return ret;
}

///// 递归删除结点, 即删除这个路径下所有子结点和当前结点
int32_t ZookeeperManager::DeletePathRecursion(const string &path) {
    // 获得路径所有的子节点, 按照顺序存储起来
    list<string> path_to_get_children;          // 需要获得子节点的节点, 预处理节点列表
    list<string> path_to_delete;                // 需要删除的节点, 越往后, 节点越深, 所以需要从后往前删除

    string abs_path = ChangeToAbsPath(path);
    path_to_get_children.push_back(abs_path);       // 将需要删除的根节点塞进去, 以备获得它的子节点

    ScopedStringVector children;
    int32_t ret;
    while (!path_to_get_children.empty()) {
        // 从预处理节点列表后面获得一个节点, 采用深度遍历（栈：后进先出）
        auto curr_path = move(*path_to_get_children.rbegin());
        path_to_get_children.pop_back();
        children.Clear();
        ret = GetChildren(curr_path.c_str(), children);
        if (ret != ZOK && ret != ZNONODE) {
            M_LOG_ERROR << "delete node[" << path
                        << "] recursively, abs_path[" << abs_path
                        << "] get sub node[" << curr_path << "] failed";
        }

        // 节点已经不存在了, 则跳过
        if (ret == ZNONODE) {
            continue;
        }

        for (int32_t i = 0; i < children.count; ++i) {
            path_to_get_children.push_back(curr_path + "/" + children.data[i]);
        }

        M_LOG_SPCL << "add node to delete stack: " << curr_path;
        // 将此节点从预处理节点列表中移动到需要删除的节点列表, 并且将它的所有子节点插入到预处理节点后
        path_to_delete.push_back(move(curr_path));
    }

    // 批量删除, 从删除列表中从后往前添加批量删除操作
    if (!path_to_delete.empty()) {
        MultiOps multi_delete_ops(this);
        for (auto path_it = path_to_delete.rbegin(); path_it != path_to_delete.rend(); ++path_it) {
            multi_delete_ops.AddDeleteOp(*path_it, -1);
        }

        vector<zoo_op_result_t> results;
        ret = Multi(multi_delete_ops, results);
        if (ret != ZOK) {
            M_LOG_ERROR << "delete node[" << path
                        << "] recursively, abs_path[" << abs_path
                        << "] failed, ret[" << ret << "], zerror[" << ZError(ret) << "]";
            return ret;
        } else {
            M_LOG_SPCL << "delete node[" << path
                       << "] recursively, abs_path[" << abs_path
                       << "] succeed, ret[" << ret << "], zerror[" << ZError(ret) << "]";
        }
    }

    // 寻找该路径下所有临时节点信息, 删除, 避免临时节点不在Multi操作中删除, 从而漏删临时节点
    // 测试用例：ZooKeeper.ZkManagerEphemeralNodeTest
    std::unique_lock<std::mutex> phemeral_node_info_lock(m_ephemeral_node_info_lock);

    string pre_path = abs_path + "/";       // 临时节点路径前缀, 注意这里是需要包含"/"的, 否则会误删
    for (auto node_it = m_ephemeral_node_info.begin(); node_it != m_ephemeral_node_info.end();) {
        if (node_it->first.find(pre_path) == 0) {
            m_ephemeral_node_info.erase(node_it++);
            M_LOG_SPCL << "deletion perhaps failed, but still delete ephemeral node info: " << node_it->first
                       << ", ephemeral node number: " << m_ephemeral_node_info.size();
        } else {
            ++node_it;
        }
    }

    // 再尝试删除abs_path自身
    if (m_ephemeral_node_info.count(abs_path) > 0) {
        m_ephemeral_node_info.erase(abs_path);
        M_LOG_SPCL << "deletion perhaps failed, but still delete ephemeral node info: " << abs_path
                   << ", ephemeral node number: " << m_ephemeral_node_info.size();
    }

    return ZOK;
}

int32_t ZookeeperManager::GetChildrenValue(const string &path, map<string, ValueStat> &children_value,
                                           uint32_t max_value_size /*= 2048*/) {
    ScopedStringVector children;
    string abs_path = ChangeToAbsPath(path);
    int32_t ret = GetChildren(abs_path, children);
    if (ret != ZOK) {
        M_LOG_ERROR << "GetChildren[" << abs_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
        return ret;
    }

    children_value.clear();
    for (int32_t i = 0; i < children.count; ++i) {
        string child_path = abs_path + "/" + children.data[i];
        auto &value_stat = children_value[children.data[i]];
        value_stat.value.resize(max_value_size);
        int value_len = max_value_size;
        ret = Get(child_path, const_cast<char *>(value_stat.value.data()),
                  &value_len, &value_stat.stat);
        if (ret != ZOK) {
            M_LOG_ERROR << "Get[" << child_path << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
            return ret;
        }

        // resize到实际长度
        value_stat.value.resize(value_len);
    }

    return ZOK;
}

int32_t ZookeeperManager::GetCString(const string &path, string &data, Stat *stat /*= NULL*/, int watch /*= 0*/) {
    int datalen = data.size() - 1;
    int32_t ret = Get(path, const_cast<char *>(data.data()), &datalen, stat, watch);
    if (ret == ZOK && datalen < static_cast<int32_t>(data.size())) {
        data[datalen] = '\0';
    }

    return ret;
}

int32_t ZookeeperManager::GetCString(const string &path, string &data, Stat *stat,
                                     shared_ptr<WatcherFuncType> watcher_fun) {
    int datalen = data.size() - 1;
    int32_t ret = Get(path, const_cast<char *>(data.data()), &datalen, stat, watcher_fun);
    if (ret == ZOK && datalen < static_cast<int32_t>(data.size())) {
        data[datalen] = '\0';
    }

    return ret;
}

std::map<std::string, uint8_t> ZookeeperManager::getGlobalWatcherPathType() {
    std::unique_lock<std::mutex> lock(m_global_watcher_path_type_mutex);
    return m_global_watcher_path_type;
};

std::map<std::string, EphemeralNodeInfo> ZookeeperManager::getEphemeralNodeInfo() {
    std::unique_lock<std::mutex> lock(m_ephemeral_node_info_lock);
    return m_ephemeral_node_info;
};

std::multimap<std::string, std::shared_ptr<ZookeeperCtx>> ZookeeperManager::getCustomWatcherContexts() {
    std::unique_lock<std::mutex> lock(m_custom_watcher_contexts_mutex);
    return m_custom_watcher_contexts;
};

void ZookeeperManager::DeleteWatcher(int type, const char *abs_path, void *p_zookeeper_context) {
    if (GetHandler() == NULL) {
        return;
    }

    // 要处理的Watcher哈希表, 根据不同的type, 有不同的表
    list<hashtable *> hashtables_to_search;

#define ADD_WATCHER_HASHTABLE(watchers) if ((watchers) != NULL && (watchers)->ht != NULL)hashtables_to_search.push_back((watchers)->ht)

    if (type == ZOO_CREATED_EVENT || type == ZOO_CHANGED_EVENT) {
        ADD_WATCHER_HASHTABLE(GetHandler()->active_node_watchers);
        ADD_WATCHER_HASHTABLE(GetHandler()->active_exist_watchers);
    } else if (type == ZOO_CHILD_EVENT) {
        ADD_WATCHER_HASHTABLE(GetHandler()->active_child_watchers);
    } else if (type == ZOO_DELETED_EVENT) {
        ADD_WATCHER_HASHTABLE(GetHandler()->active_node_watchers);
        ADD_WATCHER_HASHTABLE(GetHandler()->active_exist_watchers);
        ADD_WATCHER_HASHTABLE(GetHandler()->active_child_watchers);
    } else {
        // 无操作
    }

#undef ADD_WATCHER_HASHTABLE

    list<void *> to_free;
    for (auto hashtable_it = hashtables_to_search.begin(); hashtable_it != hashtables_to_search.end(); ++hashtable_it) {
        watcher_object_list_t *wl = (watcher_object_list_t *) hashtable_search(*hashtable_it, (void *) abs_path);
        if (wl == NULL) {
            continue;
        }

        // 删除指定context的Watcher
        watcher_object_t *p_watcher = wl->head;
        watcher_object_t *p_last = p_watcher;
        while (p_watcher != NULL) {
            // 要删除
            if (p_watcher->context == p_zookeeper_context) {
                if (p_watcher == wl->head) {
                    // 头结点
                    wl->head = p_watcher->next;
                } else {
                    p_last->next = p_watcher->next;
                }

                to_free.push_back(p_watcher);
            } else {
                // 不删除
                p_last = p_watcher;
            }

            p_watcher = p_watcher->next;
        }
    }

    for (auto free_it = to_free.begin(); free_it != to_free.end(); ++free_it) {
        free(*free_it);
    }
}

void ZookeeperManager::InnerWatcherCbFunc(zhandle_t *zh, int type, int state,
                                          const char *abs_path, void *p_zookeeper_context) {
    std::string absPath;
    if (abs_path) { absPath = std::string(abs_path); }

    // todo print type string and state string
    M_LOG_DEBUG << "call InnerWatcherCbFunc, type[" << type << "], state[" << state << "], abs_path[" << absPath << "]";

    // p_zookeeper_context must not be a dangling pointer
    ZookeeperCtx *p_context =
            const_cast<ZookeeperCtx *>(reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
    if (!IsValidWatcherContext(p_context)) {
        M_LOG_ERROR << "invalid p_zookeeper_context, "
                    << "type[" << type << "], state[" << state << "], abs_path[" << absPath << "], skip";
        return;
    }

    ZookeeperManager &manager = p_context->m_zookeeper_manager;
    if (manager.m_zk_tid == 0) { manager.m_zk_tid = syscall(__NR_gettid); }
    if (!IsValidManager(zh, manager)) {
        M_LOG_ERROR << "invalid p_context->m_zookeeper_manager, "
                    << "type[" << type << "], state[" << state
                    << "], abs_path[" << absPath << "], client_id["
                    << manager.GetClientID()->client_id << "], skip";
        return;
    }

    if (!IsValidEvent(p_context, absPath, type)) {
        M_LOG_ERROR << "invalid event, watcher type[" << GetWatcherType(p_context, absPath) << "], "
                    << "type[" << type << "], state[" << state
                    << "], abs_path[" << absPath << "], client_id["
                    << manager.GetClientID()->client_id << "], skip";
        return;
    }

    if (type == ZOO_SESSION_EVENT) {
        if (p_context->m_watcher_type == ZookeeperCtx::GLOBAL) {
            M_LOG_SPCL << "trigger watcher type[" << p_context->m_watcher_type << "], "
                       << "type[" << type << "], state[" << state
                       << "], abs_path[" << absPath << "], client_id["
                       << manager.GetClientID()->client_id << "], continue";
        } else { // so user custom watcher will not be triggered by ZOO_SESSION_EVENT
            M_LOG_WARN << "trigger watcher type[" << p_context->m_watcher_type << "], "
                       << "type[" << type << "], state[" << state
                       << "], abs_path[" << absPath << "], client_id["
                       << manager.GetClientID()->client_id << "], skip";
            return;
        }

        HandleSessionEvent(manager, state);
        // 重连之后, 直接返回, 因为上次连接的相关的各种句柄已经失效
        // if callWatcherFuncOnResume is true, all user watcher function will be called when resume success
        if (state == ZOO_EXPIRED_SESSION_STATE) return;
    }

    if (!IsValidCallBack(p_context->m_watcher_func)) {
        M_LOG_WARN << "invalid p_context->m_watcher_func, "
                   << "type[" << type << "], state[" << state
                   << "], abs_path[" << absPath << "], client_id["
                   << manager.GetClientID()->client_id << "], skip";
        return;
    }

    if (!p_context->m_auto_reg_watcher || abs_path == NULL || *abs_path == '\0') {
        static_cast<void>((*p_context->m_watcher_func)(manager, type, state, abs_path));
        return;
    }

    // 调用用户的Watcher
    // 删除指定节点的Watcher, 回调返回true或者之前流程将p_context->m_is_stop置为true表示要删除这个Watcher
    // m_is_stop is not used, user can return true in m_watcher_func, so this watcher will be removed
//    if (p_context->m_is_stop || (*p_context->m_watcher_func)(manager, type, state, abs_path)) {
    if ((*p_context->m_watcher_func)(manager, type, state, abs_path)) {
        if (p_context->m_watcher_type == ZookeeperCtx::GLOBAL) {
/////       删除指定路径的当前全局Watcher
            std::unique_lock<std::mutex> lock(manager.m_global_watcher_path_type_mutex);
            auto it = manager.m_global_watcher_path_type.find(absPath);
            if (it != manager.m_global_watcher_path_type.end()) {
                uint8_t stop_watcher_type_mask = GetStopWatcherTypeMask(p_context, absPath, type);
                if (stop_watcher_type_mask != UINT8_MAX) {
                    it->second &= stop_watcher_type_mask;
                    if (it->second == 0) {
                        M_LOG_WARN << "delete global watcher(type), "
                                   << "type[" << type << "], state[" << state
                                   << "], abs_path[" << absPath << "], client_id["
                                   << manager.GetClientID()->client_id << "]";;
                        manager.m_global_watcher_path_type.erase(it);
                    }
                }
            }
        } else {
            // 删除指定节点的自定义Watcher
//            p_context->m_is_stop = true;
/////            删除custom context之后, 到删除c api watcher之前, 这个watcher对应的事件可能还会触发, 
/////            调用到InnerWatcherCbFunc后, 如果p_context已被释放, 会有出现内存访问错误
/////            所以只在设置custom watcher invalid, 并且删除c api watcher, 但不将custom context删除
/////            M_LOG_WARN << "try to delete custom watcher(context), path[" << absPath << "]";
/////            manager.DelCustomWatcherCtx(absPath, p_context);
            M_LOG_WARN << "set custom watcher(context) invalid, path[" << absPath
                       << "], origin watcher type[" << p_context->m_watcher_type << "], "
                       << "type[" << type << "], state[" << state
                       << "], abs_path[" << absPath << "], client_id["
                       << manager.GetClientID()->client_id << "]";
            SetWatcherContextInvalid(p_context);

            // 删除上面的流程重注册的信息, 这里如果不删除的话, 可能还会触发回调, 但是context已经在上面被释放了
            // 虽然有多重机制保障不会出问题, 但是如果原始地址仍然是一个context, 还是会出问题。
            // 还可能会出现更严重的内存访问错误的问题。
            // 这里是删除原生API中回调的Watcher
            manager.DeleteWatcher(type, abs_path, p_context);
            //destroy_watcher_object_list(collectWatchers(manager.m_zhandle, type, const_cast<char *>(abs_path)));
        }
    } else {
        if (p_context->m_watcher_type == ZookeeperCtx::GLOBAL) {
            if (NO_TYPE_IN_PATH == GlobalWatcherAutoRegisterer(p_context, abs_path, type)) {
                M_LOG_ERROR << "no global watcher type[" << type << "] of path[ " << absPath << "], "
                            << "state[" << state << "], client_id["
                            << manager.GetClientID()->client_id << "], skip";
            }
        } else { CustomWatcherAutoRegisterer(p_context, abs_path, type); }
    }
}

void ZookeeperManager::HandleSessionEvent(ZookeeperManager &manager, int state) {
    if (state == ZOO_CONNECTED_STATE) {
        // 连接建立事件
        M_LOG_SPCL << "trigger connected event, if reconnected, need to resume environment, client_id: "
                   << manager.GetClientID()->client_id;
//            if (manager.m_need_resume_env) { manager.ResumeEnv(); }
        manager.m_connect_cond.notify_all();
    } else if (state == ZOO_EXPIRED_SESSION_STATE) {
        // 超时事件, 重新连接, 直到成功
        M_LOG_SPCL << "trigger session expired event, need to reconnect, client_id: "
                   << manager.GetClientID()->client_id;
        std::thread reconnectExecutor = std::thread(&ZookeeperManager::TimeoutReconnect, std::ref(manager));
        reconnectExecutor.detach();
        // 重连之后, 直接返回, 因为上次连接的相关的各种句柄已经失效
        // if callWatcherFuncOnResume is true, all user watcher function will be called when resume success
    } else {
        M_LOG_WARN << "trigger session event, current state: " << state
                   << ", client_id: " << manager.GetClientID()->client_id;
    }
}

void ZookeeperManager::TimeoutReconnect(ZookeeperManager &manager) {
    bool expectedV = false;
    if (!manager.reconnecting.compare_exchange_strong(expectedV, true)) {
        M_LOG_ERROR << "call reconnect but reconnecting is true!";
        return;
    }
    if (manager.reconnecting != true) {
        M_LOG_ERROR << "do reconnect but reconnecting is false";
        return;
    }
    int ret = ZOK;
    int period = 1;
    for (int reconnectCount = 1; reconnectCount <= manager.reconnectMaxCount; ++reconnectCount) {
        ret = manager.Reconnect();
        if (reconnectCount >= manager.reconnectAlertCount && manager.reconnectAlertNotifier != NULL) {
            manager.reconnectAlertNotifier();
        }
        if (ret != ZOK) {
            M_LOG_WARN << "failed on " << reconnectCount << " reconnection, ret[" << ret
                       << "], zerror[" << ZError(ret) << "], will retry after " << period << "s";
            sleep(period);
            if (period < 30) ++period;
            continue;
        }
        // 重连之后, 直接返回, 因为上次连接的相关的各种句柄已经失效
        M_LOG_SPCL << "succeeded on " << reconnectCount << " reconnection, now start to resume environment...";
        if (!manager.ResumeEnv()) {
            M_LOG_ERROR << "resume environment faild!";
            continue;
        }
        manager.reconnecting = false;
        return;
    }
}

bool ZookeeperManager::IsValidCallBack(std::shared_ptr<WatcherFuncType> func) {
    if (func == NULL || (*func) == NULL) {
        M_LOG_WARN << "empty callback function!";
        return false;
    }
    return true;
}

bool ZookeeperManager::IsValidWatcherContext(ZookeeperCtx *p_context) {
    if (p_context == NULL) {
        M_LOG_ERROR << "context is NULL!";
        return false;
    }
    if (p_context->m_watcher_type == ZookeeperCtx::NOT_WATCH) {
        M_LOG_ERROR << "NOT_WATCH watcher, will not call user callback function!";
        return false;
    }
    if (p_context->m_is_stop) {
        M_LOG_WARN << "stopped watcher, will not call user callback function!";
        return false;
    }
    return true;
}

void ZookeeperManager::SetWatcherContextInvalid(ZookeeperCtx *p_context) {
    if (IsValidWatcherContext(p_context)) {
        p_context->m_is_stop = true;
        M_LOG_WARN << "stop custom watcher, origin watcher type: " << p_context->m_watcher_type;
    } else { M_LOG_ERROR << "can not set invalid context"; }
}

bool ZookeeperManager::IsValidEvent(ZookeeperCtx *p_context, const std::string &absPath, int type) {
    if (p_context->m_is_stop) {
        M_LOG_ERROR << "stopped watcher of path: " << absPath << ", watcher_type[" << p_context->m_watcher_type << "]";
        return false;
    }
    uint32_t watcherType = GetWatcherType(p_context, absPath);
    if (watcherType == DEFAULT_TYPE_IN_PATH) {
        M_LOG_DEBUG << "default global watcher type";
        return true;
    } else if (watcherType == NO_TYPE_IN_PATH) {
        M_LOG_ERROR << "no global watcher type of path: " << absPath;
        return false;
    } else if (watcherType == ZookeeperCtx::GLOBAL) { ///// todo 改成一套type
        if (type == ZOO_CHANGED_EVENT) {
            return (((watcherType & WATCHER_GET) == WATCHER_GET) ||
                    ((watcherType & WATCHER_EXISTS) == WATCHER_EXISTS));
        } else if (type == ZOO_CREATED_EVENT || type == ZOO_DELETED_EVENT) {
            return (((watcherType & WATCHER_GET) == WATCHER_GET) ||
                    ((watcherType & WATCHER_EXISTS) == WATCHER_EXISTS) ||
                    ((watcherType & WATCHER_GET_CHILDREN) == WATCHER_GET_CHILDREN));
        } else if (type == ZOO_CHILD_EVENT) {
            return ((watcherType & WATCHER_GET_CHILDREN) == WATCHER_GET_CHILDREN);
        } else {
            M_LOG_DEBUG << "return true when handle other event in global watcher";
            return true;
        }
    } else if (watcherType == ZookeeperCtx::NOT_WATCH) {
        M_LOG_ERROR << "NOT_WATCH watcher of path: " << absPath;
        return false;
    } else {
        if (type == ZOO_CHANGED_EVENT) {
            return ((watcherType == ZookeeperCtx::GET) ||
                    (watcherType == ZookeeperCtx::EXISTS));
        } else if (type == ZOO_CREATED_EVENT || type == ZOO_DELETED_EVENT) {
            return ((watcherType == ZookeeperCtx::GET) ||
                    (watcherType == ZookeeperCtx::EXISTS) ||
                    (watcherType == ZookeeperCtx::GET_CHILDREN));
        } else if (type == ZOO_CHILD_EVENT) {
            return (watcherType == ZookeeperCtx::GET_CHILDREN);
        } else {
            M_LOG_DEBUG << "return false when handle other event in custom watcher";
            return false;
        }
    }
}

uint32_t ZookeeperManager::GetWatcherType(ZookeeperCtx *p_context, const std::string &absPath) {
    if (p_context->m_watcher_type != ZookeeperCtx::GLOBAL) { return p_context->m_watcher_type; }
    if (absPath.empty()) { return DEFAULT_TYPE_IN_PATH; }
    ZookeeperManager &manager = p_context->m_zookeeper_manager;
    std::unique_lock<std::mutex> lock(manager.m_global_watcher_path_type_mutex);
    auto it = manager.m_global_watcher_path_type.find(absPath);
    if (it == manager.m_global_watcher_path_type.end()) {
        M_LOG_ERROR << "can not find global watcher type of path: " << absPath;
        return NO_TYPE_IN_PATH;
    }
    return it->second;
}

bool ZookeeperManager::IsValidManager(zhandle_t *zh, ZookeeperManager &manager) {
    if (zh != manager.m_zhandle) {
        M_LOG_ERROR << "current zhandle[" << zh << "] different from local m_zhandle!";
        return false;
    }
    return true;
}

int ZookeeperManager::CustomWatcherAutoRegisterer(ZookeeperCtx *p_context, const char *abs_path, int type) {
    M_LOG_DEBUG << "debug";
    std::string absPath;
    if (abs_path) { absPath = std::string(abs_path); }
    ZookeeperManager &manager = p_context->m_zookeeper_manager;
    int ret = ZOK;
    if (p_context->m_watcher_type == ZookeeperCtx::GLOBAL) return ret;

    if (p_context->m_watcher_type == ZookeeperCtx::EXISTS) {
        if (type == ZOO_CHANGED_EVENT || type == ZOO_CREATED_EVENT || type == ZOO_DELETED_EVENT) {
            ret = zoo_wexists(manager.GetHandler(), abs_path, &ZookeeperManager::InnerWatcherCbFunc, p_context, NULL);
            if (ret != ZOK && ret != ZNONODE) {
                M_LOG_ERROR << "ret[" << ret << "], zerror[" << ZError(ret) << "], abs_path: " << absPath;
            }
        } else {
            M_LOG_ERROR << "mismatched, event type: " << type << ", custom watcher type: " << p_context->m_watcher_type;
        }
    } else if (p_context->m_watcher_type == ZookeeperCtx::GET) {
        if (type == ZOO_DELETED_EVENT) {
            M_LOG_WARN << "node has been deleted, abs_path: " << absPath;
            ret = AddExistWatcherOnce(p_context, abs_path, false);
        } else if (type == ZOO_CHANGED_EVENT || type == ZOO_CREATED_EVENT) {
            char buf;
            int buflen = 1;
            ret = zoo_wget(manager.GetHandler(), abs_path,
                           &ZookeeperManager::InnerWatcherCbFunc, p_context, &buf, &buflen, NULL);
            if (ret == ZNONODE) {
                M_LOG_WARN << "no node, abs_path: " << absPath;
                ret = AddExistWatcherOnce(p_context, abs_path, false);
            }
        } else {
            M_LOG_ERROR << "mismatched, event type: " << type << ", custom watcher type: " << p_context->m_watcher_type;
        }
    } else if (p_context->m_watcher_type == ZookeeperCtx::GET_CHILDREN) {
        if (type == ZOO_DELETED_EVENT) {
            M_LOG_WARN << "node has been deleted, abs_path: " << absPath;
            ret = AddExistWatcherOnce(p_context, abs_path, false);
        } else if (type == ZOO_CHILD_EVENT || type == ZOO_CREATED_EVENT) {
            // TODO(moon)：看是否需要把children和stat传给watcher, 避免watcher中调用, 看使用量, 这些参数可以统一封装在一个对象里
            ScopedStringVector children;
            ret = zoo_wget_children(manager.GetHandler(), abs_path,
                                    &ZookeeperManager::InnerWatcherCbFunc, p_context, &children);
            if (ret == ZNONODE) {
                M_LOG_WARN << "no node, abs_path: " << absPath;
                ret = AddExistWatcherOnce(p_context, abs_path, false);
            }
        } else {
            M_LOG_ERROR << "mismatched, event type: " << type << ", custom watcher type: " << p_context->m_watcher_type;
        }
    } else {
        M_LOG_ERROR << "invalid custom watcher type: " << p_context->m_watcher_type;
    }

    if (ret != ZOK) { M_LOG_ERROR << "ret[" << ret << "], zerror[" << ZError(ret) << "], abs_path: " << absPath; }

    return ret;
}

int ZookeeperManager::GlobalWatcherAutoRegisterer(ZookeeperCtx *p_context, const char *abs_path, int type) {
    M_LOG_DEBUG << "debug";
    std::string absPath;
    if (abs_path) { absPath = std::string(abs_path); }

    ZookeeperManager &manager = p_context->m_zookeeper_manager;
    int ret = ZOK;
    if (p_context->m_watcher_type != ZookeeperCtx::GLOBAL) return ret;

    uint32_t globalWatcherType = GetWatcherType(p_context, absPath);
    if (globalWatcherType == NO_TYPE_IN_PATH) {
        // 全局事件, 找不到type, 不再调用Watcher, 直接返回
        M_LOG_ERROR << "can not find type in m_global_watcher_path_type, do not call watcher!";
        return NO_TYPE_IN_PATH;
    }

    // Global类型, 表示使用的是默认的Watcher, 通过type和abs_path来判断使用方式与重注册
    if (type == ZOO_DELETED_EVENT) {
        if (((globalWatcherType & WATCHER_GET) == WATCHER_GET) ||
            ((globalWatcherType & WATCHER_GET_CHILDREN) == WATCHER_GET_CHILDREN)) {
            M_LOG_WARN << "node has been deleted, abs_path: " << absPath;
            ret = AddExistWatcherOnce(p_context, abs_path);
        }
    } else if (type == ZOO_CHANGED_EVENT) {
        // 分别使用Exists和Get重注册
        if ((globalWatcherType & WATCHER_EXISTS) == WATCHER_EXISTS) {
            ret = manager.Exists(abs_path, NULL, 1);
            // 节点不存在, 注册Watcher也是OK的
            if (ret == ZNONODE) { ret = ZOK; }
        }
        if ((globalWatcherType & WATCHER_GET) == WATCHER_GET) {
            char buf;
            int buflen = 1;
            ret = manager.Get(abs_path, &buf, &buflen, NULL, 1);
            if (ret == ZNONODE) {
                M_LOG_WARN << "no node, abs_path: " << absPath;
                ret = AddExistWatcherOnce(p_context, abs_path);
            }
        }
    } else if (type == ZOO_CREATED_EVENT) {
        // 如果是节点创建, 使用Exists重注册
        if ((globalWatcherType & WATCHER_EXISTS) == WATCHER_EXISTS) {
            ret = manager.Exists(abs_path, NULL, 1);
            // 节点不存在, 注册Watcher也是OK的
            if (ret == ZNONODE) { ret = ZOK; }
        }
    } else if (type == ZOO_CHILD_EVENT) {
        if ((globalWatcherType & WATCHER_GET_CHILDREN) == WATCHER_GET_CHILDREN) {
            // 如果是子节点变更, 使用GetChildren重注册
            ScopedStringVector children;
            ret = manager.GetChildren(abs_path, children, 1);
            if (ret == ZNONODE) {
                M_LOG_WARN << "no node, abs_path: " << absPath;
                ret = AddExistWatcherOnce(p_context, abs_path);
            }
        }
    } else {
        // 无操作, 直接透传给全局Watcher callback func
    }

    if (ret != ZOK) { M_LOG_ERROR << "ret[" << ret << "], zerror[" << ZError(ret) << "], abs_path: " << absPath; }

    return ret;
}

int ZookeeperManager::AddExistWatcherOnce(ZookeeperCtx *p_context, const char *abs_path, bool addToGlobalWatcher) {
    std::string absPath;
    if (abs_path) { absPath = std::string(abs_path); }
    ZookeeperManager &manager = p_context->m_zookeeper_manager;
    int ret;
    if (addToGlobalWatcher) {
        ret = zoo_exists(manager.GetHandler(), abs_path, 1, NULL);
        if (ret != ZOK && ret != ZNONODE) {
            M_LOG_ERROR << "can not add EXIST global watcher, ret[" << ret
                        << "], zerror[" << ZError(ret) << "], abs_path: " << absPath;
        } else { M_LOG_WARN << "add EXIST global watcher, abs_path: " << absPath; }
    } else {
        ret = zoo_wexists(manager.GetHandler(), abs_path, &ZookeeperManager::InnerWatcherCbFunc, p_context, NULL);
        if (ret != ZOK && ret != ZNONODE) {
            M_LOG_ERROR << "can not add EXIST custom watcher, ret[" << ret
                        << "], zerror[" << ZError(ret) << "], abs_path: " << absPath;
        } else { M_LOG_WARN << "add EXIST custom watcher, abs_path: " << absPath; }
    }

    return ret;
}

uint8_t ZookeeperManager::GetStopWatcherTypeMask(ZookeeperCtx *p_context, const std::string &absPath, int type) {
    uint8_t stop_watcher_type_mask = UINT8_MAX;
    if (p_context->m_watcher_type != ZookeeperCtx::GLOBAL) return stop_watcher_type_mask;

    ZookeeperManager &manager = p_context->m_zookeeper_manager;

    std::unique_lock<std::mutex> lock(manager.m_global_watcher_path_type_mutex);
    auto it = manager.m_global_watcher_path_type.find(absPath);
    if (it == manager.m_global_watcher_path_type.end()) {
        // 全局事件, 找不到type, 不再调用Watcher, 直接返回
        M_LOG_ERROR << "can not find type in m_global_watcher_path_type, return stop_watcher_type_mask 0!";
        return stop_watcher_type_mask;
    }

    if (type == ZOO_CHANGED_EVENT) {
        if ((it->second & WATCHER_EXISTS) == WATCHER_EXISTS) {
            stop_watcher_type_mask = ~WATCHER_EXISTS;
            M_LOG_WARN << "delete WATCHER_EXISTS watcher type of path: " << absPath;
        }
        if ((it->second & WATCHER_GET) == WATCHER_GET) {
            stop_watcher_type_mask &= ~WATCHER_GET;
            M_LOG_WARN << "delete WATCHER_GET watcher type of path: " << absPath;
        }
    } else if (type == ZOO_CREATED_EVENT) {
        if ((it->second & WATCHER_EXISTS) == WATCHER_EXISTS) {
            stop_watcher_type_mask = ~WATCHER_EXISTS;
            M_LOG_WARN << "delete WATCHER_EXISTS watcher type of path: " << absPath;
        }
    } else if (type == ZOO_CHILD_EVENT) {
        if ((it->second & WATCHER_GET_CHILDREN) == WATCHER_GET_CHILDREN) {
            stop_watcher_type_mask = ~WATCHER_GET_CHILDREN;
            M_LOG_WARN << "delete WATCHER_GET_CHILDREN watcher type of path: " << absPath;
        }
    }
    return stop_watcher_type_mask;
}

void ZookeeperManager::InnerVoidCompletion(int rc, const void *p_zookeeper_context) {
    ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
    if (p_context == NULL) {
        M_LOG_ERROR << "callback function context is NULL!";
        return;
    }

    unique_ptr<ZookeeperCtx> up_context(p_context);

    ZookeeperManager &manager = up_context->m_zookeeper_manager;

    if (rc == ZOK && !up_context->m_ephemeral_path.empty()) {
        std::unique_lock<std::mutex> phemeral_node_info_lock(manager.m_ephemeral_node_info_lock);
        if (up_context->m_ephemeral_info == NULL) {
            // 这种情况是删除临时节点
            manager.m_ephemeral_node_info.erase(up_context->m_ephemeral_path);
            M_LOG_SPCL << "delete ephemeral node info: " << up_context->m_ephemeral_path
                       << ", ephemeral node number: " << manager.m_ephemeral_node_info.size();
        } else {
            // 这种情况是修改临时节点信息, 比如ASetAcl中的操作
            manager.m_ephemeral_node_info[up_context->m_ephemeral_path] = *up_context->m_ephemeral_info;
        }
    }

    if (up_context->m_void_completion_func != NULL && *up_context->m_void_completion_func != NULL) {
        (*up_context->m_void_completion_func)(manager, rc);
    }
}

void ZookeeperManager::InnerStatCompletion(int rc, const Stat *stat, const void *p_zookeeper_context) {
    ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
    if (p_context == NULL) {
        M_LOG_ERROR << "callback function context is NULL!";
        return;
    }

    unique_ptr<ZookeeperCtx> up_context(p_context);

    ZookeeperManager &manager = up_context->m_zookeeper_manager;

    if (rc == ZOK) {
        // 处理临时节点
        if (up_context->m_ephemeral_info != NULL && !up_context->m_ephemeral_path.empty()) {
            std::unique_lock<std::mutex> phemeral_node_info_lock(manager.m_ephemeral_node_info_lock);
            manager.m_ephemeral_node_info[up_context->m_ephemeral_path] = *up_context->m_ephemeral_info;
        }
    }

    // Exist还要额外考虑ZNONODE返回值
    if (rc == ZOK || (rc == ZNONODE
                      && (up_context->m_global_watcher_add_type == WATCHER_EXISTS
                          || (up_context->m_custom_watcher_context != NULL
                              && up_context->m_custom_watcher_context->m_watcher_type == ZookeeperCtx::EXISTS)))) {
        // 处理Watcher
        manager.ProcAsyncWatcher(*up_context);
    }

    if (up_context->m_stat_completion_func != NULL && *up_context->m_stat_completion_func != NULL) {
        (*up_context->m_stat_completion_func)(manager, rc, stat);
    }
}

void ZookeeperManager::InnerDataCompletion(int rc, const char *value, int value_len,
                                           const Stat *stat, const void *p_zookeeper_context) {
    ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
    if (p_context == NULL) {
        M_LOG_ERROR << "callback function context is NULL!";
        return;
    }

    unique_ptr<ZookeeperCtx> up_context(p_context);

    ZookeeperManager &manager = up_context->m_zookeeper_manager;

    if (rc == ZOK) {
        // 处理Watcher
        manager.ProcAsyncWatcher(*up_context);
    }

    if (up_context->m_data_completion_func != NULL && *up_context->m_data_completion_func != NULL) {
        (*up_context->m_data_completion_func)(manager, rc, value, value_len, stat);
    }
}

void ZookeeperManager::InnerStringsCompletion(int rc, const String_vector *strings,
                                              const void *p_zookeeper_context) {
    ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
    if (p_context == NULL) {
        M_LOG_ERROR << "callback function context is NULL!";
        return;
    }

    unique_ptr<ZookeeperCtx> up_context(p_context);

    ZookeeperManager &manager = up_context->m_zookeeper_manager;

    if (rc == ZOK) {
        // 处理Watcher
        manager.ProcAsyncWatcher(*up_context);
    }

    if (up_context->m_strings_stat_completion_func != NULL && *up_context->m_strings_stat_completion_func != NULL) {
        (*up_context->m_strings_stat_completion_func)(manager, rc, strings, NULL);
    }
}

void ZookeeperManager::InnerStringsStatCompletion(int rc, const String_vector *strings,
                                                  const Stat *stat, const void *p_zookeeper_context) {
    ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
    if (p_context == NULL) {
        M_LOG_ERROR << "callback function context is NULL!";
        return;
    }

    unique_ptr<ZookeeperCtx> up_context(p_context);

    ZookeeperManager &manager = up_context->m_zookeeper_manager;

    if (rc == ZOK) {
        // 处理Watcher
        manager.ProcAsyncWatcher(*up_context);
    }

    if (up_context->m_strings_stat_completion_func != NULL && *up_context->m_strings_stat_completion_func != NULL) {
        (*up_context->m_strings_stat_completion_func)(manager, rc, strings, stat);
    }
}

void ZookeeperManager::InnerStringCompletion(int rc, const char *value, const void *p_zookeeper_context) {
    ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
    if (p_context == NULL) {
        M_LOG_ERROR << "callback function context is NULL!";
        return;
    }

    unique_ptr<ZookeeperCtx> up_context(p_context);

    ZookeeperManager &manager = up_context->m_zookeeper_manager;

    // 成功调用, 处理临时节点
    if (rc == ZOK && up_context->m_ephemeral_info != NULL && !up_context->m_ephemeral_path.empty()) {
        std::unique_lock<std::mutex> phemeral_node_info_lock(manager.m_ephemeral_node_info_lock);
        manager.m_ephemeral_node_info[up_context->m_ephemeral_path] = *up_context->m_ephemeral_info;
    }

    if (up_context->m_string_completion_func != NULL && *up_context->m_string_completion_func != NULL) {
        (*up_context->m_string_completion_func)(manager, rc, value);
    }
}

void ZookeeperManager::InnerAclCompletion(int rc, ACL_vector *acl, Stat *stat, const void *p_zookeeper_context) {
    ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
    if (p_context == NULL) {
        M_LOG_ERROR << "callback function context is NULL!";
        return;
    }

    unique_ptr<ZookeeperCtx> up_context(p_context);

    ZookeeperManager &manager = up_context->m_zookeeper_manager;

    if (up_context->m_acl_completion_func != NULL && *up_context->m_acl_completion_func != NULL) {
        (*up_context->m_acl_completion_func)(manager, rc, acl, stat);
    }
}

void ZookeeperManager::InnerMultiCompletion(int rc, const void *p_zookeeper_context) {
    ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
    if (p_context == NULL) {
        M_LOG_ERROR << "callback function context is NULL!";
        return;
    }

    unique_ptr<ZookeeperCtx> up_context(p_context);

    ZookeeperManager &manager = up_context->m_zookeeper_manager;

    // 处理临时节点, 这里可能会出现部分成功部分失败的情况
    manager.ProcMultiEphemeralNode(up_context->m_multi_ops->m_multi_ops, *up_context->m_multi_results);

    if (up_context->m_multi_completion_func != NULL && *up_context->m_multi_completion_func != NULL) {
        (*up_context->m_multi_completion_func)(manager, rc, up_context->m_multi_ops, up_context->m_multi_results);
    }
}

void ZookeeperManager::AddCustomWatcher(const string &abs_path, shared_ptr<ZookeeperCtx> watcher_context) {
    std::unique_lock<std::mutex> custom_watcher_contexts_lock(m_custom_watcher_contexts_mutex);

    m_custom_watcher_contexts.insert(make_pair(abs_path, watcher_context));
    M_LOG_SPCL << "add custom watcher: abs_path[" << abs_path
               << "], watcher type[" << watcher_context->m_watcher_type
               << "], local clientID: " << m_zk_client_id.client_id
               << ", current clientID: " << watcher_context->m_zookeeper_manager.m_zk_client_id.client_id;
}

std::shared_ptr<ZookeeperCtx> ZookeeperManager::GetCustomWatcherCtx(const std::string &abs_path, int type,
                                                                    const std::shared_ptr<WatcherFuncType> &watcher_fun) {
    shared_ptr<ZookeeperCtx> p_zookeeper_watcher_context = nullptr;

    {
        std::unique_lock<std::mutex> custom_watcher_contexts_lock(m_custom_watcher_contexts_mutex);
        auto find_its = m_custom_watcher_contexts.equal_range(abs_path);
        for (auto it = find_its.first; it != find_its.second; ++it) {
            if (it->second->m_watcher_type == type /* && it->second->m_watcher_func == watcher_fun */) {
                p_zookeeper_watcher_context = it->second;
                break;
            }
        }
    }

///// 假设 同一abs_path与watcher type, 只添加一次custom watcher context, 也不会并发添加
    if (p_zookeeper_watcher_context == nullptr) {
        p_zookeeper_watcher_context = make_shared<ZookeeperCtx>(*this, (ZookeeperCtx::WatcherType) type);
        p_zookeeper_watcher_context->m_watcher_func = watcher_fun;
        AddCustomWatcher(abs_path, p_zookeeper_watcher_context);
    } else {
        if (p_zookeeper_watcher_context->m_is_stop) {
            p_zookeeper_watcher_context->m_is_stop = false;
            M_LOG_WARN << "start custom watcher, path: " << abs_path << ", watcher type: " << type;
        }
    }

    return p_zookeeper_watcher_context;
}

void ZookeeperManager::DelCustomWatcherCtx(const string &abs_path, const ZookeeperCtx *watcher_context) {
    std::unique_lock<std::mutex> custom_watcher_contexts_lock(m_custom_watcher_contexts_mutex);
    auto find_its = m_custom_watcher_contexts.equal_range(abs_path);
    for (auto it = find_its.first; it != find_its.second; ++it) {
        if (it->second.get() == watcher_context) {
            // 这里erase之后, it不能再使用, 后面如果要修改, 需要注意
            m_custom_watcher_contexts.erase(it);
            M_LOG_SPCL << "delete custom watcher: abs_path[" << abs_path << "]";
            return;
        }
    }
}

bool ZookeeperManager::ResumeEnv() {
    // 重新注册所有的Watcher
    bool isOk = true;
    if (!ResumeGlobalWatcher()) M_LOG_ERROR << "resume global watcher failed!";
    if (!ResumeCustomWatcher()) M_LOG_ERROR << "resume custom watcher failed!";
    isOk = ResumeEphemeralNode();
    if (!isOk) M_LOG_ERROR << "resume eephemeral node failed!";
    // 重新注册所有的临时节点
    return isOk;

//    m_need_resume_env = false;
}

bool ZookeeperManager::ResumeGlobalWatcher() {
    bool isOk = true;
    int32_t ret = ZOK;
    // 注册全局Watcher
    M_LOG_SPCL << "start re-registering global Watcher...";
    auto m_global_watcher_path_type = getGlobalWatcherPathType();
    for (auto it = m_global_watcher_path_type.begin(); it != m_global_watcher_path_type.end(); ++it) {
        for (int resumeCount = 1; resumeCount <= resumeMaxCount; ++resumeCount) {
            if (resumeCount - 1 > 0) sleep((resumeCount - 1) <= 10 ? (resumeCount - 1) : 10);
            ret = ZOK;

            M_LOG_SPCL << "re-register global Watcher, path[" << it->first
                       << "], type[" << it->second << "], on " << resumeCount << " time";
            if (it->first.empty() || it->first.at(0) != '/') {
                M_LOG_ERROR << "re-register global Watcher, invalid path: " << it->first
                            << "], type[" << it->second << "], on " << resumeCount << " time";
                break;
            }

            if (it->second == 0) {
                M_LOG_WARN << "re-register global Watcher, invalid type[" << it->second
                           << "], path[" << it->first << "], on " << resumeCount << " time, skip";
                break;
            }

            std::string abs_path = ChangeToAbsPath(it->first);

            if ((it->second & WATCHER_EXISTS) == WATCHER_EXISTS) {
                ret = zoo_exists(m_zhandle, abs_path.c_str(), 1, NULL);
                if (ret == ZNONODE) {
                    ret = ZOK;
                }
            }

            if ((it->second & WATCHER_GET) == WATCHER_GET) {
                char buf;
                int buflen = 1;
                ret = zoo_get(m_zhandle, abs_path.c_str(), 1, &buf, &buflen, NULL);
            }

            if ((it->second & WATCHER_GET_CHILDREN) == WATCHER_GET_CHILDREN) {
                ScopedStringVector children;
                ret = zoo_get_children(m_zhandle, abs_path.c_str(), 1, &children);
            }

            if (resumeCount >= resumeAlertCount && resumeGlobalWatcherAlertNotifier) {
                M_LOG_SPCL << "call resumeGlobalWatcherAlertNotifier on resume, path: " << it->first
                           << "], watcher type[" << it->second << "], on " << resumeCount << " time";
                resumeGlobalWatcherAlertNotifier();
            }

            if (ret != ZOK) {
                isOk = false;
                M_LOG_ERROR << "re-register global Watcher error, path[" << it->first << "], ret[" << ret
                            << "], zerror[" << ZError(ret) << "], on " << resumeCount << " time";
            } else {
                if (callWatcherFuncOnResume) {
                    if (IsValidCallBack(m_global_watcher_context->m_watcher_func)) {
                        M_LOG_SPCL << "call global watcher function on resume, path: " << it->first
                                   << "], watcher type[" << it->second << "], on " << resumeCount << " time";
                        (*m_global_watcher_context->m_watcher_func)(*this,
                                                                    RESUME_EVENT, RESUME_SUCC, it->first.c_str());
                    } else { M_LOG_ERROR << "invalid global watcher function"; }
                }
                break;
            }
        }
    }
    return isOk;
}

bool ZookeeperManager::ResumeCustomWatcher() {
    bool isOk = true;
    int32_t ret = ZOK;
    // 重新注册自定义Watcher
    M_LOG_SPCL << "start re-registering custom Watcher...";
    auto m_custom_watcher_contexts = getCustomWatcherContexts();
    for (auto it = m_custom_watcher_contexts.begin(); it != m_custom_watcher_contexts.end(); ++it) {
        for (int resumeCount = 1; resumeCount <= resumeMaxCount; ++resumeCount) {
            if (resumeCount - 1 > 0) sleep((resumeCount - 1) <= 10 ? (resumeCount - 1) : 10);
            ret = ZOK;

            M_LOG_SPCL << "re-register custom Watcher, path[" << it->first
                       << "], type[" << it->second->m_watcher_type << "], on " << resumeCount << " time";
            if (it->second->m_watcher_type == ZookeeperCtx::EXISTS) {
                ret = zoo_wexists(m_zhandle, it->first.c_str(),
                                  &ZookeeperManager::InnerWatcherCbFunc, it->second.get(), NULL);
                if (ret == ZNONODE) ret = ZOK;
            } else if (it->second->m_watcher_type == ZookeeperCtx::GET) {
                char buf;
                int buflen = 1;
                ret = zoo_wget(m_zhandle, it->first.c_str(), &ZookeeperManager::InnerWatcherCbFunc,
                               it->second.get(), &buf, &buflen, NULL);
            } else if (it->second->m_watcher_type == ZookeeperCtx::GET_CHILDREN) {
                ScopedStringVector children;
                ret = zoo_wget_children(m_zhandle, it->first.c_str(),
                                        &ZookeeperManager::InnerWatcherCbFunc, it->second.get(), &children);
            } else {
                M_LOG_WARN << "re-register custom Watcher, invalid Watcher type, path: " << it->first
                           << "], type[" << it->second << "], on " << resumeCount << " time";
                break;
            }

            if (resumeCount >= resumeAlertCount && resumeCustomWatcherAlertNotifier) {
                M_LOG_SPCL << "call resumeCustomWatcherAlertNotifier on resume, path: " << it->first
                           << "], watcher type[" << it->second->m_watcher_type
                           << "], on " << resumeCount << " time";
                resumeCustomWatcherAlertNotifier();
            }

            if (ret != ZOK) {
                isOk = false;
                M_LOG_ERROR << "re-register custom Watcher error, path[" << it->first << "], ret[" << ret
                            << "], zerror[" << ZError(ret) << "], on " << resumeCount << " time";
            } else {
                if (callWatcherFuncOnResume) {
                    if (IsValidCallBack(it->second->m_watcher_func)) {
                        M_LOG_SPCL << "call custom watcher function on resume, path: " << it->first
                                   << "], watcher type[" << it->second->m_watcher_type
                                   << "], on " << resumeCount << " time";
                        (*it->second->m_watcher_func)(*this, RESUME_EVENT, RESUME_SUCC, it->first.c_str());
                    } else { M_LOG_ERROR << "invalid global watcher function"; }
                }
                break;
            }
        }
    }
    return isOk;
}

bool ZookeeperManager::ResumeEphemeralNode() {
    bool isOk = true;
    int32_t ret = ZOK;
    // 重新注册所有的临时节点
    M_LOG_SPCL << "start re-creating tmp node...";
    auto m_ephemeral_node_info = getEphemeralNodeInfo();
    for (auto it = m_ephemeral_node_info.begin(); it != m_ephemeral_node_info.end(); ++it) {
        for (int resumeCount = 1; resumeCount <= resumeMaxCount; ++resumeCount) {
            if (resumeCount - 1 > 0) sleep((resumeCount - 1) <= 10 ? (resumeCount - 1) : 10);
            M_LOG_SPCL << "re-create tmp node, path[" << it->first
                       << "], data[" << it->second.Data << "], on " << resumeCount << " time";
            // 先尝试创建一下临时节点, 如果失败提示节点不存在, 表示父节点不存在, 创建父节点后重试一次
            ret = zoo_create(m_zhandle, it->first.c_str(), it->second.Data.c_str(),
                             it->second.Data.size(), &it->second.Acl, it->second.Flags, NULL, 0);

            if (ret == ZNONODE) {
                // 递归创建父节点
                auto last_slash_pos = it->first.rfind('/');
                if (last_slash_pos == string::npos) {
                    M_LOG_ERROR << "can not create parent node, invalid tmp node path: " << it->first << ", skip";
                    break;
                }

                if (last_slash_pos == 0) {
                    M_LOG_ERROR << "can not create under root node, tmp node: " << it->first << ", skip";
                    break;
                }

                string parent_path = it->first.substr(0, last_slash_pos);
                ret = CreatePathRecursion(parent_path);
                if (ret != ZOK) {
                    // 重试一次
                    ret = CreatePathRecursion(parent_path);
                    if (ret != ZOK) {
                        if (ret == ZNODEEXISTS) {
                            M_LOG_WARN << "parent node[" << parent_path << "] already exists, ret["
                                       << ret << "], zerror[" << ZError(ret) << "], on " << resumeCount << " time";
                        } else {
                            M_LOG_ERROR << "create parent node[" << parent_path << "] failed again, ret[" << ret
                                        << "], zerror[" << ZError(ret) << "], tmp node[" << it->first
                                        << "] can not be created, on " << resumeCount << " time";
                        }
                    }
                }

                // 重新创建临时节点, 错误码在外层判断
                ret = zoo_create(m_zhandle, it->first.c_str(), it->second.Data.c_str(),
                                 it->second.Data.size(), &it->second.Acl, it->second.Flags, NULL, 0);
            }

            if (resumeCount >= resumeAlertCount && resumeEphemeralNodeAlertNotifier) {
                M_LOG_SPCL << "call resumeEphemeralNodeAlertNotifier on resume, path: " << it->first
                           << "], on " << resumeCount << " time";
                resumeEphemeralNodeAlertNotifier();
            }

            if (ret != ZOK) {
                // 重试一次
                ret = zoo_create(m_zhandle, it->first.c_str(), it->second.Data.c_str(),
                                 it->second.Data.size(), &it->second.Acl, it->second.Flags, NULL, 0);
                if (ret != ZOK) {
                    if (ret == ZNODEEXISTS) {
                        M_LOG_WARN << "tmp node[" << it->first << "] already exists, ret[" << ret
                                   << "], zerror[" << ZError(ret) << "], on " << resumeCount << " time";
                        break;
                    } else {
                        isOk = false;
                        M_LOG_ERROR << "re-create tmp node[" << it->first << "] failed again, ret[" << ret
                                    << "], zerror[" << ZError(ret) << "], on " << resumeCount << " time";
                    }
                } else { break; }
            } else { break; }
        }
    }
    return isOk;
}

void ZookeeperManager::ProcMultiEphemeralNode(const vector<zoo_op> &multi_ops,
                                              const vector<zoo_op_result_t> &multi_result) {
    // 处理临时节点, 这里可能会出现部分调用成功, 部分调用失败的情况, 目前只把成功的写到临时节点数据中
    auto result_it = multi_result.begin();
    for (auto zoo_op_it = multi_ops.begin(); zoo_op_it != multi_ops.end() && result_it != multi_result.end();
         ++zoo_op_it, ++result_it) {
        if (result_it->err != ZOK) {
            M_LOG_ERROR << "operation failed, type: " << zoo_op_it->type
                        << ", path" << CharPtrToString(zoo_op_it->create_op.path);
            continue;
        }

        if (zoo_op_it->type == ZOO_CREATE_OP && (zoo_op_it->create_op.flags & ZOO_EPHEMERAL)) {
            std::unique_lock<std::mutex> phemeral_node_info_lock(m_ephemeral_node_info_lock);
            // 如果是临时节点, 添加到临时节点列表中。
            m_ephemeral_node_info[zoo_op_it->create_op.path].Acl = *zoo_op_it->create_op.acl;
            m_ephemeral_node_info[zoo_op_it->create_op.path].Data.assign(zoo_op_it->create_op.data,
                                                                         zoo_op_it->create_op.datalen);
            m_ephemeral_node_info[zoo_op_it->create_op.path].Flags = zoo_op_it->create_op.flags;
            M_LOG_SPCL << "add ephemeral node info: " << CharPtrToString(zoo_op_it->create_op.path)
                       << ", ephemeral node number: " << m_ephemeral_node_info.size();
        } else {
            std::unique_lock<std::mutex> phemeral_node_info_lock(m_ephemeral_node_info_lock);
            if (zoo_op_it->type == ZOO_DELETE_OP
                && m_ephemeral_node_info.find(zoo_op_it->create_op.path) != m_ephemeral_node_info.end()) {
                m_ephemeral_node_info.erase(zoo_op_it->create_op.path);
                M_LOG_SPCL << "delete ephemeral node info: " << CharPtrToString(zoo_op_it->create_op.path)
                           << ", ephemeral node number: " << m_ephemeral_node_info.size();
            } else if (zoo_op_it->type == ZOO_SETDATA_OP
                       && m_ephemeral_node_info.find(zoo_op_it->create_op.path) != m_ephemeral_node_info.end()) {
                // 如果在临时节点列表中找到, 修改数据
                m_ephemeral_node_info[zoo_op_it->create_op.path].Data.assign(zoo_op_it->create_op.data,
                                                                             zoo_op_it->create_op.datalen);
            } else {
                // Nothing
            }
        }
    }
}

void ZookeeperManager::ProcAsyncWatcher(ZookeeperCtx &context) {
    if (!context.m_watch_path.empty()) {
        if (context.m_global_watcher_add_type != 0) {
            // 全局Watcher
            std::unique_lock<std::mutex> global_watcher_path_type_lock(m_global_watcher_path_type_mutex);
            m_global_watcher_path_type[context.m_watch_path] |= context.m_global_watcher_add_type;
        } else if (context.m_custom_watcher_context != NULL) {
            // 自定义Watcher
            AddCustomWatcher(context.m_watch_path, context.m_custom_watcher_context);
        } else {
            M_LOG_ERROR << "Wathcer is not custom or global, there is some problem, path: " << context.m_watch_path;
        }
    }
}

std::string ZookeeperManager::ZError(int err) {
    return CharPtrToString(zerror(err));
}

std::string ZookeeperManager::CharPtrToString(const char *p) {
    return std::string(p ? p : "");
}

void MultiOps::AddCreateOp(const string &path, const char *value, int valuelen,
                           const ACL_vector *acl /*= &ZOO_OPEN_ACL_UNSAFE*/, int flags /*= 0*/,
                           uint32_t max_real_path_size /*= 128*/) {
    zoo_op op;
    string abs_path = mp_zk_manager == NULL ? path : mp_zk_manager->ChangeToAbsPath(path);
    shared_ptr<string> curr_path = make_shared<string>(move(abs_path));
    shared_ptr<string> curr_buffer = make_shared<string>(value, valuelen);

    if (max_real_path_size > 0) {
        shared_ptr<string> real_path = make_shared<string>(max_real_path_size, '\0');
        zoo_create_op_init(&op, curr_path->c_str(), curr_buffer->data(), curr_buffer->size(),
                           acl, flags, &(*real_path).at(0), real_path->size());
        m_inner_strings.push_back(real_path);
    } else {
        zoo_create_op_init(&op, curr_path->c_str(), curr_buffer->data(), curr_buffer->size(),
                           acl, flags, NULL, 0);
    }

    m_multi_ops.push_back(op);
    m_inner_strings.push_back(curr_path);
    m_inner_strings.push_back(curr_buffer);
}

void MultiOps::AddCreateOp(const string &path, const string &value,
                           const ACL_vector *acl /*= &ZOO_OPEN_ACL_UNSAFE*/, int flags /*= 0*/,
                           uint32_t max_real_path_size /*= 128*/) {
    AddCreateOp(path, value.data(), value.size(), acl, flags, max_real_path_size);
}

void MultiOps::AddDeleteOp(const string &path, int version) {
    zoo_op op;
    string abs_path = mp_zk_manager == NULL ? path : mp_zk_manager->ChangeToAbsPath(path);
    shared_ptr<string> curr_path = make_shared<string>(move(abs_path));

    zoo_delete_op_init(&op, curr_path->c_str(), version);

    m_multi_ops.push_back(op);
    m_inner_strings.push_back(curr_path);
}

void MultiOps::AddSetOp(const string &path, const char *buffer, int buflen, int version, bool need_stat /*= false*/) {
    zoo_op op;
    string abs_path = mp_zk_manager == NULL ? path : mp_zk_manager->ChangeToAbsPath(path);
    shared_ptr<string> curr_path = make_shared<string>(move(abs_path));
    shared_ptr<string> curr_buffer = make_shared<string>(buffer, buflen);
    if (need_stat) {
        shared_ptr<string> stat_buf = make_shared<string>(sizeof(Stat), '\0');
        zoo_set_op_init(&op, curr_path->c_str(), curr_buffer->data(), curr_buffer->size(),
                        version, reinterpret_cast<Stat *>(const_cast<char *>(stat_buf->data())));
        m_inner_strings.push_back(stat_buf);
    } else {
        zoo_set_op_init(&op, curr_path->c_str(), curr_buffer->data(), curr_buffer->size(), version, NULL);
    }

    m_multi_ops.push_back(op);
    m_inner_strings.push_back(curr_path);
    m_inner_strings.push_back(curr_buffer);
}

void MultiOps::AddSetOp(const string &path, const string &buffer, int version, bool need_stat /*= false*/) {
    AddSetOp(path, buffer.data(), buffer.size(), version, need_stat);
}

void MultiOps::AddCheckOp(const string &path, int version) {
    zoo_op op;
    string abs_path = mp_zk_manager == NULL ? path : mp_zk_manager->ChangeToAbsPath(path);
    shared_ptr<string> curr_path = make_shared<string>(move(abs_path));

    zoo_check_op_init(&op, curr_path->c_str(), version);

    m_multi_ops.push_back(op);
    m_inner_strings.push_back(curr_path);
}

}
