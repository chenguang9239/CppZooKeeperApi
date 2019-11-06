#ifndef _CPP_ZOOKEEPER_H_
#define _CPP_ZOOKEEPER_H_

#include <map>
#include <list>
#include <mutex>
#include <memory>
#include <thread>
#include <string>
#include <vector>
#include <atomic>
#include <functional>
#include <condition_variable>
#include "zookeeper/zookeeper.h"

#include "boost-log/log.h"

/*
Zookeeper封装API实现功能：
    Watcher
        Watcher自动重注册，提供取消重注册的接口，Watcher触发后可以通过自定义Watcher决定是否再次注册Watcher
        TODO：Watcher触发，会将数据自动带回来（重注册需要调用原始API）
        Watcher回调支持函数对象
    函数优化
        支持函数对象回调
        封装常用操作函数，优化接口参数，减少函数量    
    内存优化
        使用shared_ptr进行内存管理，避免内存泄露
        封装内部数据结构，自动释放
    其他优化
        支持XML配置文件方式初始化
        支持相对路径（内部实现全部使用绝对路径，不使用ZooKeeper C api的相对路径功能）
        支持一些额外功能函数，如递归创建节点，获得所有子节点的节点名称和路径等
        支持Session超时自动，重连时自动注册Watcher，创建临时节点
        支持使用Client ID重连在Session没超时时重连
    
未实现的非功能可以通过GetHandler()获得原始API句柄调用

使用注意事项：
    重连时，可能会出现本地状态和Zookeeper状态不一致的情况，比如少接了一个Watcher？
    为了保险起见，最好全部重新初始化状态，重新注册相应的Watcher。这个使用者维护。
    非线程安全，一个实例只能用于一个线程，除非用户自己加锁保护
    任何回调中，不能进行阻塞操作，否则会影响后面流程的回调
*/

//watcher事件类型（type）
//ZOO_CREATED_EVENT(value=1)：节点创建事件，需要watch一个不存在的节点，当节点被创建时触发，此watch通过zoo_exists()设置
//ZOO_DELETED_EVENT(value=2)：节点删除事件，此watch通过zoo_exists()或zoo_get()设置
//ZOO_CHANGED_EVENT(value=3)：节点数据改变事件，此watch通过zoo_exists()或zoo_get()设置
//ZOO_CHILD_EVENT(value=4)：子节点列表改变事件，此watch通过zoo_get_children()或zoo_get_children2()设置
//ZOO_SESSION_EVENT(value=-1)：会话事件，客户端与服务端断开或重连时触发
//ZOO_NOTWATCHING_EVENT(value=-2)：watch移除事件，服务端出于某些原因不再为客户端watch节点时触发
//
//watcher事件状态（state）
//state=-112 会话超时状态 ZOO_EXPIRED_SESSION_STATE
//state= -113　认证失败状态 ZOO_AUTH_FAILED_STATE
//state= 1 连接建立中 ZOO_CONNECTING_STATE
//state= 2 (暂时不清楚如何理解这个状态,ZOO_ASSOCIATING_STATE)
//state=3 连接已建立状态 ZOO_CONNECTED_STATE
//state= 999 无连接状态

// add a new type
// type=30203 resume事件

// add a new state
// state=20203 resume成功状态


namespace CppZooKeeper {

class ZookeeperManager;

class ZookeeperCtx;

enum CPP_ZOO_KEEPER_EVENT {
    RESUME_EVENT = 30203
};

enum CPP_ZOO_KEEPER_STATE {
    RESUME_SUCC = 20203
};

enum CPP_ZOO_KEEPER_ERR_CODE {
    ILLEGAL_PATH = 10203,
    NO_TYPE_IN_PATH = 10405,
    DEFAULT_TYPE_IN_PATH = 10607
};

enum CPP_ZOO_KEEPER_LOG_LEVEL {
    DEBUG_LEVEL,
    INFO_LEVEL,
    WARN_LEVEL,
    ERROR_LEVEL
};

void InitCPPAPILogger(const std::string &path, const std::string &name,
                      logger::LEVEL level, unsigned int flushPeriod = 0);

void InitCAPILogger(const std::string &path, ZooLogLevel level);

class MultiOps {
public:
    /** 构造函数
     *
     * @param   ZookeeprerManager * p_zk_manager    填入mp_zk_manager可以使用相对路径，为NULL时不能使用相对路径
     * @retval
     * @author  moon
     */
    MultiOps(ZookeeperManager *p_zk_manager = NULL) : mp_zk_manager(p_zk_manager) {
    }

    // max_real_path_size：为0表示不获得创建的节点路径
    void AddCreateOp(const std::string &path, const char *value, int valuelen,
                     const ACL_vector *acl = &ZOO_OPEN_ACL_UNSAFE, int flags = 0,
                     uint32_t max_real_path_size = 128);

    void
    AddCreateOp(const std::string &path, const std::string &value, const ACL_vector *acl = &ZOO_OPEN_ACL_UNSAFE,
                int flags = 0, uint32_t max_real_path_size = 128);

    void AddDeleteOp(const std::string &path, int version);

    // need_stat：是否获得stat信息
    void AddSetOp(const std::string &path, const char *buffer, int buflen, int version, bool need_stat = false);

    void AddSetOp(const std::string &path, const std::string &buffer, int version, bool need_stat = false);

    // 检查节点是否存在，并且是否符合version，当version为-1时不考虑version是否匹配
    void AddCheckOp(const std::string &path, int version);

    uint32_t Size() {
        return m_multi_ops.size();
    }

    bool Empty() {
        return m_multi_ops.empty();
    }

    std::vector<zoo_op> m_multi_ops;

protected:
    // 保存内部产生的一些需要保存的空间，结构销毁后，自动释放内存
    std::list<std::shared_ptr<std::string>> m_inner_strings;
    ZookeeperManager *mp_zk_manager;
};

// 值和Stat
struct ValueStat {
    std::string value;
    Stat stat;
};

// WatcherFunType的返回值表示是否停止此Watcher，如果停止(返回true)，则不再重注册
typedef std::function<bool(ZookeeperManager &zookeeper_manager,
                           int type, int state, const char *path)> WatcherFuncType;

typedef std::function<void(ZookeeperManager &zookeeper_manager, int rc,
                           const char *value)> StringCompletionFuncType;

typedef std::function<void(ZookeeperManager &zookeeper_manager, int rc,
                           ACL_vector *acl, Stat *stat)> AclCompletionFuncType;

typedef std::function<void(ZookeeperManager &zookeeper_manager, int rc,
                           const char *value, int value_len, const Stat *stat)> DataCompletionFuncType;

typedef std::function<void(ZookeeperManager &zookeeper_manager, int rc,
                           const String_vector *strings, const Stat *stat)> StringsStatCompletionFuncType;

typedef std::function<void(ZookeeperManager &zookeeper_manager, int rc, std::shared_ptr<MultiOps> &multi_ops,
                           std::shared_ptr<std::vector<zoo_op_result_t>> &multi_results)> MultiCompletionFuncType;

typedef std::function<void(ZookeeperManager &zookeeper_manager, int rc)> VoidCompletionFuncType;

typedef std::function<void(ZookeeperManager &zookeeper_manager, int rc, const Stat *stat)> StatCompletionFuncType;

class ScopedStringVector : public String_vector {
public:
    ScopedStringVector() {
        count = 0;
        data = NULL;
    }

    char *GetData(int32_t i) {
        if (i >= count) { return NULL; }
        return data[i];
    }

    virtual ~ScopedStringVector() { Clear(); }

    void Clear() {
        deallocate_String_vector(this);
        count = 0;
    }

private:
    ScopedStringVector(ScopedStringVector &&right) = delete;

    ScopedStringVector(const ScopedStringVector &right) = delete;

    ScopedStringVector &operator=(ScopedStringVector &&right) = delete;

    ScopedStringVector &operator=(const ScopedStringVector &right) = delete;

};

class ScopedAclVector : public ACL_vector {
public:
    ScopedAclVector() {
        count = 0;
        data = NULL;
    }

    ACL *GetData(int32_t i) {
        if (i >= count) { return NULL; }
        return &data[i];
    }

    virtual ~ScopedAclVector() { Clear(); }

    void Clear() {
        deallocate_ACL_vector(this);
        count = 0;
    }

private:
    ScopedAclVector(ScopedAclVector &&right) = delete;

    ScopedAclVector(const ScopedAclVector &right) = delete;

    ScopedAclVector &operator=(ScopedAclVector &&right) = delete;

    ScopedAclVector &operator=(const ScopedAclVector &right) = delete;
};

struct EphemeralNodeInfo {
    std::string Data;
    ACL_vector Acl;
    int Flags;
};

class ZookeeperManager {
public:
    std::atomic<bool> reconnecting;

    // int32_t m_errno;        // 暂时没想好要不要用这个，先不要用吧
    // 是否在析构的时候不主动关闭连接，特殊配置，一般情况保持false，不要使用，只用于在重启时不希望删除临时节点时使用
    bool m_dont_close;

    ZookeeperManager();

    virtual ~ZookeeperManager();

#ifdef CPP_ZK_USE_BOOST
    /** 从配置文件中读取配置，配置文件内容为xml：
    <?xml version="1.0" encoding="UTF-8" ?>
    <ZkConf>
        <Root>/QQ_IOS</Root>
        <Hosts>192.168.174.128:2181</Hosts>
    </ZkConf>
     *
     * @param   const std::string & config_file_path
     * @retval  int32_t
     * @author  moon
     */
    int32_t InitFromFile(const std::string &config_file_path, const clientid_t *client_id = NULL);
#endif

    /**
     *
     * @param   const std::string & hosts       格式：ip:port,ip:port
     * @param   const std::string & root_path   根节点必须为有效路径，为了支持路径填写相对或者绝对路径
     * @retval  int32_t
     * @author  moon
     */
    int32_t Init(const std::string &hosts, const std::string &root_path = "/", const clientid_t *client_id = NULL);

    void SetReconnectOptions(std::function<void()> userReconnectAlertNotifier,
                             int userReconnectAlertCount = 3, int userReconnectMaxCount = INT_MAX);

    void SetResumeOptions(std::function<void()> userResumeEphemeralNodeAlertNotifier,
                          std::function<void()> userResumeCustomWatcherAlertNotifier = nullptr,
                          std::function<void()> userResumeGlobalWatcherAlertNotifier = nullptr,
                          int userResumeAlertCount = 3, int userResumeMaxCount = 5);

    void SetCallWatcherFuncOnResume(bool flag);

    /** 连接，阻塞操作，直到连接成功或者超时，超时后，也许会连接成功，更加稳妥的做法是，重新连接
     *
     * @param   const std::string & hosts
     * @param   std::shared_ptr<WatcherFunType> global_watcher_fun
     * @param   int32_t recv_timeout_ms
     * @param   uint32_t conn_timeout_ms                            连接超时时间，为0表示永久等待
     * @retval  int32_t
     * @author  moon
     */
    int32_t Connect(std::shared_ptr<WatcherFuncType> global_watcher_fun,
                    int32_t recv_timeout_ms, uint32_t conn_timeout_ms = 3000);

    /** 获得ClientID
     *
     * @retval 	const zookeeper::clientid_t *
     * @author 	moon
     */
    const clientid_t *GetClientID() { return &m_zk_client_id; }

    /** 重连，重连会导致所有的Watcher重新注册，使用shared_ptr避免重连时的一次拷贝构造
     *  阻塞操作
     *
     * @retval  int32_t
     * @author  moon
     */
    int32_t Reconnect();

    int GetStatus() { return zoo_state(m_zhandle); }

    zhandle_t *GetHandler() { return m_zhandle; }

    // AExists接口如果调用成功，节点存在，一定包含Stat数据
    int32_t AExists(const std::string &path,
                    std::shared_ptr<StatCompletionFuncType> stat_completion_fun, int watch = 0);

    int32_t AExists(const std::string &path, std::shared_ptr<StatCompletionFuncType> stat_completion_fun,
                    std::shared_ptr<WatcherFuncType> watcher_fun);

    int32_t Exists(const std::string &path, Stat *stat = NULL, int watch = 0);

    int32_t Exists(const std::string &path, Stat *stat, std::shared_ptr<WatcherFuncType> watcher_fun);

    // Aget的数据内存由Zookeeper API分配和释放，调用者无需释放，Get的内存由调用者申请和释放
    // Get接口拿到的数据注意不会在Buf后面补"\0"的，因为它不一定是C字符串格式，如果确定是文本，需要调用者补零
    // Aget接口如果调用成功，一定包含Stat数据并且不需要用户释放，需要注意的是，回调函数中的Buffer和len不需要自己释放，
    // 但是不保证是'\0'结尾，作为字符串的话要复制一份并且在末尾补'\0'
    int32_t AGet(const std::string &path,
                 std::shared_ptr<DataCompletionFuncType> data_completion_fun, int watch = 0);

    int32_t AGet(const std::string &path, std::shared_ptr<DataCompletionFuncType> data_completion_fun,
                 std::shared_ptr<WatcherFuncType> watcher_fun);

    int32_t Get(const std::string &path, char *buffer, int *buflen, Stat *stat = NULL, int watch = 0);

    int32_t Get(const std::string &path, char *buffer,
                int *buflen, Stat *stat, std::shared_ptr<WatcherFuncType> watcher_fun);

    // GetChildren函数实际上是使用StringsCompletionFunType的，但是只有它用，就使用StringsStatCompletionFuncType了，
    // 如果不需要stat的话，传入的stat为NULL，去掉StringsCompletionFunType
    // AGetChildren回调函数中返回的String_vector不需要用户释放，zookeeper的API会自动释放内存
    // GetChildren中使用ScopedStringVector作为数据传出结构，包含自动释放内存
    int32_t AGetChildren(const std::string &path,
                         std::shared_ptr<StringsStatCompletionFuncType> strings_stat_completion_fun,
                         int watch = 0, bool need_stat = false);

    int32_t AGetChildren(const std::string &path,
                         std::shared_ptr<StringsStatCompletionFuncType> strings_stat_completion_fun,
                         std::shared_ptr<WatcherFuncType> watcher_fun, bool need_stat = false);

    int32_t GetChildren(const std::string &path, ScopedStringVector &strings, int watch = 0, Stat *stat = NULL);

    int32_t
    GetChildren(const std::string &path, ScopedStringVector &strings, std::shared_ptr<WatcherFuncType> watcher_fun,
                Stat *stat = NULL);

    // std::string *p_real_path需要使用的话，应该先resize()到合适的大小，内部是将它的size()作为缓冲区最大空间，传出的是绝对路径
    // ephemeral_exist_skip仅在有临时节点时，使用新实例和老的ClientID连接ZK所用
    int32_t ACreate(const std::string &path, const char *value, int valuelen,
                    std::shared_ptr<StringCompletionFuncType> string_completion_fun,
                    const ACL_vector *acl = &ZOO_OPEN_ACL_UNSAFE, int flags = 0);

    int32_t ACreate(const std::string &path, const std::string &value,
                    std::shared_ptr<StringCompletionFuncType> string_completion_fun,
                    const ACL_vector *acl = &ZOO_OPEN_ACL_UNSAFE, int flags = 0);

    int32_t Create(const std::string &path, const char *value, int valuelen, std::string *p_real_path = NULL,
                   const ACL_vector *acl = &ZOO_OPEN_ACL_UNSAFE, int flags = 0, bool ephemeral_exist_skip = false);

    int32_t Create(const std::string &path, const std::string &value, std::string *p_real_path = NULL,
                   const ACL_vector *acl = &ZOO_OPEN_ACL_UNSAFE, int flags = 0, bool ephemeral_exist_skip = false);

    int32_t CreateRecursively(const std::string &path,
                              const std::string &value,
                              std::string *p_real_path = NULL,
                              const ACL_vector *acl = &ZOO_OPEN_ACL_UNSAFE,
                              int flags = 0,
                              bool ephemeral_exist_skip = false);

    int32_t ASet(const std::string &path, const char *buffer, int buflen,
                 int version, std::shared_ptr<StatCompletionFuncType> stat_completion_fun);

    int32_t ASet(const std::string &path, const std::string &buffer,
                 int version, std::shared_ptr<StatCompletionFuncType> stat_completion_fun);

    int32_t Set(const std::string &path, const char *buffer, int buflen, int version, Stat *stat = NULL);

    int32_t Set(const std::string &path, const std::string &buffer, int version, Stat *stat = NULL);

    // 删除节点必须要传入version，避免误删，如果真的要强制删除，version填入-1即可
    int32_t ADelete(const std::string &path, int version,
                    std::shared_ptr<VoidCompletionFuncType> void_completion_fun);

    int32_t Delete(const std::string &path, int version = -1);

    // AGetAcl回调中acl和stat均无需调用方释放
    int32_t AGetAcl(const std::string &path, std::shared_ptr<AclCompletionFuncType> acl_completion_fun);

    int32_t GetAcl(const std::string &path, ScopedAclVector &acl, Stat *stat);

    int32_t ASetAcl(const std::string &path, int version, ACL_vector *acl,
                    std::shared_ptr<VoidCompletionFuncType> void_completion_fun);

    int32_t SetAcl(const std::string &path, int version, ACL_vector *acl);

    // 批量操作接口，同步操作返回值results不需要填充长度，内部会清空并填充
    // 结果中的value字段仅在Create操作中表示创建的节点的实际地址，如果没有传入Buffer的话，为NULL，目前此API均会传入Buffer
    // results字段中的数据如果要使用时必须保证MultiOps没有被销毁，因为数据实际存储空间是在MultiOps中创建并且管理的
    MultiOps CreateMultiOps();

    int32_t AMulti(std::shared_ptr<MultiOps> &multi_ops,
                   std::shared_ptr<MultiCompletionFuncType> multi_completion_fun);

    int32_t Multi(MultiOps &multi_ops, std::vector<zoo_op_result_t> &results);

    /** 相对路径转绝对路径，如果已经是绝对路径了，就原样返回
     *
     * @param   const std::string & path
     * @retval  const std::string
     * @author  moon
     */
    const std::string ChangeToAbsPath(const std::string &path);

    /* 额外接口 */

    /** 递归创建路径，内容为空，仅支持创建普通节点，因为增加其他的操作会增加不少复杂度
     *
     * @param   const std::string & path
     * @retval  int32_t
     * @author  moon
     */
    int32_t CreatePathRecursion(const std::string &path);

    /** 递归删除路径
     *
     * @param   const std::string & path
     * @retval  int32_t
     * @author  moon
     */
    int32_t DeletePathRecursion(const std::string &path);

    /** 将节点的子节点的Key和Value都拿出来
     *
     * @param 	const std::string & path
     * @param 	std::map<std::string
     * @param 	ValueStat> & children_value
     * @param 	uint32_t max_value_size         由于获得节点内容需要预先分配内存，这个值表示每个Value预先分配内存的大小
     * @retval 	int32_t
     * @author 	moon
     */
    int32_t GetChildrenValue(const std::string &path,
                             std::map<std::string, ValueStat> &children_value, uint32_t max_value_size = 2048);

    // 获得以'\0'结尾的字符串数据，缓冲区data的长度需要用户预先分配（包含结尾的'\0'）
    int32_t GetCString(const std::string &path, std::string &data, Stat *stat = NULL, int watch = 0);

    int32_t GetCString(const std::string &path, std::string &data,
                       Stat *stat, std::shared_ptr<WatcherFuncType> watcher_fun);


    std::map<std::string, uint8_t> getGlobalWatcherPathType();

    std::map<std::string, EphemeralNodeInfo> getEphemeralNodeInfo();

    std::multimap<std::string, std::shared_ptr<ZookeeperCtx>> getCustomWatcherContexts();

protected:
    zhandle_t *m_zhandle;
    std::string m_hosts;
    // API内部根目录，初始化后，一定是合法的
    std::string m_root_path;

    // Zookeeper创建的线程的ID
    pid_t m_zk_tid;

    // 是否需要重连后重新注册Watcher和临时节点
//    bool m_need_resume_env;

    // Zookeeper连接成功后，会置上这个ClientID，初始化时也可以填写，client_id为0表示不使用
    clientid_t m_zk_client_id;

    std::mutex m_connect_lock;
    std::condition_variable m_connect_cond;

    // 全局Watcher的context
    std::shared_ptr<ZookeeperCtx> m_global_watcher_context;

    std::mutex m_custom_watcher_contexts_mutex;
    // <绝对路径,用户自定义Watcher的context>，用于断线重连注册Watcher
    std::multimap<std::string, std::shared_ptr<ZookeeperCtx>> m_custom_watcher_contexts;

    std::mutex m_global_watcher_path_type_mutex;
    // <全局Watcher的绝对路径,Watcher类型>，类型为GlobalWatcherType的值或结果，用于自动重注册Watcher和断线重连注册
    std::map<std::string, uint8_t> m_global_watcher_path_type;

    //std::recursive_mutex m_ephemeral_node_info_lock;
    std::mutex m_ephemeral_node_info_lock;

    // <绝对路径,所有临时节点信息>
    std::map<std::string, EphemeralNodeInfo> m_ephemeral_node_info;

    // user specified whether call watcher function on resume

    bool callWatcherFuncOnResume;

    // user specified retry max count and alert count
    int reconnectMaxCount; // if <= 0 then reconnect until succeed
    int resumeMaxCount;  // if <= 0, will use default value
    int reconnectAlertCount; // if <= 0, will use default value
    int resumeAlertCount; // if <= 0, will use default value

    // user specified interface
    std::function<void()> reconnectAlertNotifier;
    std::function<void()> resumeGlobalWatcherAlertNotifier;
    std::function<void()> resumeCustomWatcherAlertNotifier;
    std::function<void()> resumeEphemeralNodeAlertNotifier;

    /* 内部函数，用于传递给Zookeeper API */
    // When global or custom watcher is triggered，InnerWatcherCbFunc will be called
    // Each watcher has unique context
    // InnerWatcherCbFunc re-registers the watcher and call global or custom watcher callback func in its context
    static void InnerWatcherCbFunc(zhandle_t *zh, int type, int state,
                                   const char *abs_path, void *p_zookeeper_context);

    static void HandleSessionEvent(ZookeeperManager &manager, int state);

    static void TimeoutReconnect(ZookeeperManager &manager);

    static bool IsValidCallBack(std::shared_ptr<WatcherFuncType> func);

    static bool IsValidWatcherContext(ZookeeperCtx *p_context);

    static void SetWatcherContextInvalid(ZookeeperCtx *p_context);

    static bool IsValidManager(zhandle_t *zh, ZookeeperManager &manager);

    static bool IsValidEvent(ZookeeperCtx *p_context, const std::string &absPath, int type);

    static uint32_t GetWatcherType(ZookeeperCtx *p_context, const std::string &absPath = "");

    static int CustomWatcherAutoRegisterer(ZookeeperCtx *p_context, const char *abs_path, int type);

    static int GlobalWatcherAutoRegisterer(ZookeeperCtx *p_context, const char *abs_path, int type);

    static int AddExistWatcherOnce(ZookeeperCtx *p_context, const char *abs_path, bool addToGlobalWatcher = true);

    static uint8_t GetStopWatcherTypeMask(ZookeeperCtx *p_context, const std::string &absPath, int type);

    static void InnerVoidCompletion(int rc, const void *p_zookeeper_context);

    static void InnerStatCompletion(int rc, const Stat *stat, const void *p_zookeeper_context);

    static void InnerDataCompletion(int rc, const char *value, int value_len,
                                    const Stat *stat, const void *p_zookeeper_context);

    static void InnerStringsCompletion(int rc, const String_vector *strings, const void *p_zookeeper_context);

    static void InnerStringsStatCompletion(int rc, const String_vector *strings,
                                           const Stat *stat, const void *p_zookeeper_context);

    static void InnerStringCompletion(int rc, const char *value, const void *p_zookeeper_context);

    static void InnerAclCompletion(int rc, ACL_vector *acl, Stat *stat, const void *p_zookeeper_context);

    static void InnerMultiCompletion(int rc, const void *p_zookeeper_context);

    static std::string ZError(int err);

    static std::string CharPtrToString(const char *p);

    void AddCustomWatcher(const std::string &abs_path, std::shared_ptr<ZookeeperCtx> watcher_context);

    std::shared_ptr<ZookeeperCtx> GetCustomWatcherCtx(const std::string &path, int type,
                                                      const std::shared_ptr<WatcherFuncType> &watcher_fun);

    void DelCustomWatcherCtx(const std::string &abs_path, const ZookeeperCtx *watcher_context);

    bool ResumeEnv();

    bool ResumeGlobalWatcher();

    bool ResumeCustomWatcher();

    bool ResumeEphemeralNode();

    /** 处理批量操作过程中对临时节点列表的操作
     *
     * @param 	const std::vector<zoo_op> & multi_ops
     * @param 	const std::vector<zoo_op_result_t> & multi_result
     * @retval 	void
     * @author 	moon
     */
    void ProcMultiEphemeralNode(const std::vector<zoo_op> &multi_ops,
                                const std::vector<zoo_op_result_t> &multi_result);

    /** 处理异步操作成功后，对于Watcher的处理
     *
     * @param 	ZookeeperCtx & context
     * @retval 	void
     * @author 	moon
     */
    void ProcAsyncWatcher(ZookeeperCtx &context);

    /** 删除原生API中指定Watcher
     *
     * @param 	int type
     * @param 	const char * abs_path
     * @param 	void * p_zookeeper_context
     * @retval 	void
     * @author 	moon
     */
    void DeleteWatcher(int type, const char *abs_path, void *p_zookeeper_context);

    ZookeeperManager(ZookeeperManager &&right) = delete;

    ZookeeperManager(const ZookeeperManager &right) = delete;

    ZookeeperManager &operator=(ZookeeperManager &&right) = delete;

    ZookeeperManager &operator=(const ZookeeperManager &right) = delete;
};

}

#endif
