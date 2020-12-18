#ifndef _CPP_ZOOKEEPER_H_
#define _CPP_ZOOKEEPER_H_

#include <atomic>
#include <condition_variable>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "boost-log/log.h"
#include "zookeeper/zookeeper.h"

namespace zookeeper {

class ZookeeperManager;

class ZookeeperCtx;

// resume event
enum CPP_ZOO_KEEPER_EVENT { RESUME_EVENT = 30203 };

// resume success state
enum CPP_ZOO_KEEPER_STATE { RESUME_SUCC = 20203 };

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

void InitLogger(const std::string &path, const std::string &name,
                logger::LEVEL level, unsigned int flush_period = 0);

class MultiOps {
 public:
  MultiOps(ZookeeperManager *zk_manager = NULL)
      : multi_zk_manager(zk_manager) {}

  void AddCreateOp(const std::string &path, const char *value, int value_len,
                   const ACL_vector *acl = &ZOO_OPEN_ACL_UNSAFE, int flags = 0,
                   uint32_t max_real_path_size = 128);

  void AddCreateOp(const std::string &path, const std::string &value,
                   const ACL_vector *acl = &ZOO_OPEN_ACL_UNSAFE, int flags = 0,
                   uint32_t max_real_path_size = 128);

  void AddDeleteOp(const std::string &path, int version);

  void AddSetOp(const std::string &path, const char *buffer, int buf_len,
                int version, bool need_stat = false);

  void AddSetOp(const std::string &path, const std::string &buffer, int version,
                bool need_stat = false);

  void AddCheckOp(const std::string &path, int version);

  uint32_t Size() { return m_multi_ops.size(); }

  bool Empty() { return m_multi_ops.empty(); }

  std::vector<zoo_op> m_multi_ops;

 protected:
  std::list<std::shared_ptr<std::string>> m_inner_strings;
  ZookeeperManager *multi_zk_manager;
};

struct ValueStat {
  std::string value;
  Stat stat;
};

// stop to watch when return true
typedef std::function<bool(ZookeeperManager &zookeeper_manager, int type,
                           int state, const char *path)>
    WatcherFuncType;

typedef std::function<void(ZookeeperManager &zookeeper_manager, int ret_code,
                           const char *value)>
    StringCompletionFuncType;

typedef std::function<void(ZookeeperManager &zookeeper_manager, int ret_code,
                           ACL_vector *acl, Stat *stat)>
    AclCompletionFuncType;

typedef std::function<void(ZookeeperManager &zookeeper_manager, int ret_code,
                           const char *value, int value_len, const Stat *stat)>
    DataCompletionFuncType;

typedef std::function<void(ZookeeperManager &zookeeper_manager, int ret_code,
                           const String_vector *strings, const Stat *stat)>
    StringsStatCompletionFuncType;

typedef std::function<void(
    ZookeeperManager &zookeeper_manager, int ret_code,
    std::shared_ptr<MultiOps> &multi_ops,
    std::shared_ptr<std::vector<zoo_op_result_t>> &multi_results)>
    MultiCompletionFuncType;

typedef std::function<void(ZookeeperManager &zookeeper_manager, int ret_code)>
    VoidCompletionFuncType;

typedef std::function<void(ZookeeperManager &zookeeper_manager, int ret_code,
                           const Stat *stat)>
    StatCompletionFuncType;

class ScopedStringVector : public String_vector {
 public:
  ScopedStringVector() {
    count = 0;
    data = NULL;
  }

  char *GetData(int32_t i) {
    if (i >= count) {
      return NULL;
    }
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
    if (i >= count) {
      return NULL;
    }
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
  std::string data;
  ACL_vector acl;
  int flags;
};

class ZookeeperManager {
 public:
  std::atomic<bool> reconnecting_;
  bool m_dont_close_;

  ZookeeperManager();

  virtual ~ZookeeperManager();

  int32_t Init(const std::string &hosts, const std::string &root_path = "/",
               const clientid_t *client_id = NULL);

  void SetReconnectOptions(std::function<void()> reconnect_alerter,
                           int reconnect_alert_count = 3,
                           int reconnect_max_count = INT_MAX);

  void SetResumeOptions(
      std::function<void()> resume_ephemeral_node_alerter,
      std::function<void()> resume_custom_watcher_alerter = nullptr,
      std::function<void()> resume_global_watcher_alerter = nullptr,
      int resume_alert_count = 3, int resume_max_count = 5);

  void SetCallWatcherFuncOnResume(bool flag);

  int32_t Connect(std::shared_ptr<WatcherFuncType> global_watcher_fun,
                  int32_t recv_timeout_ms, uint32_t conn_timeout_ms = 3000);

  const clientid_t *GetClientID() { return &m_zk_client_id_; }

  int32_t Reconnect();

  int GetStatus() { return zoo_state(m_zhandle_); }

  zhandle_t *GetHandler() { return m_zhandle_; }

  int32_t AExists(const std::string &path,
                  std::shared_ptr<StatCompletionFuncType> stat_completion_func,
                  int watch = 0);

  int32_t AExists(const std::string &path,
                  std::shared_ptr<StatCompletionFuncType> stat_completion_func,
                  std::shared_ptr<WatcherFuncType> watcher_func);

  int32_t Exists(const std::string &path, Stat *stat = NULL, int watch = 0);

  int32_t Exists(const std::string &path, Stat *stat,
                 std::shared_ptr<WatcherFuncType> watcher_func);

  // Aget的数据内存由Zookeeper
  // API分配和释放，调用者无需释放，Get的内存由调用者申请和释放
  // Get接口拿到的数据注意不会在Buf后面补"\0"的，因为它不一定是C字符串格式，如果确定是文本，需要调用者补零
  // Aget接口如果调用成功，一定包含Stat数据并且不需要用户释放，需要注意的是，回调函数中的Buffer和len不需要自己释放，
  // 但是不保证是'\0'结尾，作为字符串的话要复制一份并且在末尾补'\0'
  int32_t AGet(const std::string &path,
               std::shared_ptr<DataCompletionFuncType> data_completion_func,
               int watch = 0);

  int32_t AGet(const std::string &path,
               std::shared_ptr<DataCompletionFuncType> data_completion_func,
               std::shared_ptr<WatcherFuncType> watcher_func);

  int32_t Get(const std::string &path, char *buffer, int *buf_len,
              Stat *stat = NULL, int watch = 0);

  int32_t Get(const std::string &path, char *buffer, int *buf_len, Stat *stat,
              std::shared_ptr<WatcherFuncType> watcher_func);

  // GetChildren函数实际上是使用StringsCompletionFunType的，但是只有它用，就使用StringsStatCompletionFuncType了，
  // 如果不需要stat的话，传入的stat为NULL，去掉StringsCompletionFunType
  // AGetChildren回调函数中返回的String_vector不需要用户释放，zookeeper的API会自动释放内存
  // GetChildren中使用ScopedStringVector作为数据传出结构，包含自动释放内存
  int32_t AGetChildren(const std::string &path,
                       std::shared_ptr<StringsStatCompletionFuncType>
                           strings_stat_completion_func,
                       int watch = 0, bool need_stat = false);

  int32_t AGetChildren(const std::string &path,
                       std::shared_ptr<StringsStatCompletionFuncType>
                           strings_stat_completion_func,
                       std::shared_ptr<WatcherFuncType> watcher_func,
                       bool need_stat = false);

  int32_t GetChildren(const std::string &path, ScopedStringVector &strings,
                      int watch = 0, Stat *stat = NULL);

  int32_t GetChildren(const std::string &path, ScopedStringVector &strings,
                      std::shared_ptr<WatcherFuncType> watcher_func,
                      Stat *stat = NULL);

  // std::string
  // *real_path需要使用的话，应该先resize()到合适的大小，内部是将它的size()作为缓冲区最大空间，传出的是绝对路径
  // ephemeral_exist_skip仅在有临时节点时，使用新实例和老的ClientID连接ZK所用
  int32_t ACreate(
      const std::string &path, const char *value, int value_len,
      std::shared_ptr<StringCompletionFuncType> string_completion_func,
      const ACL_vector *acl = &ZOO_OPEN_ACL_UNSAFE, int flags = 0);

  int32_t ACreate(
      const std::string &path, const std::string &value,
      std::shared_ptr<StringCompletionFuncType> string_completion_func,
      const ACL_vector *acl = &ZOO_OPEN_ACL_UNSAFE, int flags = 0);

  int32_t Create(const std::string &path, const char *value, int value_len,
                 std::string *real_path = NULL,
                 const ACL_vector *acl = &ZOO_OPEN_ACL_UNSAFE, int flags = 0,
                 bool ephemeral_exist_skip = false);

  int32_t Create(const std::string &path, const std::string &value,
                 std::string *real_path = NULL,
                 const ACL_vector *acl = &ZOO_OPEN_ACL_UNSAFE, int flags = 0,
                 bool ephemeral_exist_skip = false);

  int32_t CreateRecursively(const std::string &path, const std::string &value,
                            std::string *real_path = NULL,
                            const ACL_vector *acl = &ZOO_OPEN_ACL_UNSAFE,
                            int flags = 0, bool ephemeral_exist_skip = false);

  int32_t ASet(const std::string &path, const char *buffer, int buf_len,
               int version,
               std::shared_ptr<StatCompletionFuncType> stat_completion_func);

  int32_t ASet(const std::string &path, const std::string &buffer, int version,
               std::shared_ptr<StatCompletionFuncType> stat_completion_func);

  int32_t Set(const std::string &path, const char *buffer, int buf_len,
              int version, Stat *stat = NULL);

  int32_t Set(const std::string &path, const std::string &buffer, int version,
              Stat *stat = NULL);

  // 删除节点必须要传入version，避免误删，如果真的要强制删除，version填入-1即可
  int32_t ADelete(const std::string &path, int version,
                  std::shared_ptr<VoidCompletionFuncType> void_completion_fun);

  int32_t Delete(const std::string &path, int version = -1);

  // AGetAcl回调中acl和stat均无需调用方释放
  int32_t AGetAcl(const std::string &path,
                  std::shared_ptr<AclCompletionFuncType> acl_completion_func);

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

  const std::string ChangeToAbsPath(const std::string &path);

  int32_t CreatePathRecursion(const std::string &path);

  int32_t DeletePathRecursion(const std::string &path);

  int32_t GetChildrenValue(const std::string &path,
                           std::map<std::string, ValueStat> &children_value,
                           uint32_t max_value_size = 2048);

  int32_t GetCString(const std::string &path, std::string &data,
                     Stat *stat = NULL, int watch = 0);

  int32_t GetCString(const std::string &path, std::string &data, Stat *stat,
                     std::shared_ptr<WatcherFuncType> watcher_func);

  std::map<std::string, uint8_t> GetGlobalWatcherPathType();

  std::map<std::string, EphemeralNodeInfo> GetEphemeralNodeInfo();

  std::multimap<std::string, std::shared_ptr<ZookeeperCtx>>
  GetCustomWatcherContexts();

 protected:
  zhandle_t *m_zhandle_;
  std::string m_hosts_;
  std::string m_root_path_;

  pid_t m_zk_tid_;
  clientid_t m_zk_client_id_;

  std::mutex m_connect_lock_;
  std::condition_variable m_connect_cond_;

  std::shared_ptr<ZookeeperCtx> m_global_watcher_context_;

  std::mutex m_custom_watcher_contexts_mutex_;
  std::multimap<std::string, std::shared_ptr<ZookeeperCtx>>
      m_custom_watcher_contexts_;

  std::mutex m_global_watcher_path_type_mutex_;
  std::map<std::string, uint8_t> m_global_watcher_path_type_;

  std::mutex m_ephemeral_node_info_lock_;
  std::map<std::string, EphemeralNodeInfo> m_ephemeral_node_info_;

  bool call_watcher_func_on_resume_;

  int reconnect_max_count_;    // if <= 0 then reconnect until succeed
  int resume_max_count_;       // if <= 0, will use default value
  int reconnect_alert_count_;  // if <= 0, will use default value
  int resume_alert_count_;     // if <= 0, will use default value

  std::function<void()> reconnect_alerter_;
  std::function<void()> resume_global_watcher_alerter_;
  std::function<void()> resume_custom_watcher_alerter_;
  std::function<void()> resume_ephemeral_node_alerter_;

  // When global or custom watcher is triggered，InnerWatcherCbFunc will be
  // called Each watcher has unique context InnerWatcherCbFunc re-registers the
  // watcher and call global or custom watcher callback func in its context
  static void InnerWatcherCbFunc(zhandle_t *zh, int type, int state,
                                 const char *p_abs_path,
                                 void *zookeeper_context);

  static void HandleSessionEvent(ZookeeperManager &manager, int state);

  static void TimeoutReconnect(ZookeeperManager &manager);

  static bool IsValidCallBack(std::shared_ptr<WatcherFuncType> func);

  static bool IsValidWatcherContext(ZookeeperCtx *context);

  static void SetWatcherContextInvalid(ZookeeperCtx *context);

  static bool IsValidManager(zhandle_t *zh, ZookeeperManager &manager);

  static bool IsValidEvent(ZookeeperCtx *context, const std::string &abs_path,
                           int type);

  static uint32_t GetWatcherType(ZookeeperCtx *context,
                                 const std::string &abs_path = "");

  static int CustomWatcherAutoRegisterer(ZookeeperCtx *context,
                                         const char *p_abs_path, int type);

  static int GlobalWatcherAutoRegisterer(ZookeeperCtx *context,
                                         const char *p_abs_path, int type);

  static int AddExistWatcherOnce(ZookeeperCtx *context, const char *p_abs_path,
                                 bool add_to_global_watcher = true);

  static uint8_t GetStopWatcherTypeMask(ZookeeperCtx *context,
                                        const std::string &abs_path, int type);

  static void InnerVoidCompletion(int ret_code, const void *zookeeper_context);

  static void InnerStatCompletion(int ret_code, const Stat *stat,
                                  const void *zookeeper_context);

  static void InnerDataCompletion(int ret_code, const char *value,
                                  int value_len, const Stat *stat,
                                  const void *zookeeper_context);

  static void InnerStringsCompletion(int ret_code, const String_vector *strings,
                                     const void *zookeeper_context);

  static void InnerStringsStatCompletion(int ret_code,
                                         const String_vector *strings,
                                         const Stat *stat,
                                         const void *zookeeper_context);

  static void InnerStringCompletion(int ret_code, const char *value,
                                    const void *zookeeper_context);

  static void InnerAclCompletion(int ret_code, ACL_vector *acl, Stat *stat,
                                 const void *zookeeper_context);

  static void InnerMultiCompletion(int ret_code, const void *zookeeper_context);

  static std::string ZError(int err);

  static std::string CharPtrToString(const char *p);

  void AddCustomWatcher(const std::string &abs_path,
                        std::shared_ptr<ZookeeperCtx> watcher_context);

  std::shared_ptr<ZookeeperCtx> GetCustomWatcherCtx(
      const std::string &path, int type,
      const std::shared_ptr<WatcherFuncType> &watcher_func);

  void DelCustomWatcherCtx(const std::string &abs_path,
                           const ZookeeperCtx *watcher_context);

  bool ResumeEnv();

  bool ResumeGlobalWatcher();

  bool ResumeCustomWatcher();

  bool ResumeEphemeralNode();

  void ProcMultiEphemeralNode(const std::vector<zoo_op> &multi_ops,
                              const std::vector<zoo_op_result_t> &multi_result);

  void ProcAsyncWatcher(ZookeeperCtx &context);

  void DeleteWatcher(int type, const char *p_abs_path, void *zookeeper_context);

  ZookeeperManager(ZookeeperManager &&right) = delete;

  ZookeeperManager(const ZookeeperManager &right) = delete;

  ZookeeperManager &operator=(ZookeeperManager &&right) = delete;

  ZookeeperManager &operator=(const ZookeeperManager &right) = delete;
};

}  // namespace zookeeper

#endif
