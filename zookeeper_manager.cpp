#include "zookeeper_manager.h"

#include <arpa/inet.h>
#include <stdio.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cstring>

#include "zookeeper/hashtable/hashtable_private.h"
#include "zookeeper/zk_adaptor.h"

#define THREADED

// from zk_hashtable.c
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

namespace zookeeper {

void InitLogger(const std::string &path, const std::string &name,
                logger::LEVEL level, unsigned int flush_period) {
  logger::getInstance()->initLogger(path, name, level, flush_period);
  M_LOG_SPCL << "set zookeeper manager log path: " << path
             << ", log name: " << name << ", flush period: " << flush_period;

  ZooLogLevel zoo_log_level;
  switch (level) {
    case logger::LEVEL::L_DEBUG: {
      zoo_log_level = ZOO_LOG_LEVEL_DEBUG;
      break;
    }
    case logger::LEVEL::L_INFO: {
      zoo_log_level = ZOO_LOG_LEVEL_INFO;
      break;
    }
    case logger::LEVEL::L_WARN: {
      zoo_log_level = ZOO_LOG_LEVEL_WARN;
      break;
    }
    default: {
      zoo_log_level = ZOO_LOG_LEVEL_ERROR;
    }
  }

  zoo_set_debug_level(zoo_log_level);
  if (!path.empty()) {
    FILE *fp = nullptr;
    fp = fopen(path.c_str(), "a+");
    if (nullptr == fp) M_LOG_ERROR << "can not create zookeeper log: " << path;
    zoo_set_log_stream(fp);
    M_LOG_SPCL << "set zookeeper log: " << path;
  }
}

void SplitStr(string str, const vector<string> &split_str,
              vector<string> &result, bool remove_empty_elm = true,
              size_t max_count = 0) {
  result.clear();
  size_t cur_count = 0;

  size_t index = string::npos;
  size_t split_len = 0;
  size_t cur_index;
  for (vector<string>::const_iterator it = split_str.begin();
       it != split_str.end(); ++it) {
    if (it->length() == 0) {
      continue;
    }

    cur_index = str.find(*it);
    if (cur_index != string::npos && cur_index < index) {
      index = cur_index;
      split_len = it->length();
    }
  }

  while (index != string::npos) {
    if (index != 0 || !remove_empty_elm) {
      ++cur_count;
      if (max_count > 0 && cur_count >= max_count) {
        break;
      }
      result.push_back(str.substr(0, index));
    }

    str.erase(str.begin(), str.begin() + index + split_len);

    index = string::npos;
    for (vector<string>::const_iterator it = split_str.begin();
         it != split_str.end(); ++it) {
      if (it->length() == 0) {
        continue;
      }

      cur_index = str.find(*it);
      if (cur_index != string::npos && cur_index < index) {
        index = cur_index;
        split_len = it->length();
      }
    }
  }

  if (str.length() > 0 || !remove_empty_elm) {
    result.push_back(str);
  }
}

void SplitStr(string str, const string &split_str, vector<string> &result,
              bool remove_empty_elm = true, size_t max_count = 0) {
  SplitStr(str, vector<string>(1, split_str), result, remove_empty_elm,
           max_count);
}

enum GlobalWatcherType {
  WATCHER_GET = 1,          // 00000001
  WATCHER_EXISTS = 2,       // 00000010
  WATCHER_GET_CHILDREN = 4  // 00000100
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
               bool need_reg_watcher = true)
      : m_is_stop(false),
        m_zookeeper_manager(zookeeper_manager),
        m_auto_reg_watcher(need_reg_watcher),
        m_watcher_type(watcher_type),
        m_global_watcher_add_type(0) {}

  virtual ~ZookeeperCtx() {}

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

  bool m_auto_reg_watcher;
  WatcherType m_watcher_type;

  string m_ephemeral_path;
  shared_ptr<EphemeralNodeInfo> m_ephemeral_info;

  string m_watch_path;
  shared_ptr<ZookeeperCtx> m_custom_watcher_context;

  uint8_t m_global_watcher_add_type;

  shared_ptr<MultiOps> m_multi_ops;
  shared_ptr<vector<zoo_op_result_t>> m_multi_results;

 private:
  ZookeeperCtx(ZookeeperCtx &&right) = delete;
  ZookeeperCtx(const ZookeeperCtx &right) = delete;
  ZookeeperCtx &operator=(ZookeeperCtx &&right) = delete;
  ZookeeperCtx &operator=(const ZookeeperCtx &right) = delete;
};

ZookeeperManager::ZookeeperManager()
    : reconnecting_(false),
      m_dont_close_(false),
      m_zhandle_(NULL),
      m_zk_tid_(0) {
  m_zk_client_id_.client_id = 0;
}

int32_t ZookeeperManager::Init(const string &hosts,
                               const string &root_path /*= "/"*/,
                               const clientid_t *client_id /*= NULL*/) {
  m_hosts_ = hosts;
  m_root_path_ = root_path;

  if (m_root_path_.empty() || m_root_path_.at(0) != '/') {
    M_LOG_ERROR << "invalid API root path: " << m_root_path_;
    return ZBADARGUMENTS;
  }

  if (client_id != NULL) {
    m_zk_client_id_ = *client_id;
  }

  reconnect_max_count_ = INT_MAX;
  resume_max_count_ = 5;
  reconnect_alert_count_ = 3;
  resume_alert_count_ = 3;
  call_watcher_func_on_resume_ = true;

  M_LOG_SPCL << "zookeeper config: m_hosts_[" << m_hosts_ << "], m_root_path_["
             << m_root_path_ << "], client_id[" << m_zk_client_id_.client_id
             << "], default reconnect_max_count_[" << reconnect_max_count_
             << "], default resume_max_count_[" << resume_max_count_
             << "], default reconnect_alert_count_[" << reconnect_alert_count_
             << "], default resume_alert_count_[" << resume_alert_count_
             << "], default call_watcher_func_on_resume_["
             << call_watcher_func_on_resume_ << "]";

  return ZOK;
}

void ZookeeperManager::SetReconnectOptions(
    std::function<void()> reconnect_alerter, int reconnect_alert_count,
    int reconnect_max_count) {
  if (reconnect_alerter) {
    reconnect_alerter_ = reconnect_alerter;
    M_LOG_SPCL << "set user specified reconnect_alerter_ ok";
  } else {
    M_LOG_ERROR << "user specified reconnect_alerter_ is nullptr";
  }

  if (reconnect_max_count > 0)
    reconnect_max_count_ = reconnect_max_count;
  else {
    M_LOG_ERROR << "user specified reconnect_max_count_ is invalid: "
                << reconnect_max_count;
  }
  if (reconnect_alert_count > 0)
    reconnect_alert_count_ = reconnect_alert_count;
  else {
    M_LOG_ERROR << "user specified reconnect_alert_count_ is invalid: "
                << reconnect_alert_count;
  }
  M_LOG_SPCL << "set reconnect_max_count_: " << reconnect_max_count_
             << ", reconnect_alert_count_: " << reconnect_alert_count_;
}

void ZookeeperManager::SetResumeOptions(
    std::function<void()> resume_ephemeral_node_alerter,
    std::function<void()> resume_custom_watcher_alerter,
    std::function<void()> resume_global_watcher_alerter, int resume_alert_count,
    int resume_max_count) {
  if (resume_ephemeral_node_alerter) {
    resume_ephemeral_node_alerter_ = resume_ephemeral_node_alerter;
    M_LOG_SPCL << "set user specified resume_ephemeral_node_alerter_ ok";
  } else {
    M_LOG_ERROR << "user specified reconnect_alerter_ is nullptr";
  }

  if (resume_custom_watcher_alerter) {
    resume_custom_watcher_alerter_ = resume_custom_watcher_alerter;
    M_LOG_SPCL << "set user specified resume_custom_watcher_alerter_ ok";
  } else {
    M_LOG_ERROR << "user specified resume_custom_watcher_alerter_ is nullptr";
  }

  if (resume_global_watcher_alerter) {
    resume_global_watcher_alerter_ = resume_global_watcher_alerter;
    M_LOG_SPCL << "set user specified resume_global_watcher_alerter_ ok";
  } else {
    M_LOG_ERROR << "user specified resume_global_watcher_alerter_ is nullptr";
  }

  if (resume_max_count > 0)
    resume_max_count_ = resume_max_count;
  else {
    M_LOG_ERROR << "user specified resume_max_count_ is invalid: "
                << resume_max_count;
  }
  if (resume_alert_count > 0)
    resume_alert_count_ = resume_alert_count;
  else {
    M_LOG_ERROR << "user specified resume_alert_count_ is invalid: "
                << resume_alert_count;
  }
  M_LOG_SPCL << "set resume_max_count_: " << resume_max_count_
             << ", resume_alert_count_: " << resume_alert_count_;
}

void ZookeeperManager::SetCallWatcherFuncOnResume(bool flag) {
  call_watcher_func_on_resume_ = flag;
  M_LOG_SPCL << "set call_watcher_func_on_resume_: "
             << call_watcher_func_on_resume_;
}

int32_t ZookeeperManager::Connect(
    shared_ptr<WatcherFuncType> global_watcher_fun, int32_t recv_timeout_ms,
    uint32_t conn_timeout_ms /*= 3000*/) {
  m_zk_tid_ = 0;
  if (m_zhandle_ != NULL) {
    M_LOG_SPCL << "close none NULL m_zhandle_!";
    zookeeper_close(m_zhandle_);
    m_zhandle_ = NULL;
  }

  if (m_global_watcher_context_ == NULL ||
      m_global_watcher_context_->m_watcher_func != global_watcher_fun) {
    m_global_watcher_context_ =
        make_shared<ZookeeperCtx>(*this, ZookeeperCtx::GLOBAL);
    m_global_watcher_context_->m_watcher_func = global_watcher_fun;
  }

  M_LOG_SPCL << "start connecting...";
  m_zhandle_ = zookeeper_init(
      m_hosts_.c_str(), &ZookeeperManager::InnerWatcherCbFunc, recv_timeout_ms,
      m_zk_client_id_.client_id != 0 ? &m_zk_client_id_ : NULL,
      m_global_watcher_context_.get(), 0);

  if (m_zhandle_ == NULL) {
    M_LOG_ERROR << "zookeeper_init error, return NULL zhandle, host["
                << m_hosts_ << "], errno[" << errno << "], error["
                << ZError(errno) << "]";
    if (errno == ZOK) {
      return ZSYSTEMERROR;
    }
    return errno;
  }

  if (syscall(__NR_gettid) != m_zk_tid_) {
    M_LOG_SPCL << "wait for connected event...";
    unique_lock<mutex> conn_lock(m_connect_lock_);
    while (GetStatus() != ZOO_CONNECTED_STATE) {
      if (conn_timeout_ms > 0) {
        m_connect_cond_.wait_for(conn_lock,
                                 chrono::milliseconds(conn_timeout_ms));
        if (GetStatus() != ZOO_CONNECTED_STATE) {
          M_LOG_ERROR << "connect timeout!";
          return ZOPERATIONTIMEOUT;
        }
      } else {
        m_connect_cond_.wait(conn_lock);
      }
    }

    const clientid_t *p_curr_client_id = zoo_client_id(m_zhandle_);
    if (p_curr_client_id != NULL) {
      m_zk_client_id_ = *p_curr_client_id;
    }
    M_LOG_SPCL << "connected, client_id[" << m_zk_client_id_.client_id << "]";
  }

  return ZOK;
}

int32_t ZookeeperManager::Reconnect() {
  M_LOG_SPCL << "start reconnecting...";

  m_zk_client_id_.client_id = 0;
  ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(
      reinterpret_cast<const ZookeeperCtx *>(zoo_get_context(m_zhandle_)));
  if (p_context == NULL) {
    M_LOG_ERROR << "context is NULL!";
    return ZSYSTEMERROR;
  }

  return Connect(p_context->m_watcher_func, zoo_recv_timeout(m_zhandle_));
}

ZookeeperManager::~ZookeeperManager() {
  if (m_zhandle_ != NULL && !m_dont_close_) {
    zookeeper_close(m_zhandle_);
    m_zhandle_ = NULL;
  }
}

int32_t ZookeeperManager::AExists(
    const string &path, shared_ptr<StatCompletionFuncType> stat_completion_func,
    int watch /*= 0*/) {
  int32_t ret = ZOK;
  ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
  p_zookeeper_context->m_stat_completion_func = stat_completion_func;

  string abs_path = ChangeToAbsPath(path);
  if (watch != 0) {
    p_zookeeper_context->m_watch_path = abs_path;
    p_zookeeper_context->m_global_watcher_add_type = WATCHER_EXISTS;
  }

  ret =
      zoo_aexists(m_zhandle_, abs_path.c_str(), watch,
                  &ZookeeperManager::InnerStatCompletion, p_zookeeper_context);
  if (ret != ZOK) {
    M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                << ZError(ret) << "]";
    delete p_zookeeper_context;
  }

  return ret;
}

int32_t ZookeeperManager::AExists(
    const string &path, shared_ptr<StatCompletionFuncType> stat_completion_func,
    shared_ptr<WatcherFuncType> watcher_func) {
  int32_t ret = ZOK;
  ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
  p_zookeeper_context->m_stat_completion_func = stat_completion_func;

  shared_ptr<ZookeeperCtx> p_zookeeper_watcher_context =
      make_shared<ZookeeperCtx>(*this, ZookeeperCtx::EXISTS);
  p_zookeeper_watcher_context->m_watcher_func = watcher_func;

  string abs_path = ChangeToAbsPath(path);
  p_zookeeper_context->m_watch_path = abs_path;
  p_zookeeper_context->m_custom_watcher_context = p_zookeeper_watcher_context;

  ret = zoo_awexists(
      m_zhandle_, abs_path.c_str(), &ZookeeperManager::InnerWatcherCbFunc,
      p_zookeeper_watcher_context.get(), &ZookeeperManager::InnerStatCompletion,
      p_zookeeper_context);

  if (ret != ZOK) {
    M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                << ZError(ret) << "]";
    delete p_zookeeper_context;
  }

  return ret;
}

int32_t ZookeeperManager::Exists(const string &path, Stat *stat,
                                 int watch /*= 0*/) {
  string abs_path = ChangeToAbsPath(path);
  int32_t ret = zoo_exists(m_zhandle_, abs_path.c_str(), watch, stat);
  if (ret == ZOK || ret == ZNONODE) {
    if (watch != 0) {
      if ((m_global_watcher_path_type_[abs_path] & WATCHER_EXISTS) !=
          WATCHER_EXISTS) {
        std::unique_lock<std::mutex> lock(m_global_watcher_path_type_mutex_);
        m_global_watcher_path_type_[abs_path] |= WATCHER_EXISTS;
      }
    }
  } else {
    M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                << ZError(ret) << "]";
  }

  return ret;
}

int32_t ZookeeperManager::Exists(const string &path, Stat *stat,
                                 shared_ptr<WatcherFuncType> watcher_func) {
  int32_t ret = ZOK;
  string abs_path = ChangeToAbsPath(path);
  shared_ptr<ZookeeperCtx> p_zookeeper_watcher_context =
      GetCustomWatcherCtx(abs_path, ZookeeperCtx::EXISTS, watcher_func);
  ret = zoo_wexists(m_zhandle_, abs_path.c_str(),
                    &ZookeeperManager::InnerWatcherCbFunc,
                    p_zookeeper_watcher_context.get(), stat);

  if ((ret != ZOK && ret != ZNONODE) || watcher_func == nullptr ||
      watcher_func.get() == nullptr) {
    M_LOG_ERROR
        << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
        << ZError(ret)
        << "], try to delete custom watcher(context), watcher_func is nullptr: "
        << (watcher_func == nullptr) << ", watcher_func.get() is nullptr: "
        << (watcher_func.get() == nullptr);
    DelCustomWatcherCtx(abs_path, p_zookeeper_watcher_context.get());
  }

  return ret;
}

int32_t ZookeeperManager::AGet(
    const string &path, shared_ptr<DataCompletionFuncType> data_completion_func,
    int watch /*= 0*/) {
  int32_t ret = ZOK;
  ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
  p_zookeeper_context->m_data_completion_func = data_completion_func;

  string abs_path = ChangeToAbsPath(path);
  if (watch != 0) {
    p_zookeeper_context->m_watch_path = abs_path;
    p_zookeeper_context->m_global_watcher_add_type = WATCHER_GET;
  }

  ret = zoo_aget(m_zhandle_, abs_path.c_str(), watch,
                 &ZookeeperManager::InnerDataCompletion, p_zookeeper_context);

  if (ret != ZOK) {
    M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                << ZError(ret) << "]";
    delete p_zookeeper_context;
  }

  return ret;
}

int32_t ZookeeperManager::AGet(
    const string &path, shared_ptr<DataCompletionFuncType> data_completion_func,
    shared_ptr<WatcherFuncType> watcher_func) {
  int32_t ret = ZOK;
  ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
  p_zookeeper_context->m_data_completion_func = data_completion_func;

  shared_ptr<ZookeeperCtx> p_zookeeper_watcher_context =
      make_shared<ZookeeperCtx>(*this, ZookeeperCtx::GET);
  p_zookeeper_watcher_context->m_watcher_func = watcher_func;

  string abs_path = ChangeToAbsPath(path);
  p_zookeeper_context->m_watch_path = abs_path;
  p_zookeeper_context->m_custom_watcher_context = p_zookeeper_watcher_context;

  ret = zoo_awget(m_zhandle_, abs_path.c_str(),
                  &ZookeeperManager::InnerWatcherCbFunc,
                  p_zookeeper_watcher_context.get(),
                  &ZookeeperManager::InnerDataCompletion, p_zookeeper_context);
  if (ret != ZOK) {
    M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                << ZError(ret) << "]";
    delete p_zookeeper_context;
  }

  return ret;
}

int32_t ZookeeperManager::Get(const string &path, char *buffer, int *buf_len,
                              Stat *stat, int watch /*= 0*/) {
  string abs_path = ChangeToAbsPath(path);
  int32_t ret =
      zoo_get(m_zhandle_, abs_path.c_str(), watch, buffer, buf_len, stat);
  if (ret != ZOK) {
    M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                << ZError(ret) << "]";
  } else if (watch != 0) {
    if ((m_global_watcher_path_type_[abs_path] & WATCHER_GET) != WATCHER_GET) {
      std::unique_lock<std::mutex> lock(m_global_watcher_path_type_mutex_);
      m_global_watcher_path_type_[abs_path] |= WATCHER_GET;
    }
  } else {
    // Noting
  }

  return ret;
}

int32_t ZookeeperManager::Get(const string &path, char *buffer, int *buf_len,
                              Stat *stat,
                              shared_ptr<WatcherFuncType> watcher_func) {
  int32_t ret = ZOK;
  string abs_path = ChangeToAbsPath(path);
  shared_ptr<ZookeeperCtx> p_zookeeper_watcher_context =
      GetCustomWatcherCtx(abs_path, ZookeeperCtx::GET, watcher_func);

  ret = zoo_wget(m_zhandle_, abs_path.c_str(),
                 &ZookeeperManager::InnerWatcherCbFunc,
                 p_zookeeper_watcher_context.get(), buffer, buf_len, stat);

  if (ret != ZOK || watcher_func == nullptr || watcher_func.get() == nullptr) {
    M_LOG_ERROR
        << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
        << ZError(ret)
        << "], try to delete custom watcher(context), watcher_func is nullptr: "
        << (watcher_func == nullptr) << ", watcher_func.get() is nullptr: "
        << (watcher_func.get() == nullptr);
    DelCustomWatcherCtx(abs_path, p_zookeeper_watcher_context.get());
  }

  return ret;
}

int32_t ZookeeperManager::AGetChildren(
    const string &path,
    shared_ptr<StringsStatCompletionFuncType> strings_stat_completion_func,
    int watch /*= 0*/, bool need_stat /*= false*/) {
  int32_t ret = ZOK;
  ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
  p_zookeeper_context->m_strings_stat_completion_func =
      strings_stat_completion_func;
  string abs_path = ChangeToAbsPath(path);
  if (watch != 0) {
    p_zookeeper_context->m_watch_path = abs_path;
    p_zookeeper_context->m_global_watcher_add_type = WATCHER_GET_CHILDREN;
  }

  if (need_stat) {
    ret = zoo_aget_children2(m_zhandle_, abs_path.c_str(), watch,
                             &ZookeeperManager::InnerStringsStatCompletion,
                             p_zookeeper_context);

  } else {
    ret = zoo_aget_children(m_zhandle_, abs_path.c_str(), watch,
                            &ZookeeperManager::InnerStringsCompletion,
                            p_zookeeper_context);
  }

  if (ret != ZOK) {
    M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                << ZError(ret) << "]";
    delete p_zookeeper_context;
  }

  return ret;
}

int32_t ZookeeperManager::AGetChildren(
    const string &path,
    shared_ptr<StringsStatCompletionFuncType> strings_stat_completion_func,
    shared_ptr<WatcherFuncType> watcher_func, bool need_stat /*= false*/) {
  int32_t ret = ZOK;

  ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
  p_zookeeper_context->m_strings_stat_completion_func =
      strings_stat_completion_func;

  shared_ptr<ZookeeperCtx> p_zookeeper_watcher_context =
      make_shared<ZookeeperCtx>(*this, ZookeeperCtx::GET_CHILDREN);
  p_zookeeper_watcher_context->m_watcher_func = watcher_func;

  string abs_path = ChangeToAbsPath(path);
  p_zookeeper_context->m_watch_path = abs_path;
  p_zookeeper_context->m_custom_watcher_context = p_zookeeper_watcher_context;

  if (need_stat) {
    ret = zoo_awget_children2(
        m_zhandle_, abs_path.c_str(), &ZookeeperManager::InnerWatcherCbFunc,
        p_zookeeper_watcher_context.get(),
        &ZookeeperManager::InnerStringsStatCompletion, p_zookeeper_context);
  } else {
    ret = zoo_awget_children(
        m_zhandle_, abs_path.c_str(), &ZookeeperManager::InnerWatcherCbFunc,
        p_zookeeper_watcher_context.get(),
        &ZookeeperManager::InnerStringsCompletion, p_zookeeper_context);
  }

  if (ret != ZOK) {
    M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                << ZError(ret) << "]";
    delete p_zookeeper_context;
  }

  return ret;
}

int32_t ZookeeperManager::GetChildren(const string &path,
                                      ScopedStringVector &strings,
                                      int watch /*= 0*/,
                                      Stat *stat /*= NULL*/) {
  strings.Clear();
  string abs_path = ChangeToAbsPath(path);
  int32_t ret = ZOK;
  if (stat == NULL) {
    ret = zoo_get_children(m_zhandle_, abs_path.c_str(), watch, &strings);
  } else {
    ret =
        zoo_get_children2(m_zhandle_, abs_path.c_str(), watch, &strings, stat);
  }

  if (ret != ZOK) {
    M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                << ZError(ret) << "]";
  } else if (watch != 0) {
    if ((m_global_watcher_path_type_[abs_path] & WATCHER_GET_CHILDREN) !=
        WATCHER_GET_CHILDREN) {
      std::unique_lock<std::mutex> lock(m_global_watcher_path_type_mutex_);
      m_global_watcher_path_type_[abs_path] |= WATCHER_GET_CHILDREN;
    }
  } else {
    // Nothing
  }

  return ret;
}

int32_t ZookeeperManager::GetChildren(const string &path,
                                      ScopedStringVector &strings,
                                      shared_ptr<WatcherFuncType> watcher_func,
                                      Stat *stat /*= NULL*/) {
  strings.Clear();
  int32_t ret = ZOK;
  string abs_path = ChangeToAbsPath(path);
  shared_ptr<ZookeeperCtx> p_zookeeper_watcher_context =
      GetCustomWatcherCtx(abs_path, ZookeeperCtx::GET_CHILDREN, watcher_func);

  if (stat == NULL) {
    ret = zoo_wget_children(m_zhandle_, abs_path.c_str(),
                            &ZookeeperManager::InnerWatcherCbFunc,
                            p_zookeeper_watcher_context.get(), &strings);
  } else {
    ret = zoo_wget_children2(m_zhandle_, abs_path.c_str(),
                             &ZookeeperManager::InnerWatcherCbFunc,
                             p_zookeeper_watcher_context.get(), &strings, stat);
  }

  if (ret != ZOK || watcher_func == nullptr || watcher_func.get() == nullptr) {
    M_LOG_ERROR
        << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
        << ZError(ret)
        << "], try to delete custom watcher(context), watcher_func is nullptr: "
        << (watcher_func == nullptr) << ", watcher_func.get() is nullptr: "
        << (watcher_func.get() == nullptr);
    DelCustomWatcherCtx(abs_path, p_zookeeper_watcher_context.get());
  }

  return ret;
}

int32_t ZookeeperManager::ACreate(
    const string &path, const char *value, int value_len,
    shared_ptr<StringCompletionFuncType> string_completion_func,
    const ACL_vector *acl /*= &ZOO_OPEN_ACL_UNSAFE*/, int flags /*= 0*/) {
  int32_t ret = ZOK;
  ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
  string abs_path = ChangeToAbsPath(path);

  p_zookeeper_context->m_string_completion_func = string_completion_func;
  if (flags & ZOO_EPHEMERAL) {
    p_zookeeper_context->m_ephemeral_path = abs_path;
    p_zookeeper_context->m_ephemeral_info = make_shared<EphemeralNodeInfo>();
    p_zookeeper_context->m_ephemeral_info->acl = *acl;
    p_zookeeper_context->m_ephemeral_info->data.assign(value, value_len);
    p_zookeeper_context->m_ephemeral_info->flags = flags;
  }

  ret = zoo_acreate(m_zhandle_, abs_path.c_str(), value, value_len, acl, flags,
                    &ZookeeperManager::InnerStringCompletion,
                    p_zookeeper_context);

  if (ret != ZOK) {
    M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                << ZError(ret) << "]";
    delete p_zookeeper_context;
  }

  return ret;
}

int32_t ZookeeperManager::ACreate(
    const string &path, const string &value,
    shared_ptr<StringCompletionFuncType> string_completion_func,
    const ACL_vector *acl /*= &ZOO_OPEN_ACL_UNSAFE*/, int flags /*= 0*/) {
  return ACreate(path, value.data(), value.size(), string_completion_func, acl,
                 flags);
}

int32_t ZookeeperManager::Create(
    const string &path, const char *value, int value_len,
    string *p_real_path /*= NULL*/,
    const ACL_vector *acl /*= &ZOO_OPEN_ACL_UNSAFE*/, int flags /*= 0*/,
    bool ephemeral_exist_skip /*= false*/) {
  string abs_path = ChangeToAbsPath(path);
  int32_t ret;
  string exist_value;
  string exist_path;
  Stat exist_stat;
  bzero(&exist_stat, sizeof(exist_stat));

  if (ephemeral_exist_skip && (flags & ZOO_EPHEMERAL)) {
    exist_value.resize(value_len);
    if (flags & ZOO_SEQUENCE) {
      size_t index = abs_path.rfind('/');
      if (index == string::npos) {
        M_LOG_ERROR << "can not get parent path of: " << abs_path;
        return ZBADARGUMENTS;
      }

      string parent_path = abs_path.substr(0, index);
      string node_name = abs_path.substr(index + 1);

      ScopedStringVector children;
      ret = GetChildren(parent_path, children);
      if (ret != ZOK) {
        M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret
                    << "], zerror[" << ZError(ret) << "]";
        return ret;
      }

      static const uint32_t SEQUENCE_LEN = 10;
      list<string> match_children;
      for (int32_t ci = 0; ci < children.count; ++ci) {
        char *child = children.GetData(ci);
        uint32_t children_len = strlen(child);

        if (node_name.size() + SEQUENCE_LEN != children_len) {
          continue;
        }

        if (memcmp(child, node_name.c_str(), node_name.size()) != 0) {
          continue;
        }

        uint32_t i = node_name.size();
        for (; i < node_name.size() + SEQUENCE_LEN; ++i) {
          if (!isdigit(child[i])) {
            break;
          }
        }

        if (i == node_name.size() + SEQUENCE_LEN) {
          match_children.push_back(child);
        }
      }

      for (auto child_it = match_children.begin();
           child_it != match_children.end(); ++child_it) {
        int buf_len = value_len;
        string child_path = parent_path + "/" + *child_it;
        ret = Get(child_path, const_cast<char *>(exist_value.data()), &buf_len,
                  &exist_stat);
        if (ret != ZOK) {
          M_LOG_ERROR << "child_path[" << child_path << "], ret[" << ret
                      << "], zerror[" << ZError(ret) << "]";
          return ret;
        }

        if (exist_stat.ephemeralOwner == m_zk_client_id_.client_id) {
          exist_path = child_path;
          break;
        }
      }
    } else {
      char child_buf[1];
      int buf_len = sizeof(child_buf);
      ret = Get(path, child_buf, &buf_len, &exist_stat);
      if (ret != ZOK) {
        M_LOG_ERROR << "child_path[" << path << "], ret[" << ret << "], zerror["
                    << ZError(ret) << "]";
        return ret;
      }

      if (exist_stat.ephemeralOwner == m_zk_client_id_.client_id) {
        exist_path = path;
      }
    }
  }

  if (!exist_path.empty()) {
    if (p_real_path != NULL) {
      strncpy(const_cast<char *>(p_real_path->data()), exist_path.c_str(),
              p_real_path->size());
    }

    if (memcmp(exist_value.c_str(), value, value_len) != 0) {
      ret = Set(exist_path, value, value_len, exist_stat.version);
      if (ret != ZOK) {
        M_LOG_ERROR << "exist_path[" << exist_path << "], ret[" << ret
                    << "], zerror[" << ZError(ret) << "]";
        return ret;
      }
    }
  } else {
    ret = zoo_create(
        m_zhandle_, abs_path.c_str(), value, value_len, acl, flags,
        p_real_path != NULL ? const_cast<char *>(p_real_path->data()) : NULL,
        p_real_path != NULL ? p_real_path->size() : 0);

    if (ret != ZOK) {
      M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                  << ZError(ret) << "]";
      return ret;
    }
  }

  if (flags & ZOO_EPHEMERAL) {
    std::unique_lock<std::mutex> ephemeral_node_info_lock(
        m_ephemeral_node_info_lock_);
    if (m_ephemeral_node_info_.count(abs_path) == 0) {
      m_ephemeral_node_info_[abs_path].acl = *acl;
      m_ephemeral_node_info_[abs_path].data.assign(value, value_len);
      m_ephemeral_node_info_[abs_path].flags = flags;
      M_LOG_SPCL << "add ephemeral node info: " << abs_path
                 << ", ephemeral node number: "
                 << m_ephemeral_node_info_.size();
    }
  }

  return ZOK;
}

int32_t ZookeeperManager::Create(
    const string &path, const string &value, string *p_real_path /*= NULL*/,
    const ACL_vector *acl /*= &ZOO_OPEN_ACL_UNSAFE*/, int flags /*= 0*/,
    bool ephemeral_exist_skip /*= false*/) {
  return Create(path, value.data(), value.size(), p_real_path, acl, flags,
                ephemeral_exist_skip);
}

int32_t ZookeeperManager::CreateRecursively(
    const std::string &path, const std::string &value,
    std::string *p_real_path /*= NULL*/,
    const ACL_vector *acl /*= &ZOO_OPEN_ACL_UNSAFE*/, int flags /*= 0*/,
    bool ephemeral_exist_skip /*= false*/) {
  std::string zk_path = path;
  if (zk_path.front() != '/') zk_path = "/" + zk_path;
  while (!zk_path.empty() && zk_path.back() == '/') zk_path.pop_back();
  if (zk_path.size() > 1) {
    size_t parent_node_end_index = zk_path.find_last_of('/');
    if (parent_node_end_index != std::string::npos) {
      std::string parent_node_path = zk_path.substr(0, parent_node_end_index);
      int ret = CreatePathRecursion(parent_node_path);
      if (ZOK == ret) {
        M_LOG_SPCL << "create parent node ok: path[" << parent_node_path
                   << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
      } else {
        M_LOG_ERROR << "create parent node error: path[" << parent_node_path
                    << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
      }
    }

    int ret =
        Create(zk_path, value, p_real_path, acl, flags, ephemeral_exist_skip);
    if (ZOK == ret) {
      M_LOG_SPCL << "create node ok: path[" << zk_path << "], ret[" << ret
                 << "], zerror[" << ZError(ret) << "]";
    } else if (ZNODEEXISTS == ret) {
      M_LOG_WARN << "create node warning: path[" << zk_path << "], ret[" << ret
                 << "], zerror[" << ZError(ret) << "]";
    } else {
      M_LOG_ERROR << "create node error: path[" << zk_path << "], ret[" << ret
                  << "], zerror[" << ZError(ret) << "]";
    }

    return ret;
  } else {
    M_LOG_ERROR << "illegal path: " << path;
    return ILLEGAL_PATH;
  }
}

int32_t ZookeeperManager::ASet(
    const string &path, const char *buffer, int buf_len, int version,
    shared_ptr<StatCompletionFuncType> stat_completion_func) {
  int32_t ret = ZOK;
  ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
  p_zookeeper_context->m_stat_completion_func = stat_completion_func;
  string abs_path = ChangeToAbsPath(path);

  {
    std::unique_lock<std::mutex> ephemeral_node_info_lock(
        m_ephemeral_node_info_lock_);
    auto ephemeral_it = m_ephemeral_node_info_.find(abs_path);
    if (ephemeral_it != m_ephemeral_node_info_.end()) {
      p_zookeeper_context->m_ephemeral_path = abs_path;
      p_zookeeper_context->m_ephemeral_info = make_shared<EphemeralNodeInfo>();
      *p_zookeeper_context->m_ephemeral_info = ephemeral_it->second;
      p_zookeeper_context->m_ephemeral_info->data.assign(buffer, buf_len);
    }
  }

  ret = zoo_aset(m_zhandle_, abs_path.c_str(), buffer, buf_len, version,
                 &ZookeeperManager::InnerStatCompletion, p_zookeeper_context);

  if (ret != ZOK) {
    M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                << ZError(ret) << "]";
    delete p_zookeeper_context;
  }

  return ret;
}

int32_t ZookeeperManager::ASet(
    const string &path, const string &buffer, int version,
    shared_ptr<StatCompletionFuncType> stat_completion_func) {
  return ASet(path, buffer.data(), buffer.size(), version,
              stat_completion_func);
}

int32_t ZookeeperManager::Set(const string &path, const char *buffer,
                              int buf_len, int version, Stat *stat /*= NULL*/) {
  int32_t ret = ZOK;
  string abs_path = ChangeToAbsPath(path);
  if (stat == NULL) {
    ret = zoo_set(m_zhandle_, abs_path.c_str(), buffer, buf_len, version);
  } else {
    ret =
        zoo_set2(m_zhandle_, abs_path.c_str(), buffer, buf_len, version, stat);
  }

  if (ret != ZOK) {
    M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                << ZError(ret) << "]";
  } else {
    std::unique_lock<std::mutex> ephemeral_node_info_lock(
        m_ephemeral_node_info_lock_);
    auto ephemeral_it = m_ephemeral_node_info_.find(abs_path);
    if (ephemeral_it != m_ephemeral_node_info_.end()) {
      ephemeral_it->second.data.assign(buffer, buf_len);
    }
  }

  return ret;
}

int32_t ZookeeperManager::Set(const string &path, const string &buffer,
                              int version, Stat *stat /*= NULL*/) {
  return Set(path, buffer.data(), buffer.size(), version, stat);
}

int32_t ZookeeperManager::ADelete(
    const string &path, int version,
    shared_ptr<VoidCompletionFuncType> void_completion_fun) {
  int32_t ret = ZOK;
  ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
  p_zookeeper_context->m_void_completion_func = void_completion_fun;
  string abs_path = ChangeToAbsPath(path);

  {
    std::unique_lock<std::mutex> ephemeral_node_info_lock(
        m_ephemeral_node_info_lock_);
    auto ephemeral_it = m_ephemeral_node_info_.find(abs_path);
    if (ephemeral_it != m_ephemeral_node_info_.end()) {
      p_zookeeper_context->m_ephemeral_path = abs_path;
    }
  }

  ret =
      zoo_adelete(m_zhandle_, abs_path.c_str(), version,
                  &ZookeeperManager::InnerVoidCompletion, p_zookeeper_context);

  if (ret != ZOK) {
    M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                << ZError(ret) << "]";
    delete p_zookeeper_context;
  }

  return ret;
}

int32_t ZookeeperManager::Delete(const string &path, int version /*= -1*/) {
  string abs_path = ChangeToAbsPath(path);
  int32_t ret = zoo_delete(m_zhandle_, abs_path.c_str(), version);
  if (ret != ZOK) {
    M_LOG_ERROR << "delete failed, abs_path[" << abs_path << "], ret[" << ret
                << "], zerror[" << ZError(ret) << "]";
  } else {
    M_LOG_SPCL << "delete succeed, abs_path[" << abs_path << "], ret[" << ret
               << "], zerror[" << ZError(ret) << "]";
    std::unique_lock<std::mutex> ephemeral_node_info_lock(
        m_ephemeral_node_info_lock_);
    if (m_ephemeral_node_info_.find(abs_path) != m_ephemeral_node_info_.end()) {
      m_ephemeral_node_info_.erase(abs_path);
      M_LOG_SPCL << "delete ephemeral node info: " << abs_path
                 << ", ephemeral node number: "
                 << m_ephemeral_node_info_.size();
    }
  }

  return ret;
}

int32_t ZookeeperManager::AGetAcl(
    const string &path, shared_ptr<AclCompletionFuncType> acl_completion_func) {
  int32_t ret = ZOK;
  ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
  p_zookeeper_context->m_acl_completion_func = acl_completion_func;
  string abs_path = ChangeToAbsPath(path);
  ret =
      zoo_aget_acl(m_zhandle_, abs_path.c_str(),
                   &ZookeeperManager::InnerAclCompletion, p_zookeeper_context);

  if (ret != ZOK) {
    M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                << ZError(ret) << "]";
    delete p_zookeeper_context;
  }

  return ret;
}

int32_t ZookeeperManager::GetAcl(const string &path, ScopedAclVector &acl,
                                 Stat *stat) {
  acl.Clear();
  string abs_path = ChangeToAbsPath(path);
  int32_t ret = zoo_get_acl(m_zhandle_, abs_path.c_str(), &acl, stat);
  if (ret != ZOK) {
    M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                << ZError(ret) << "]";
  }

  return ret;
}

int32_t ZookeeperManager::ASetAcl(
    const string &path, int version, ACL_vector *acl,
    shared_ptr<VoidCompletionFuncType> void_completion_fun) {
  int32_t ret = ZOK;
  ZookeeperCtx *p_zookeeper_context = new ZookeeperCtx(*this);
  p_zookeeper_context->m_void_completion_func = void_completion_fun;
  string abs_path = ChangeToAbsPath(path);

  {
    std::unique_lock<std::mutex> ephemeral_node_info_lock(
        m_ephemeral_node_info_lock_);
    auto ephemeral_it = m_ephemeral_node_info_.find(abs_path);
    if (ephemeral_it != m_ephemeral_node_info_.end()) {
      p_zookeeper_context->m_ephemeral_path = abs_path;
      p_zookeeper_context->m_ephemeral_info = make_shared<EphemeralNodeInfo>();
      *p_zookeeper_context->m_ephemeral_info = ephemeral_it->second;
      p_zookeeper_context->m_ephemeral_info->acl = *acl;
    }
  }

  ret =
      zoo_aset_acl(m_zhandle_, abs_path.c_str(), version, acl,
                   &ZookeeperManager::InnerVoidCompletion, p_zookeeper_context);

  if (ret != ZOK) {
    M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                << ZError(ret) << "]";
    delete p_zookeeper_context;
  }

  return ret;
}

int32_t ZookeeperManager::SetAcl(const string &path, int version,
                                 ACL_vector *acl) {
  string abs_path = ChangeToAbsPath(path);
  int32_t ret = zoo_set_acl(m_zhandle_, abs_path.c_str(), version, acl);
  if (ret != ZOK) {
    M_LOG_ERROR << "abs_path[" << abs_path << "], ret[" << ret << "], zerror["
                << ZError(ret) << "]";
  } else {
    std::unique_lock<std::mutex> ephemeral_node_info_lock(
        m_ephemeral_node_info_lock_);
    auto ephemeral_it = m_ephemeral_node_info_.find(abs_path);
    if (ephemeral_it != m_ephemeral_node_info_.end()) {
      ephemeral_it->second.acl = *acl;
    }
  }

  return ret;
}

MultiOps ZookeeperManager::CreateMultiOps() { return MultiOps(this); }

int32_t ZookeeperManager::AMulti(
    shared_ptr<MultiOps> &multi_ops,
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
  ret = zoo_amulti(
      m_zhandle_, multi_ops->m_multi_ops.size(), &multi_ops->m_multi_ops[0],
      &(*p_zookeeper_context->m_multi_results)[0],
      &ZookeeperManager::InnerMultiCompletion, p_zookeeper_context);

  if (ret != ZOK) {
    M_LOG_ERROR << "multiple operations count[" << multi_ops->m_multi_ops.size()
                << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
    delete p_zookeeper_context;
  }

  return ret;
}

int32_t ZookeeperManager::Multi(MultiOps &multi_ops,
                                vector<zoo_op_result_t> &results) {
  results.clear();
  results.resize(multi_ops.m_multi_ops.size());

  if (multi_ops.m_multi_ops.empty()) {
    M_LOG_WARN << "multiple operations count is 0!";
    return ZBADARGUMENTS;
  }

  int32_t ret = zoo_multi(m_zhandle_, multi_ops.m_multi_ops.size(),
                          &multi_ops.m_multi_ops[0], &results[0]);
  if (ret != ZOK) {
    M_LOG_ERROR << "multiple operations count[" << multi_ops.m_multi_ops.size()
                << "], ret[" << ret << "], zerror[" << ZError(ret) << "]";
  }

  ProcMultiEphemeralNode(multi_ops.m_multi_ops, results);

  return ret;
}

const string ZookeeperManager::ChangeToAbsPath(const string &path) {
  if (path.empty()) {
    return m_root_path_;
  }

  if (path.at(0) == '/') {
    return path;
  }

  if (!m_root_path_.empty() && m_root_path_.back() == '/') {
    return m_root_path_ + path;
  }

  return m_root_path_ + "/" + path;
}

int32_t ZookeeperManager::CreatePathRecursion(const string &path) {
  int32_t ret;

  string abs_path = ChangeToAbsPath(path);
  vector<string> dirs;
  SplitStr(abs_path, "/", dirs);
  string curr_path;

  MultiOps multi_check_ops(this);
  for (auto dir_it = dirs.begin(); dir_it != dirs.end(); ++dir_it) {
    curr_path += string("/") + *dir_it;
    multi_check_ops.AddCheckOp(curr_path, -1);
  }

  vector<zoo_op_result_t> results;
  ret = Multi(multi_check_ops, results);
  if (ret == ZNONODE) {
    MultiOps multi_create_ops(this);
    bool start_no_node = false;

    auto check_op_it = multi_check_ops.m_multi_ops.begin();
    for (auto result_it = results.begin();
         result_it != results.end() &&
         check_op_it != multi_check_ops.m_multi_ops.end();
         ++result_it, ++check_op_it) {
      if (result_it->err == ZOK && !start_no_node) {
        continue;
      }

      start_no_node = true;

      multi_create_ops.AddCreateOp(check_op_it->check_op.path, "");
    }

    ret = Multi(multi_create_ops, results);
    if (ret != ZOK) {
      M_LOG_ERROR << "multiple creation failed, ret[" << ret << "], zerror["
                  << ZError(ret) << "]";
      return ret;
    }
  }

  return ret;
}

int32_t ZookeeperManager::DeletePathRecursion(const string &path) {
  list<string> path_to_get_children;
  list<string> path_to_delete;
  string abs_path = ChangeToAbsPath(path);
  path_to_get_children.push_back(abs_path);

  ScopedStringVector children;
  int32_t ret;
  while (!path_to_get_children.empty()) {
    auto curr_path = move(*path_to_get_children.rbegin());
    path_to_get_children.pop_back();
    children.Clear();
    ret = GetChildren(curr_path.c_str(), children);
    if (ret != ZOK && ret != ZNONODE) {
      M_LOG_ERROR << "delete node[" << path << "] recursively, abs_path["
                  << abs_path << "] get sub node[" << curr_path << "] failed";
    }

    if (ret == ZNONODE) {
      continue;
    }

    for (int32_t i = 0; i < children.count; ++i) {
      path_to_get_children.push_back(curr_path + "/" + children.data[i]);
    }

    M_LOG_SPCL << "add node to delete stack: " << curr_path;
    path_to_delete.push_back(move(curr_path));
  }

  if (!path_to_delete.empty()) {
    MultiOps multi_delete_ops(this);
    for (auto path_it = path_to_delete.rbegin();
         path_it != path_to_delete.rend(); ++path_it) {
      multi_delete_ops.AddDeleteOp(*path_it, -1);
    }

    vector<zoo_op_result_t> results;
    ret = Multi(multi_delete_ops, results);
    if (ret != ZOK) {
      M_LOG_ERROR << "delete node[" << path << "] recursively, abs_path["
                  << abs_path << "] failed, ret[" << ret << "], zerror["
                  << ZError(ret) << "]";
      return ret;
    } else {
      M_LOG_SPCL << "delete node[" << path << "] recursively, abs_path["
                 << abs_path << "] succeed, ret[" << ret << "], zerror["
                 << ZError(ret) << "]";
    }
  }

  std::unique_lock<std::mutex> ephemeral_node_info_lock(
      m_ephemeral_node_info_lock_);

  string pre_path = abs_path + "/";
  for (auto node_it = m_ephemeral_node_info_.begin();
       node_it != m_ephemeral_node_info_.end();) {
    if (node_it->first.find(pre_path) == 0) {
      m_ephemeral_node_info_.erase(node_it++);
      M_LOG_SPCL
          << "deletion perhaps failed, but still delete ephemeral node info: "
          << node_it->first
          << ", ephemeral node number: " << m_ephemeral_node_info_.size();
    } else {
      ++node_it;
    }
  }

  if (m_ephemeral_node_info_.count(abs_path) > 0) {
    m_ephemeral_node_info_.erase(abs_path);
    M_LOG_SPCL
        << "deletion perhaps failed, but still delete ephemeral node info: "
        << abs_path
        << ", ephemeral node number: " << m_ephemeral_node_info_.size();
  }

  return ZOK;
}

int32_t ZookeeperManager::GetChildrenValue(
    const string &path, map<string, ValueStat> &children_value,
    uint32_t max_value_size /*= 2048*/) {
  ScopedStringVector children;
  string abs_path = ChangeToAbsPath(path);
  int32_t ret = GetChildren(abs_path, children);
  if (ret != ZOK) {
    M_LOG_ERROR << "GetChildren[" << abs_path << "], ret[" << ret
                << "], zerror[" << ZError(ret) << "]";
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
      M_LOG_ERROR << "Get[" << child_path << "], ret[" << ret << "], zerror["
                  << ZError(ret) << "]";
      return ret;
    }

    value_stat.value.resize(value_len);
  }

  return ZOK;
}

int32_t ZookeeperManager::GetCString(const string &path, string &data,
                                     Stat *stat /*= NULL*/, int watch /*= 0*/) {
  int data_len = data.size() - 1;
  int32_t ret =
      Get(path, const_cast<char *>(data.data()), &data_len, stat, watch);
  if (ret == ZOK && data_len < static_cast<int32_t>(data.size())) {
    data[data_len] = '\0';
  }

  return ret;
}

int32_t ZookeeperManager::GetCString(const string &path, string &data,
                                     Stat *stat,
                                     shared_ptr<WatcherFuncType> watcher_func) {
  int data_len = data.size() - 1;
  int32_t ret =
      Get(path, const_cast<char *>(data.data()), &data_len, stat, watcher_func);
  if (ret == ZOK && data_len < static_cast<int32_t>(data.size())) {
    data[data_len] = '\0';
  }

  return ret;
}

std::map<std::string, uint8_t> ZookeeperManager::GetGlobalWatcherPathType() {
  std::unique_lock<std::mutex> lock(m_global_watcher_path_type_mutex_);
  return m_global_watcher_path_type_;
};

std::map<std::string, EphemeralNodeInfo>
ZookeeperManager::GetEphemeralNodeInfo() {
  std::unique_lock<std::mutex> lock(m_ephemeral_node_info_lock_);
  return m_ephemeral_node_info_;
};

std::multimap<std::string, std::shared_ptr<ZookeeperCtx>>
ZookeeperManager::GetCustomWatcherContexts() {
  std::unique_lock<std::mutex> lock(m_custom_watcher_contexts_mutex_);
  return m_custom_watcher_contexts_;
};

void ZookeeperManager::DeleteWatcher(int type, const char *p_abs_path,
                                     void *p_zookeeper_context) {
  if (GetHandler() == NULL) {
    return;
  }

  list<hashtable *> hashtables_to_search;

#define ADD_WATCHER_HASHTABLE(watchers)             \
  if ((watchers) != NULL && (watchers)->ht != NULL) \
  hashtables_to_search.push_back((watchers)->ht)

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
    // Nothing
  }

#undef ADD_WATCHER_HASHTABLE

  list<void *> to_free;
  for (auto hashtable_it = hashtables_to_search.begin();
       hashtable_it != hashtables_to_search.end(); ++hashtable_it) {
    watcher_object_list_t *wl = (watcher_object_list_t *)hashtable_search(
        *hashtable_it, (void *)p_abs_path);
    if (wl == NULL) {
      continue;
    }

    watcher_object_t *p_watcher = wl->head;
    watcher_object_t *p_last = p_watcher;
    while (p_watcher != NULL) {
      if (p_watcher->context == p_zookeeper_context) {
        if (p_watcher == wl->head) {
          wl->head = p_watcher->next;
        } else {
          p_last->next = p_watcher->next;
        }

        to_free.push_back(p_watcher);
      } else {
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
                                          const char *p_abs_path,
                                          void *p_zookeeper_context) {
  std::string abs_path;
  if (p_abs_path) {
    abs_path = std::string(p_abs_path);
  }

  // todo print type string and state string
  M_LOG_DEBUG << "call InnerWatcherCbFunc, type[" << type << "], state["
              << state << "], abs_path[" << abs_path << "]";

  // p_zookeeper_context must not be a dangling pointer
  ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(
      reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
  if (!IsValidWatcherContext(p_context)) {
    M_LOG_ERROR << "invalid p_zookeeper_context, "
                << "type[" << type << "], state[" << state << "], abs_path["
                << abs_path << "], skip";
    return;
  }

  ZookeeperManager &manager = p_context->m_zookeeper_manager;
  if (manager.m_zk_tid_ == 0) {
    manager.m_zk_tid_ = syscall(__NR_gettid);
  }
  if (!IsValidManager(zh, manager)) {
    M_LOG_ERROR << "invalid p_context->m_zookeeper_manager, "
                << "type[" << type << "], state[" << state << "], abs_path["
                << abs_path << "], client_id["
                << manager.GetClientID()->client_id << "], skip";
    return;
  }

  if (!IsValidEvent(p_context, abs_path, type)) {
    M_LOG_ERROR << "invalid event, watcher type["
                << GetWatcherType(p_context, abs_path) << "], "
                << "type[" << type << "], state[" << state << "], abs_path["
                << abs_path << "], client_id["
                << manager.GetClientID()->client_id << "], skip";
    return;
  }

  if (type == ZOO_SESSION_EVENT) {
    if (p_context->m_watcher_type == ZookeeperCtx::GLOBAL) {
      M_LOG_SPCL << "trigger watcher type[" << p_context->m_watcher_type
                 << "], "
                 << "type[" << type << "], state[" << state << "], abs_path["
                 << abs_path << "], client_id["
                 << manager.GetClientID()->client_id << "], continue";
    } else {  // so user custom watcher will not be triggered by
              // ZOO_SESSION_EVENT
      M_LOG_WARN << "trigger watcher type[" << p_context->m_watcher_type
                 << "], "
                 << "type[" << type << "], state[" << state << "], abs_path["
                 << abs_path << "], client_id["
                 << manager.GetClientID()->client_id << "], skip";
      return;
    }

    HandleSessionEvent(manager, state);
    // if call_watcher_func_on_resume_ is true, all user watcher function will
    // be called when resume success
    if (state == ZOO_EXPIRED_SESSION_STATE) return;
  }

  if (!IsValidCallBack(p_context->m_watcher_func)) {
    M_LOG_WARN << "invalid p_context->m_watcher_func, "
               << "type[" << type << "], state[" << state << "], abs_path["
               << abs_path << "], client_id["
               << manager.GetClientID()->client_id << "], skip";
    return;
  }

  if (!p_context->m_auto_reg_watcher || p_abs_path == NULL ||
      *p_abs_path == '\0') {
    static_cast<void>(
        (*p_context->m_watcher_func)(manager, type, state, p_abs_path));
    return;
  }

  if ((*p_context->m_watcher_func)(manager, type, state, p_abs_path)) {
    if (p_context->m_watcher_type == ZookeeperCtx::GLOBAL) {
      std::unique_lock<std::mutex> lock(
          manager.m_global_watcher_path_type_mutex_);
      auto it = manager.m_global_watcher_path_type_.find(abs_path);
      if (it != manager.m_global_watcher_path_type_.end()) {
        uint8_t stop_watcher_type_mask =
            GetStopWatcherTypeMask(p_context, abs_path, type);
        if (stop_watcher_type_mask != UINT8_MAX) {
          it->second &= stop_watcher_type_mask;
          if (it->second == 0) {
            M_LOG_WARN << "delete global watcher(type), "
                       << "type[" << type << "], state[" << state
                       << "], abs_path[" << abs_path << "], client_id["
                       << manager.GetClientID()->client_id << "]";
            ;
            manager.m_global_watcher_path_type_.erase(it);
          }
        }
      }
    } else {
      M_LOG_WARN << "set custom watcher(context) invalid, path[" << abs_path
                 << "], origin watcher type[" << p_context->m_watcher_type
                 << "], "
                 << "type[" << type << "], state[" << state << "], abs_path["
                 << abs_path << "], client_id["
                 << manager.GetClientID()->client_id << "]";
      SetWatcherContextInvalid(p_context);

      manager.DeleteWatcher(type, p_abs_path, p_context);
    }
  } else {
    if (p_context->m_watcher_type == ZookeeperCtx::GLOBAL) {
      if (NO_TYPE_IN_PATH ==
          GlobalWatcherAutoRegisterer(p_context, p_abs_path, type)) {
        M_LOG_ERROR << "no global watcher type[" << type << "] of path[ "
                    << abs_path << "], "
                    << "state[" << state << "], client_id["
                    << manager.GetClientID()->client_id << "], skip";
      }
    } else {
      CustomWatcherAutoRegisterer(p_context, p_abs_path, type);
    }
  }
}

void ZookeeperManager::HandleSessionEvent(ZookeeperManager &manager,
                                          int state) {
  if (state == ZOO_CONNECTED_STATE) {
    M_LOG_SPCL << "trigger connected event, if reconnected, need to resume "
                  "environment, client_id: "
               << manager.GetClientID()->client_id;
    manager.m_connect_cond_.notify_all();
  } else if (state == ZOO_EXPIRED_SESSION_STATE) {
    M_LOG_SPCL
        << "trigger session expired event, need to reconnect, client_id: "
        << manager.GetClientID()->client_id;
    std::thread reconnectExecutor =
        std::thread(&ZookeeperManager::TimeoutReconnect, std::ref(manager));
    reconnectExecutor.detach();
  } else {
    M_LOG_WARN << "trigger session event, current state: " << state
               << ", client_id: " << manager.GetClientID()->client_id;
  }
}

void ZookeeperManager::TimeoutReconnect(ZookeeperManager &manager) {
  bool expected_v = false;
  if (!manager.reconnecting_.compare_exchange_strong(expected_v, true)) {
    M_LOG_ERROR << "call reconnect but reconnecting_ is true!";
    return;
  }
  if (manager.reconnecting_ != true) {
    M_LOG_ERROR << "do reconnect but reconnecting_ is false";
    return;
  }
  int ret = ZOK;
  int period = 1;
  for (int reconnect_count = 1; reconnect_count <= manager.reconnect_max_count_;
       ++reconnect_count) {
    ret = manager.Reconnect();
    if (reconnect_count >= manager.reconnect_alert_count_ &&
        manager.reconnect_alerter_ != NULL) {
      manager.reconnect_alerter_();
    }
    if (ret != ZOK) {
      M_LOG_WARN << "failed on " << reconnect_count << " reconnection, ret["
                 << ret << "], zerror[" << ZError(ret) << "], will retry after "
                 << period << "s";
      sleep(period);
      if (period < 30) ++period;
      continue;
    }
    M_LOG_SPCL << "succeeded on " << reconnect_count
               << " reconnection, now start to resume environment...";
    if (!manager.ResumeEnv()) {
      M_LOG_ERROR << "resume environment faild!";
      continue;
    }
    manager.reconnecting_ = false;
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
    M_LOG_WARN << "stop custom watcher, origin watcher type: "
               << p_context->m_watcher_type;
  } else {
    M_LOG_ERROR << "can not set invalid context";
  }
}

bool ZookeeperManager::IsValidEvent(ZookeeperCtx *p_context,
                                    const std::string &abs_path, int type) {
  if (p_context->m_is_stop) {
    M_LOG_ERROR << "stopped watcher of path: " << abs_path << ", watcher_type["
                << p_context->m_watcher_type << "]";
    return false;
  }
  uint32_t watcher_type = GetWatcherType(p_context, abs_path);
  if (watcher_type == DEFAULT_TYPE_IN_PATH) {
    M_LOG_DEBUG << "default global watcher type";
    return true;
  } else if (watcher_type == NO_TYPE_IN_PATH) {
    M_LOG_ERROR << "no global watcher type of path: " << abs_path;
    return false;
  } else if (watcher_type == ZookeeperCtx::GLOBAL) {
    if (type == ZOO_CHANGED_EVENT) {
      return (((watcher_type & WATCHER_GET) == WATCHER_GET) ||
              ((watcher_type & WATCHER_EXISTS) == WATCHER_EXISTS));
    } else if (type == ZOO_CREATED_EVENT || type == ZOO_DELETED_EVENT) {
      return (((watcher_type & WATCHER_GET) == WATCHER_GET) ||
              ((watcher_type & WATCHER_EXISTS) == WATCHER_EXISTS) ||
              ((watcher_type & WATCHER_GET_CHILDREN) == WATCHER_GET_CHILDREN));
    } else if (type == ZOO_CHILD_EVENT) {
      return ((watcher_type & WATCHER_GET_CHILDREN) == WATCHER_GET_CHILDREN);
    } else {
      M_LOG_DEBUG << "return true when handle other event in global watcher";
      return true;
    }
  } else if (watcher_type == ZookeeperCtx::NOT_WATCH) {
    M_LOG_ERROR << "NOT_WATCH watcher of path: " << abs_path;
    return false;
  } else {
    if (type == ZOO_CHANGED_EVENT) {
      return ((watcher_type == ZookeeperCtx::GET) ||
              (watcher_type == ZookeeperCtx::EXISTS));
    } else if (type == ZOO_CREATED_EVENT || type == ZOO_DELETED_EVENT) {
      return ((watcher_type == ZookeeperCtx::GET) ||
              (watcher_type == ZookeeperCtx::EXISTS) ||
              (watcher_type == ZookeeperCtx::GET_CHILDREN));
    } else if (type == ZOO_CHILD_EVENT) {
      return (watcher_type == ZookeeperCtx::GET_CHILDREN);
    } else {
      M_LOG_DEBUG << "return false when handle other event in custom watcher";
      return false;
    }
  }
}

uint32_t ZookeeperManager::GetWatcherType(ZookeeperCtx *p_context,
                                          const std::string &abs_path) {
  if (p_context->m_watcher_type != ZookeeperCtx::GLOBAL) {
    return p_context->m_watcher_type;
  }
  if (abs_path.empty()) {
    return DEFAULT_TYPE_IN_PATH;
  }
  ZookeeperManager &manager = p_context->m_zookeeper_manager;
  std::unique_lock<std::mutex> lock(manager.m_global_watcher_path_type_mutex_);
  auto it = manager.m_global_watcher_path_type_.find(abs_path);
  if (it == manager.m_global_watcher_path_type_.end()) {
    M_LOG_ERROR << "can not find global watcher type of path: " << abs_path;
    return NO_TYPE_IN_PATH;
  }
  return it->second;
}

bool ZookeeperManager::IsValidManager(zhandle_t *zh,
                                      ZookeeperManager &manager) {
  if (zh != manager.m_zhandle_) {
    M_LOG_ERROR << "current zhandle[" << zh
                << "] different from local m_zhandle_!";
    return false;
  }
  return true;
}

int ZookeeperManager::CustomWatcherAutoRegisterer(ZookeeperCtx *p_context,
                                                  const char *p_abs_path,
                                                  int type) {
  M_LOG_DEBUG << "debug";
  std::string abs_path;
  if (p_abs_path) {
    abs_path = std::string(p_abs_path);
  }
  ZookeeperManager &manager = p_context->m_zookeeper_manager;
  int ret = ZOK;
  if (p_context->m_watcher_type == ZookeeperCtx::GLOBAL) return ret;

  if (p_context->m_watcher_type == ZookeeperCtx::EXISTS) {
    if (type == ZOO_CHANGED_EVENT || type == ZOO_CREATED_EVENT ||
        type == ZOO_DELETED_EVENT) {
      ret = zoo_wexists(manager.GetHandler(), p_abs_path,
                        &ZookeeperManager::InnerWatcherCbFunc, p_context, NULL);
      if (ret != ZOK && ret != ZNONODE) {
        M_LOG_ERROR << "ret[" << ret << "], zerror[" << ZError(ret)
                    << "], abs_path: " << abs_path;
      }
    } else {
      M_LOG_ERROR << "mismatched, event type: " << type
                  << ", custom watcher type: " << p_context->m_watcher_type;
    }
  } else if (p_context->m_watcher_type == ZookeeperCtx::GET) {
    if (type == ZOO_DELETED_EVENT) {
      M_LOG_WARN << "node has been deleted, abs_path: " << abs_path;
      ret = AddExistWatcherOnce(p_context, p_abs_path, false);
    } else if (type == ZOO_CHANGED_EVENT || type == ZOO_CREATED_EVENT) {
      char buf;
      int buf_len = 1;
      ret = zoo_wget(manager.GetHandler(), p_abs_path,
                     &ZookeeperManager::InnerWatcherCbFunc, p_context, &buf,
                     &buf_len, NULL);
      if (ret == ZNONODE) {
        M_LOG_WARN << "no node, abs_path: " << abs_path;
        ret = AddExistWatcherOnce(p_context, p_abs_path, false);
      }
    } else {
      M_LOG_ERROR << "mismatched, event type: " << type
                  << ", custom watcher type: " << p_context->m_watcher_type;
    }
  } else if (p_context->m_watcher_type == ZookeeperCtx::GET_CHILDREN) {
    if (type == ZOO_DELETED_EVENT) {
      M_LOG_WARN << "node has been deleted, abs_path: " << abs_path;
      ret = AddExistWatcherOnce(p_context, p_abs_path, false);
    } else if (type == ZOO_CHILD_EVENT || type == ZOO_CREATED_EVENT) {
      ScopedStringVector children;
      ret = zoo_wget_children(manager.GetHandler(), p_abs_path,
                              &ZookeeperManager::InnerWatcherCbFunc, p_context,
                              &children);
      if (ret == ZNONODE) {
        M_LOG_WARN << "no node, abs_path: " << abs_path;
        ret = AddExistWatcherOnce(p_context, p_abs_path, false);
      }
    } else {
      M_LOG_ERROR << "mismatched, event type: " << type
                  << ", custom watcher type: " << p_context->m_watcher_type;
    }
  } else {
    M_LOG_ERROR << "invalid custom watcher type: " << p_context->m_watcher_type;
  }

  if (ret != ZOK) {
    M_LOG_ERROR << "ret[" << ret << "], zerror[" << ZError(ret)
                << "], abs_path: " << abs_path;
  }

  return ret;
}

int ZookeeperManager::GlobalWatcherAutoRegisterer(ZookeeperCtx *p_context,
                                                  const char *p_abs_path,
                                                  int type) {
  M_LOG_DEBUG << "debug";
  std::string abs_path;
  if (p_abs_path) {
    abs_path = std::string(p_abs_path);
  }

  ZookeeperManager &manager = p_context->m_zookeeper_manager;
  int ret = ZOK;
  if (p_context->m_watcher_type != ZookeeperCtx::GLOBAL) return ret;

  uint32_t global_watcher_type = GetWatcherType(p_context, abs_path);
  if (global_watcher_type == NO_TYPE_IN_PATH) {
    M_LOG_ERROR << "can not find type in m_global_watcher_path_type_, do not "
                   "call watcher!";
    return NO_TYPE_IN_PATH;
  }

  if (type == ZOO_DELETED_EVENT) {
    if (((global_watcher_type & WATCHER_GET) == WATCHER_GET) ||
        ((global_watcher_type & WATCHER_GET_CHILDREN) ==
         WATCHER_GET_CHILDREN)) {
      M_LOG_WARN << "node has been deleted, abs_path: " << abs_path;
      ret = AddExistWatcherOnce(p_context, p_abs_path);
    }
  } else if (type == ZOO_CHANGED_EVENT) {
    if ((global_watcher_type & WATCHER_EXISTS) == WATCHER_EXISTS) {
      ret = manager.Exists(abs_path, NULL, 1);
      if (ret == ZNONODE) {
        ret = ZOK;
      }
    }
    if ((global_watcher_type & WATCHER_GET) == WATCHER_GET) {
      char buf;
      int buf_len = 1;
      ret = manager.Get(abs_path, &buf, &buf_len, NULL, 1);
      if (ret == ZNONODE) {
        M_LOG_WARN << "no node, abs_path: " << abs_path;
        ret = AddExistWatcherOnce(p_context, p_abs_path);
      }
    }
  } else if (type == ZOO_CREATED_EVENT) {
    if ((global_watcher_type & WATCHER_EXISTS) == WATCHER_EXISTS) {
      ret = manager.Exists(abs_path, NULL, 1);
      if (ret == ZNONODE) {
        ret = ZOK;
      }
    }
  } else if (type == ZOO_CHILD_EVENT) {
    if ((global_watcher_type & WATCHER_GET_CHILDREN) == WATCHER_GET_CHILDREN) {
      ScopedStringVector children;
      ret = manager.GetChildren(abs_path, children, 1);
      if (ret == ZNONODE) {
        M_LOG_WARN << "no node, abs_path: " << abs_path;
        ret = AddExistWatcherOnce(p_context, p_abs_path);
      }
    }
  } else {
    // Nothing
  }

  if (ret != ZOK) {
    M_LOG_ERROR << "ret[" << ret << "], zerror[" << ZError(ret)
                << "], abs_path: " << abs_path;
  }

  return ret;
}

int ZookeeperManager::AddExistWatcherOnce(ZookeeperCtx *p_context,
                                          const char *p_abs_path,
                                          bool add_to_global_watcher) {
  std::string abs_path;
  if (p_abs_path) {
    abs_path = std::string(p_abs_path);
  }
  ZookeeperManager &manager = p_context->m_zookeeper_manager;
  int ret;
  if (add_to_global_watcher) {
    ret = zoo_exists(manager.GetHandler(), p_abs_path, 1, NULL);
    if (ret != ZOK && ret != ZNONODE) {
      M_LOG_ERROR << "can not add EXIST global watcher, ret[" << ret
                  << "], zerror[" << ZError(ret) << "], abs_path: " << abs_path;
    } else {
      M_LOG_WARN << "add EXIST global watcher, abs_path: " << abs_path;
    }
  } else {
    ret = zoo_wexists(manager.GetHandler(), p_abs_path,
                      &ZookeeperManager::InnerWatcherCbFunc, p_context, NULL);
    if (ret != ZOK && ret != ZNONODE) {
      M_LOG_ERROR << "can not add EXIST custom watcher, ret[" << ret
                  << "], zerror[" << ZError(ret) << "], abs_path: " << abs_path;
    } else {
      M_LOG_WARN << "add EXIST custom watcher, abs_path: " << abs_path;
    }
  }

  return ret;
}

uint8_t ZookeeperManager::GetStopWatcherTypeMask(ZookeeperCtx *p_context,
                                                 const std::string &abs_path,
                                                 int type) {
  uint8_t stop_watcher_type_mask = UINT8_MAX;
  if (p_context->m_watcher_type != ZookeeperCtx::GLOBAL)
    return stop_watcher_type_mask;

  ZookeeperManager &manager = p_context->m_zookeeper_manager;

  std::unique_lock<std::mutex> lock(manager.m_global_watcher_path_type_mutex_);
  auto it = manager.m_global_watcher_path_type_.find(abs_path);
  if (it == manager.m_global_watcher_path_type_.end()) {
    M_LOG_ERROR << "can not find type in m_global_watcher_path_type_, return "
                   "stop_watcher_type_mask 0!";
    return stop_watcher_type_mask;
  }

  if (type == ZOO_CHANGED_EVENT) {
    if ((it->second & WATCHER_EXISTS) == WATCHER_EXISTS) {
      stop_watcher_type_mask = ~WATCHER_EXISTS;
      M_LOG_WARN << "delete WATCHER_EXISTS watcher type of path: " << abs_path;
    }
    if ((it->second & WATCHER_GET) == WATCHER_GET) {
      stop_watcher_type_mask &= ~WATCHER_GET;
      M_LOG_WARN << "delete WATCHER_GET watcher type of path: " << abs_path;
    }
  } else if (type == ZOO_CREATED_EVENT) {
    if ((it->second & WATCHER_EXISTS) == WATCHER_EXISTS) {
      stop_watcher_type_mask = ~WATCHER_EXISTS;
      M_LOG_WARN << "delete WATCHER_EXISTS watcher type of path: " << abs_path;
    }
  } else if (type == ZOO_CHILD_EVENT) {
    if ((it->second & WATCHER_GET_CHILDREN) == WATCHER_GET_CHILDREN) {
      stop_watcher_type_mask = ~WATCHER_GET_CHILDREN;
      M_LOG_WARN << "delete WATCHER_GET_CHILDREN watcher type of path: "
                 << abs_path;
    }
  }
  return stop_watcher_type_mask;
}

void ZookeeperManager::InnerVoidCompletion(int ret_code,
                                           const void *p_zookeeper_context) {
  ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(
      reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
  if (p_context == NULL) {
    M_LOG_ERROR << "callback function context is NULL!";
    return;
  }

  unique_ptr<ZookeeperCtx> up_context(p_context);

  ZookeeperManager &manager = up_context->m_zookeeper_manager;

  if (ret_code == ZOK && !up_context->m_ephemeral_path.empty()) {
    std::unique_lock<std::mutex> ephemeral_node_info_lock(
        manager.m_ephemeral_node_info_lock_);
    if (up_context->m_ephemeral_info == NULL) {
      manager.m_ephemeral_node_info_.erase(up_context->m_ephemeral_path);
      M_LOG_SPCL << "delete ephemeral node info: "
                 << up_context->m_ephemeral_path << ", ephemeral node number: "
                 << manager.m_ephemeral_node_info_.size();
    } else {
      manager.m_ephemeral_node_info_[up_context->m_ephemeral_path] =
          *up_context->m_ephemeral_info;
    }
  }

  if (up_context->m_void_completion_func != NULL &&
      *up_context->m_void_completion_func != NULL) {
    (*up_context->m_void_completion_func)(manager, ret_code);
  }
}

void ZookeeperManager::InnerStatCompletion(int ret_code, const Stat *stat,
                                           const void *p_zookeeper_context) {
  ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(
      reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
  if (p_context == NULL) {
    M_LOG_ERROR << "callback function context is NULL!";
    return;
  }

  unique_ptr<ZookeeperCtx> up_context(p_context);

  ZookeeperManager &manager = up_context->m_zookeeper_manager;

  if (ret_code == ZOK) {
    if (up_context->m_ephemeral_info != NULL &&
        !up_context->m_ephemeral_path.empty()) {
      std::unique_lock<std::mutex> ephemeral_node_info_lock(
          manager.m_ephemeral_node_info_lock_);
      manager.m_ephemeral_node_info_[up_context->m_ephemeral_path] =
          *up_context->m_ephemeral_info;
    }
  }

  if (ret_code == ZOK ||
      (ret_code == ZNONODE &&
       (up_context->m_global_watcher_add_type == WATCHER_EXISTS ||
        (up_context->m_custom_watcher_context != NULL &&
         up_context->m_custom_watcher_context->m_watcher_type ==
             ZookeeperCtx::EXISTS)))) {
    manager.ProcAsyncWatcher(*up_context);
  }

  if (up_context->m_stat_completion_func != NULL &&
      *up_context->m_stat_completion_func != NULL) {
    (*up_context->m_stat_completion_func)(manager, ret_code, stat);
  }
}

void ZookeeperManager::InnerDataCompletion(int ret_code, const char *value,
                                           int value_len, const Stat *stat,
                                           const void *p_zookeeper_context) {
  ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(
      reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
  if (p_context == NULL) {
    M_LOG_ERROR << "callback function context is NULL!";
    return;
  }

  unique_ptr<ZookeeperCtx> up_context(p_context);

  ZookeeperManager &manager = up_context->m_zookeeper_manager;

  if (ret_code == ZOK) {
    manager.ProcAsyncWatcher(*up_context);
  }

  if (up_context->m_data_completion_func != NULL &&
      *up_context->m_data_completion_func != NULL) {
    (*up_context->m_data_completion_func)(manager, ret_code, value, value_len,
                                          stat);
  }
}

void ZookeeperManager::InnerStringsCompletion(int ret_code,
                                              const String_vector *strings,
                                              const void *p_zookeeper_context) {
  ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(
      reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
  if (p_context == NULL) {
    M_LOG_ERROR << "callback function context is NULL!";
    return;
  }

  unique_ptr<ZookeeperCtx> up_context(p_context);

  ZookeeperManager &manager = up_context->m_zookeeper_manager;

  if (ret_code == ZOK) {
    manager.ProcAsyncWatcher(*up_context);
  }

  if (up_context->m_strings_stat_completion_func != NULL &&
      *up_context->m_strings_stat_completion_func != NULL) {
    (*up_context->m_strings_stat_completion_func)(manager, ret_code, strings,
                                                  NULL);
  }
}

void ZookeeperManager::InnerStringsStatCompletion(
    int ret_code, const String_vector *strings, const Stat *stat,
    const void *p_zookeeper_context) {
  ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(
      reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
  if (p_context == NULL) {
    M_LOG_ERROR << "callback function context is NULL!";
    return;
  }

  unique_ptr<ZookeeperCtx> up_context(p_context);

  ZookeeperManager &manager = up_context->m_zookeeper_manager;

  if (ret_code == ZOK) {
    manager.ProcAsyncWatcher(*up_context);
  }

  if (up_context->m_strings_stat_completion_func != NULL &&
      *up_context->m_strings_stat_completion_func != NULL) {
    (*up_context->m_strings_stat_completion_func)(manager, ret_code, strings,
                                                  stat);
  }
}

void ZookeeperManager::InnerStringCompletion(int ret_code, const char *value,
                                             const void *p_zookeeper_context) {
  ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(
      reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
  if (p_context == NULL) {
    M_LOG_ERROR << "callback function context is NULL!";
    return;
  }

  unique_ptr<ZookeeperCtx> up_context(p_context);

  ZookeeperManager &manager = up_context->m_zookeeper_manager;

  if (ret_code == ZOK && up_context->m_ephemeral_info != NULL &&
      !up_context->m_ephemeral_path.empty()) {
    std::unique_lock<std::mutex> ephemeral_node_info_lock(
        manager.m_ephemeral_node_info_lock_);
    manager.m_ephemeral_node_info_[up_context->m_ephemeral_path] =
        *up_context->m_ephemeral_info;
  }

  if (up_context->m_string_completion_func != NULL &&
      *up_context->m_string_completion_func != NULL) {
    (*up_context->m_string_completion_func)(manager, ret_code, value);
  }
}

void ZookeeperManager::InnerAclCompletion(int ret_code, ACL_vector *acl,
                                          Stat *stat,
                                          const void *p_zookeeper_context) {
  ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(
      reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
  if (p_context == NULL) {
    M_LOG_ERROR << "callback function context is NULL!";
    return;
  }

  unique_ptr<ZookeeperCtx> up_context(p_context);

  ZookeeperManager &manager = up_context->m_zookeeper_manager;

  if (up_context->m_acl_completion_func != NULL &&
      *up_context->m_acl_completion_func != NULL) {
    (*up_context->m_acl_completion_func)(manager, ret_code, acl, stat);
  }
}

void ZookeeperManager::InnerMultiCompletion(int ret_code,
                                            const void *p_zookeeper_context) {
  ZookeeperCtx *p_context = const_cast<ZookeeperCtx *>(
      reinterpret_cast<const ZookeeperCtx *>(p_zookeeper_context));
  if (p_context == NULL) {
    M_LOG_ERROR << "callback function context is NULL!";
    return;
  }

  unique_ptr<ZookeeperCtx> up_context(p_context);

  ZookeeperManager &manager = up_context->m_zookeeper_manager;

  manager.ProcMultiEphemeralNode(up_context->m_multi_ops->m_multi_ops,
                                 *up_context->m_multi_results);

  if (up_context->m_multi_completion_func != NULL &&
      *up_context->m_multi_completion_func != NULL) {
    (*up_context->m_multi_completion_func)(manager, ret_code,
                                           up_context->m_multi_ops,
                                           up_context->m_multi_results);
  }
}

void ZookeeperManager::AddCustomWatcher(
    const string &abs_path, shared_ptr<ZookeeperCtx> watcher_context) {
  std::unique_lock<std::mutex> custom_watcher_contexts_lock(
      m_custom_watcher_contexts_mutex_);

  m_custom_watcher_contexts_.insert(make_pair(abs_path, watcher_context));
  M_LOG_SPCL << "add custom watcher: abs_path[" << abs_path
             << "], watcher type[" << watcher_context->m_watcher_type
             << "], local clientID: " << m_zk_client_id_.client_id
             << ", current clientID: "
             << watcher_context->m_zookeeper_manager.m_zk_client_id_.client_id;
}

std::shared_ptr<ZookeeperCtx> ZookeeperManager::GetCustomWatcherCtx(
    const std::string &abs_path, int type,
    const std::shared_ptr<WatcherFuncType> &watcher_func) {
  shared_ptr<ZookeeperCtx> p_zookeeper_watcher_context = nullptr;

  {
    std::unique_lock<std::mutex> custom_watcher_contexts_lock(
        m_custom_watcher_contexts_mutex_);
    auto find_its = m_custom_watcher_contexts_.equal_range(abs_path);
    for (auto it = find_its.first; it != find_its.second; ++it) {
      if (it->second->m_watcher_type ==
          type /* && it->second->m_watcher_func == watcher_func */) {
        p_zookeeper_watcher_context = it->second;
        break;
      }
    }
  }

  if (p_zookeeper_watcher_context == nullptr) {
    p_zookeeper_watcher_context =
        make_shared<ZookeeperCtx>(*this, (ZookeeperCtx::WatcherType)type);
    p_zookeeper_watcher_context->m_watcher_func = watcher_func;
    AddCustomWatcher(abs_path, p_zookeeper_watcher_context);
  } else {
    if (p_zookeeper_watcher_context->m_is_stop) {
      p_zookeeper_watcher_context->m_is_stop = false;
      M_LOG_WARN << "start custom watcher, path: " << abs_path
                 << ", watcher type: " << type;
    }
  }

  return p_zookeeper_watcher_context;
}

void ZookeeperManager::DelCustomWatcherCtx(
    const string &abs_path, const ZookeeperCtx *watcher_context) {
  std::unique_lock<std::mutex> custom_watcher_contexts_lock(
      m_custom_watcher_contexts_mutex_);
  auto find_its = m_custom_watcher_contexts_.equal_range(abs_path);
  for (auto it = find_its.first; it != find_its.second; ++it) {
    if (it->second.get() == watcher_context) {
      m_custom_watcher_contexts_.erase(it);
      M_LOG_SPCL << "delete custom watcher: abs_path[" << abs_path << "]";
      return;
    }
  }
}

bool ZookeeperManager::ResumeEnv() {
  bool is_ok = true;
  if (!ResumeGlobalWatcher()) M_LOG_ERROR << "resume global watcher failed!";
  if (!ResumeCustomWatcher()) M_LOG_ERROR << "resume custom watcher failed!";
  is_ok = ResumeEphemeralNode();
  if (!is_ok) M_LOG_ERROR << "resume eephemeral node failed!";
  return is_ok;
}

bool ZookeeperManager::ResumeGlobalWatcher() {
  bool is_ok = true;
  int32_t ret = ZOK;
  // 注册全局Watcher
  M_LOG_SPCL << "start re-registering global Watcher...";
  auto m_global_watcher_path_type = GetGlobalWatcherPathType();
  for (auto it = m_global_watcher_path_type.begin();
       it != m_global_watcher_path_type.end(); ++it) {
    for (int resume_count = 1; resume_count <= resume_max_count_;
         ++resume_count) {
      if (resume_count - 1 > 0)
        sleep((resume_count - 1) <= 10 ? (resume_count - 1) : 10);
      ret = ZOK;

      M_LOG_SPCL << "re-register global Watcher, path[" << it->first
                 << "], type[" << it->second << "], on " << resume_count
                 << " time";
      if (it->first.empty() || it->first.at(0) != '/') {
        M_LOG_ERROR << "re-register global Watcher, invalid path: " << it->first
                    << "], type[" << it->second << "], on " << resume_count
                    << " time";
        break;
      }

      if (it->second == 0) {
        M_LOG_WARN << "re-register global Watcher, invalid type[" << it->second
                   << "], path[" << it->first << "], on " << resume_count
                   << " time, skip";
        break;
      }

      std::string abs_path = ChangeToAbsPath(it->first);

      if ((it->second & WATCHER_EXISTS) == WATCHER_EXISTS) {
        ret = zoo_exists(m_zhandle_, abs_path.c_str(), 1, NULL);
        if (ret == ZNONODE) {
          ret = ZOK;
        }
      }

      if ((it->second & WATCHER_GET) == WATCHER_GET) {
        char buf;
        int buf_len = 1;
        ret = zoo_get(m_zhandle_, abs_path.c_str(), 1, &buf, &buf_len, NULL);
      }

      if ((it->second & WATCHER_GET_CHILDREN) == WATCHER_GET_CHILDREN) {
        ScopedStringVector children;
        ret = zoo_get_children(m_zhandle_, abs_path.c_str(), 1, &children);
      }

      if (resume_count >= resume_alert_count_ &&
          resume_global_watcher_alerter_) {
        M_LOG_SPCL << "call resume_global_watcher_alerter_ on resume, path: "
                   << it->first << "], watcher type[" << it->second << "], on "
                   << resume_count << " time";
        resume_global_watcher_alerter_();
      }

      if (ret != ZOK) {
        is_ok = false;
        M_LOG_ERROR << "re-register global Watcher error, path[" << it->first
                    << "], ret[" << ret << "], zerror[" << ZError(ret)
                    << "], on " << resume_count << " time";
      } else {
        if (call_watcher_func_on_resume_) {
          if (IsValidCallBack(m_global_watcher_context_->m_watcher_func)) {
            M_LOG_SPCL << "call global watcher function on resume, path: "
                       << it->first << "], watcher type[" << it->second
                       << "], on " << resume_count << " time";
            (*m_global_watcher_context_->m_watcher_func)(
                *this, RESUME_EVENT, RESUME_SUCC, it->first.c_str());
          } else {
            M_LOG_ERROR << "invalid global watcher function";
          }
        }
        break;
      }
    }
  }
  return is_ok;
}

bool ZookeeperManager::ResumeCustomWatcher() {
  bool is_ok = true;
  int32_t ret = ZOK;
  M_LOG_SPCL << "start re-registering custom Watcher...";
  auto m_custom_watcher_contexts_ = GetCustomWatcherContexts();
  for (auto it = m_custom_watcher_contexts_.begin();
       it != m_custom_watcher_contexts_.end(); ++it) {
    for (int resume_count = 1; resume_count <= resume_max_count_;
         ++resume_count) {
      if (resume_count - 1 > 0)
        sleep((resume_count - 1) <= 10 ? (resume_count - 1) : 10);
      ret = ZOK;

      M_LOG_SPCL << "re-register custom Watcher, path[" << it->first
                 << "], type[" << it->second->m_watcher_type << "], on "
                 << resume_count << " time";
      if (it->second->m_watcher_type == ZookeeperCtx::EXISTS) {
        ret = zoo_wexists(m_zhandle_, it->first.c_str(),
                          &ZookeeperManager::InnerWatcherCbFunc,
                          it->second.get(), NULL);
        if (ret == ZNONODE) ret = ZOK;
      } else if (it->second->m_watcher_type == ZookeeperCtx::GET) {
        char buf;
        int buf_len = 1;
        ret = zoo_wget(m_zhandle_, it->first.c_str(),
                       &ZookeeperManager::InnerWatcherCbFunc, it->second.get(),
                       &buf, &buf_len, NULL);
      } else if (it->second->m_watcher_type == ZookeeperCtx::GET_CHILDREN) {
        ScopedStringVector children;
        ret = zoo_wget_children(m_zhandle_, it->first.c_str(),
                                &ZookeeperManager::InnerWatcherCbFunc,
                                it->second.get(), &children);
      } else {
        M_LOG_WARN << "re-register custom Watcher, invalid Watcher type, path: "
                   << it->first << "], type[" << it->second << "], on "
                   << resume_count << " time";
        break;
      }

      if (resume_count >= resume_alert_count_ &&
          resume_custom_watcher_alerter_) {
        M_LOG_SPCL << "call resume_custom_watcher_alerter_ on resume, path: "
                   << it->first << "], watcher type["
                   << it->second->m_watcher_type << "], on " << resume_count
                   << " time";
        resume_custom_watcher_alerter_();
      }

      if (ret != ZOK) {
        is_ok = false;
        M_LOG_ERROR << "re-register custom Watcher error, path[" << it->first
                    << "], ret[" << ret << "], zerror[" << ZError(ret)
                    << "], on " << resume_count << " time";
      } else {
        if (call_watcher_func_on_resume_) {
          if (IsValidCallBack(it->second->m_watcher_func)) {
            M_LOG_SPCL << "call custom watcher function on resume, path: "
                       << it->first << "], watcher type["
                       << it->second->m_watcher_type << "], on " << resume_count
                       << " time";
            (*it->second->m_watcher_func)(*this, RESUME_EVENT, RESUME_SUCC,
                                          it->first.c_str());
          } else {
            M_LOG_ERROR << "invalid global watcher function";
          }
        }
        break;
      }
    }
  }
  return is_ok;
}

bool ZookeeperManager::ResumeEphemeralNode() {
  bool is_ok = true;
  int32_t ret = ZOK;
  M_LOG_SPCL << "start re-creating tmp node...";
  auto m_ephemeral_node_info = GetEphemeralNodeInfo();
  for (auto it = m_ephemeral_node_info.begin();
       it != m_ephemeral_node_info.end(); ++it) {
    for (int resume_count = 1; resume_count <= resume_max_count_;
         ++resume_count) {
      if (resume_count - 1 > 0)
        sleep((resume_count - 1) <= 10 ? (resume_count - 1) : 10);
      M_LOG_SPCL << "re-create tmp node, path[" << it->first << "], data["
                 << it->second.data << "], on " << resume_count << " time";
      ret = zoo_create(m_zhandle_, it->first.c_str(), it->second.data.c_str(),
                       it->second.data.size(), &it->second.acl,
                       it->second.flags, NULL, 0);

      if (ret == ZNONODE) {
        auto last_slash_pos = it->first.rfind('/');
        if (last_slash_pos == string::npos) {
          M_LOG_ERROR << "can not create parent node, invalid tmp node path: "
                      << it->first << ", skip";
          break;
        }

        if (last_slash_pos == 0) {
          M_LOG_ERROR << "can not create under root node, tmp node: "
                      << it->first << ", skip";
          break;
        }

        string parent_path = it->first.substr(0, last_slash_pos);
        ret = CreatePathRecursion(parent_path);
        if (ret != ZOK) {
          ret = CreatePathRecursion(parent_path);
          if (ret != ZOK) {
            if (ret == ZNODEEXISTS) {
              M_LOG_WARN << "parent node[" << parent_path
                         << "] already exists, ret[" << ret << "], zerror["
                         << ZError(ret) << "], on " << resume_count << " time";
            } else {
              M_LOG_ERROR << "create parent node[" << parent_path
                          << "] failed again, ret[" << ret << "], zerror["
                          << ZError(ret) << "], tmp node[" << it->first
                          << "] can not be created, on " << resume_count
                          << " time";
            }
          }
        }

        ret = zoo_create(m_zhandle_, it->first.c_str(), it->second.data.c_str(),
                         it->second.data.size(), &it->second.acl,
                         it->second.flags, NULL, 0);
      }

      if (resume_count >= resume_alert_count_ &&
          resume_ephemeral_node_alerter_) {
        M_LOG_SPCL << "call resume_ephemeral_node_alerter_ on resume, path: "
                   << it->first << "], on " << resume_count << " time";
        resume_ephemeral_node_alerter_();
      }

      if (ret != ZOK) {
        ret = zoo_create(m_zhandle_, it->first.c_str(), it->second.data.c_str(),
                         it->second.data.size(), &it->second.acl,
                         it->second.flags, NULL, 0);
        if (ret != ZOK) {
          if (ret == ZNODEEXISTS) {
            M_LOG_WARN << "tmp node[" << it->first << "] already exists, ret["
                       << ret << "], zerror[" << ZError(ret) << "], on "
                       << resume_count << " time";
            break;
          } else {
            is_ok = false;
            M_LOG_ERROR << "re-create tmp node[" << it->first
                        << "] failed again, ret[" << ret << "], zerror["
                        << ZError(ret) << "], on " << resume_count << " time";
          }
        } else {
          break;
        }
      } else {
        break;
      }
    }
  }
  return is_ok;
}

void ZookeeperManager::ProcMultiEphemeralNode(
    const vector<zoo_op> &multi_ops,
    const vector<zoo_op_result_t> &multi_result) {
  auto result_it = multi_result.begin();
  for (auto zoo_op_it = multi_ops.begin();
       zoo_op_it != multi_ops.end() && result_it != multi_result.end();
       ++zoo_op_it, ++result_it) {
    if (result_it->err != ZOK) {
      M_LOG_ERROR << "operation failed, type: " << zoo_op_it->type << ", path"
                  << CharPtrToString(zoo_op_it->create_op.path);
      continue;
    }

    if (zoo_op_it->type == ZOO_CREATE_OP &&
        (zoo_op_it->create_op.flags & ZOO_EPHEMERAL)) {
      std::unique_lock<std::mutex> ephemeral_node_info_lock(
          m_ephemeral_node_info_lock_);
      m_ephemeral_node_info_[zoo_op_it->create_op.path].acl =
          *zoo_op_it->create_op.acl;
      m_ephemeral_node_info_[zoo_op_it->create_op.path].data.assign(
          zoo_op_it->create_op.data, zoo_op_it->create_op.data_len);
      m_ephemeral_node_info_[zoo_op_it->create_op.path].flags =
          zoo_op_it->create_op.flags;
      M_LOG_SPCL << "add ephemeral node info: "
                 << CharPtrToString(zoo_op_it->create_op.path)
                 << ", ephemeral node number: "
                 << m_ephemeral_node_info_.size();
    } else {
      std::unique_lock<std::mutex> ephemeral_node_info_lock(
          m_ephemeral_node_info_lock_);
      if (zoo_op_it->type == ZOO_DELETE_OP &&
          m_ephemeral_node_info_.find(zoo_op_it->create_op.path) !=
              m_ephemeral_node_info_.end()) {
        m_ephemeral_node_info_.erase(zoo_op_it->create_op.path);
        M_LOG_SPCL << "delete ephemeral node info: "
                   << CharPtrToString(zoo_op_it->create_op.path)
                   << ", ephemeral node number: "
                   << m_ephemeral_node_info_.size();
      } else if (zoo_op_it->type == ZOO_SETDATA_OP &&
                 m_ephemeral_node_info_.find(zoo_op_it->create_op.path) !=
                     m_ephemeral_node_info_.end()) {
        m_ephemeral_node_info_[zoo_op_it->create_op.path].data.assign(
            zoo_op_it->create_op.data, zoo_op_it->create_op.data_len);
      } else {
        // Nothing
      }
    }
  }
}

void ZookeeperManager::ProcAsyncWatcher(ZookeeperCtx &context) {
  if (!context.m_watch_path.empty()) {
    if (context.m_global_watcher_add_type != 0) {
      std::unique_lock<std::mutex> global_watcher_path_type_lock(
          m_global_watcher_path_type_mutex_);
      m_global_watcher_path_type_[context.m_watch_path] |=
          context.m_global_watcher_add_type;
    } else if (context.m_custom_watcher_context != NULL) {
      AddCustomWatcher(context.m_watch_path, context.m_custom_watcher_context);
    } else {
      M_LOG_ERROR
          << "Wathcer is not custom or global, there is some problem, path: "
          << context.m_watch_path;
    }
  }
}

std::string ZookeeperManager::ZError(int err) {
  return CharPtrToString(zerror(err));
}

std::string ZookeeperManager::CharPtrToString(const char *p) {
  return std::string(p ? p : "");
}

void MultiOps::AddCreateOp(const string &path, const char *value, int value_len,
                           const ACL_vector *acl /*= &ZOO_OPEN_ACL_UNSAFE*/,
                           int flags /*= 0*/,
                           uint32_t max_real_path_size /*= 128*/) {
  zoo_op op;
  string abs_path =
      multi_zk_manager == NULL ? path : multi_zk_manager->ChangeToAbsPath(path);
  shared_ptr<string> curr_path = make_shared<string>(move(abs_path));
  shared_ptr<string> curr_buffer = make_shared<string>(value, value_len);

  if (max_real_path_size > 0) {
    shared_ptr<string> real_path =
        make_shared<string>(max_real_path_size, '\0');
    zoo_create_op_init(&op, curr_path->c_str(), curr_buffer->data(),
                       curr_buffer->size(), acl, flags, &(*real_path).at(0),
                       real_path->size());
    m_inner_strings.push_back(real_path);
  } else {
    zoo_create_op_init(&op, curr_path->c_str(), curr_buffer->data(),
                       curr_buffer->size(), acl, flags, NULL, 0);
  }

  m_multi_ops.push_back(op);
  m_inner_strings.push_back(curr_path);
  m_inner_strings.push_back(curr_buffer);
}

void MultiOps::AddCreateOp(const string &path, const string &value,
                           const ACL_vector *acl /*= &ZOO_OPEN_ACL_UNSAFE*/,
                           int flags /*= 0*/,
                           uint32_t max_real_path_size /*= 128*/) {
  AddCreateOp(path, value.data(), value.size(), acl, flags, max_real_path_size);
}

void MultiOps::AddDeleteOp(const string &path, int version) {
  zoo_op op;
  string abs_path =
      multi_zk_manager == NULL ? path : multi_zk_manager->ChangeToAbsPath(path);
  shared_ptr<string> curr_path = make_shared<string>(move(abs_path));

  zoo_delete_op_init(&op, curr_path->c_str(), version);

  m_multi_ops.push_back(op);
  m_inner_strings.push_back(curr_path);
}

void MultiOps::AddSetOp(const string &path, const char *buffer, int buf_len,
                        int version, bool need_stat /*= false*/) {
  zoo_op op;
  string abs_path =
      multi_zk_manager == NULL ? path : multi_zk_manager->ChangeToAbsPath(path);
  shared_ptr<string> curr_path = make_shared<string>(move(abs_path));
  shared_ptr<string> curr_buffer = make_shared<string>(buffer, buf_len);
  if (need_stat) {
    shared_ptr<string> stat_buf = make_shared<string>(sizeof(Stat), '\0');
    zoo_set_op_init(
        &op, curr_path->c_str(), curr_buffer->data(), curr_buffer->size(),
        version,
        reinterpret_cast<Stat *>(const_cast<char *>(stat_buf->data())));
    m_inner_strings.push_back(stat_buf);
  } else {
    zoo_set_op_init(&op, curr_path->c_str(), curr_buffer->data(),
                    curr_buffer->size(), version, NULL);
  }

  m_multi_ops.push_back(op);
  m_inner_strings.push_back(curr_path);
  m_inner_strings.push_back(curr_buffer);
}

void MultiOps::AddSetOp(const string &path, const string &buffer, int version,
                        bool need_stat /*= false*/) {
  AddSetOp(path, buffer.data(), buffer.size(), version, need_stat);
}

void MultiOps::AddCheckOp(const string &path, int version) {
  zoo_op op;
  string abs_path =
      multi_zk_manager == NULL ? path : multi_zk_manager->ChangeToAbsPath(path);
  shared_ptr<string> curr_path = make_shared<string>(move(abs_path));

  zoo_check_op_init(&op, curr_path->c_str(), version);

  m_multi_ops.push_back(op);
  m_inner_strings.push_back(curr_path);
}

}  // namespace zookeeper
