//
// Created by admin on 2018/9/11.
//
#include <thread>
#include <unistd.h>
#include <boost/log/sinks.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/smart_ptr/shared_ptr.hpp>

#ifndef DOC_FEATURE_LOG_H
#define DOC_FEATURE_LOG_H

namespace CppZooKeeper {

class logger {
public:
    enum LEVEL {
        L_DEBUG,
        L_INFO,
        L_WARN,
        L_ERROR,
        L_FATAL,
        L_SPCL
    };

    typedef boost::log::sinks::synchronous_sink<boost::log::sinks::text_file_backend> file_sink_t;
    typedef boost::log::sinks::synchronous_sink<boost::log::sinks::text_ostream_backend> basic_sink_t;

    bool debug;
    bool info;
    bool warn;
    bool error;
    bool fatal;

    boost::shared_ptr<basic_sink_t> defaultSinkPtr;
    boost::shared_ptr<file_sink_t> fileSinkPtr;
    boost::log::sources::severity_logger<LEVEL> lg;
//    static boost::log::sources::severity_logger<boost::log::trivial::severity_level> lg;

    ~logger();

    void flush();

    void initLogger(const std::string &logPath, const std::string &logName,
                    enum LEVEL level, unsigned int flushPeriod = 0);

    static logger *getInstance();

private:
    bool running;
    unsigned int flushPeriod;
    std::thread autoFlushThread;

    logger();

    logger(logger &&other) = delete;

    logger(const logger &other) = delete;

    logger &operator=(logger &&right) = delete;

    logger &operator=(const logger &right) = delete;

    void autoFlush();

    void stopAutoFlush();

    void stopDefaultLogging();

    boost::shared_ptr<basic_sink_t> init();

    static std::string innerPutTime(struct tm *t);

    static std::string padTime(const std::string &time);

    static std::string TimeStampToLocalTime(uint64_t timestamp);
};

#define LOG_SPCL BOOST_LOG_SEV(logger::getInstance()->lg,logger::L_SPCL) << "[" << __FILE__ << "|" << __FUNCTION__ << ":" << __LINE__ << "]: "
#define LOG_DEBUG BOOST_LOG_SEV(logger::getInstance()->lg,logger::L_DEBUG) << "[" << __FILE__<< "|" << __FUNCTION__ << ":" << __LINE__<< "]: "
#define LOG_INFO BOOST_LOG_SEV(logger::getInstance()->lg,logger::L_INFO) << "[" << __FILE__ << "|" << __FUNCTION__ << ":" << __LINE__ << "]: "
#define LOG_WARN BOOST_LOG_SEV(logger::getInstance()->lg,logger::L_WARN) << "[" << __FILE__ << "|" << __FUNCTION__ << ":" << __LINE__ << "]: "
#define LOG_ERROR BOOST_LOG_SEV(logger::getInstance()->lg,logger::L_ERROR) << "[" << __FILE__ << "|" << __FUNCTION__ << ":" << __LINE__ << "]: "
#define LOG_FATAL BOOST_LOG_SEV(logger::getInstance()->lg,logger::L_FATAL) << "[" << __FILE__ << "|" << __FUNCTION__ << ":" << __LINE__ <<  "]: "

#define LOG_SPCL_IMM(data) {LOG_SPCL << (data);logger::getInstance()->flush();}
#define LOG_DEBUG_IMM(data) {if(logger::getInstance()->debug) {LOG_DEBUG << (data);logger::getInstance()->flush();}}
#define LOG_INFO_IMM(data) {if(logger::getInstance()->info) {LOG_INFO << (data);logger::getInstance()->flush();}}
#define LOG_WARN_IMM(data) {if(logger::getInstance()->warn) {LOG_WARN << (data);logger::getInstance()->flush();}}
#define LOG_ERROR_IMM(data) {if(logger::getInstance()->error) {LOG_ERROR << (data);logger::getInstance()->flush();}}
#define LOG_FATAL_IMM(data) {LOG_FATAL << (data);logger::getInstance()->flush();}

}

#endif //DOC_FEATURE_LOG_H
