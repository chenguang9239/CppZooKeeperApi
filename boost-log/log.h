//
// Created by admin on 2018/9/11.
//
#include <thread>
#include <unistd.h>
#include <boost/log/sinks.hpp>
#include <boost/log/trivial.hpp>
//#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/severity_channel_logger.hpp>
#include <boost/smart_ptr/shared_ptr.hpp>

#ifndef CPP_ZK_API_LOG_H
#define CPP_ZK_API_LOG_H

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
//    boost::log::sources::severity_logger<LEVEL> lg;
    boost::log::sources::severity_channel_logger<LEVEL, std::string> lg;
//    static boost::log::sources::severity_logger<boost::log::trivial::severity_level> lg;

    ~logger();

    void flush();

    void initLogger(const std::string &logPath, const std::string &logName,
                    enum LEVEL level, unsigned int flushPeriod = 0, const std::string &channelString = "Z");

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

#define M_LOG_SPCL BOOST_LOG_SEV(logger::getInstance()->lg,logger::L_SPCL) << "[" << __FILE__ << "|" << __FUNCTION__ << ":" << __LINE__ << "]: "
#define M_LOG_DEBUG BOOST_LOG_SEV(logger::getInstance()->lg,logger::L_DEBUG) << "[" << __FILE__<< "|" << __FUNCTION__ << ":" << __LINE__<< "]: "
#define M_LOG_INFO BOOST_LOG_SEV(logger::getInstance()->lg,logger::L_INFO) << "[" << __FILE__ << "|" << __FUNCTION__ << ":" << __LINE__ << "]: "
#define M_LOG_WARN BOOST_LOG_SEV(logger::getInstance()->lg,logger::L_WARN) << "[" << __FILE__ << "|" << __FUNCTION__ << ":" << __LINE__ << "]: "
#define M_LOG_ERROR BOOST_LOG_SEV(logger::getInstance()->lg,logger::L_ERROR) << "[" << __FILE__ << "|" << __FUNCTION__ << ":" << __LINE__ << "]: "
#define M_LOG_FATAL BOOST_LOG_SEV(logger::getInstance()->lg,logger::L_FATAL) << "[" << __FILE__ << "|" << __FUNCTION__ << ":" << __LINE__ <<  "]: "

#define M_LOG_SPCL_IMM(data) {M_LOG_SPCL << (data);logger::getInstance()->flush();}
#define M_LOG_DEBUG_IMM(data) {if(logger::getInstance()->debug) {M_LOG_DEBUG << (data);logger::getInstance()->flush();}}
#define M_LOG_INFO_IMM(data) {if(logger::getInstance()->info) {M_LOG_INFO << (data);logger::getInstance()->flush();}}
#define M_LOG_WARN_IMM(data) {if(logger::getInstance()->warn) {M_LOG_WARN << (data);logger::getInstance()->flush();}}
#define M_LOG_ERROR_IMM(data) {if(logger::getInstance()->error) {M_LOG_ERROR << (data);logger::getInstance()->flush();}}
#define M_LOG_FATAL_IMM(data) {M_LOG_FATAL << (data);logger::getInstance()->flush();}

}

#endif //CPP_ZK_API_LOG_H
