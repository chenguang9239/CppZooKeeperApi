//
// Created by admin on 2018/9/11.
//
#include "log.h"
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <chrono>
#include <iomanip>

namespace CppZooKeeper {

BOOST_LOG_ATTRIBUTE_KEYWORD(severity, "Severity", logger::LEVEL)
BOOST_LOG_ATTRIBUTE_KEYWORD(channel, "Channel", std::string)

boost::shared_ptr<logger::basic_sink_t> logger::init() {
    auto ptr = boost::log::add_console_log(
            std::cout,
            boost::log::keywords::format = (boost::log::expressions::stream
                    << boost::log::expressions::format_date_time<boost::posix_time::ptime>(
                            "TimeStamp", "%Y-%m-%d|%H:%M:%S.%f")
                    << "[" << severity << "]" << boost::log::expressions::smessage));
    boost::log::add_common_attributes();
    return ptr;
}

std::ostream &operator<<(std::ostream &strm, logger::LEVEL level) {
    static const char *strings[] = {"DEBUG",
                                    "INFO",
                                    "WARN",
                                    "ERROR",
                                    "FATAL",
                                    "SPCL"};
    if (static_cast< std::size_t >(level) < sizeof(strings) / sizeof(*strings))
        strm << strings[level];
    else
        strm << static_cast< int >(level);
    return strm;
}

void logger::stopDefaultLogging() {
    if (defaultSinkPtr) {
        boost::log::core::get()->remove_sink(defaultSinkPtr);
        defaultSinkPtr.reset();
    }
}

void logger::flush() {
    if (fileSinkPtr) fileSinkPtr->flush();
}

void logger::autoFlush() {
    if (flushPeriod <= 0) return;
    using std::chrono::duration_cast;
    using std::chrono::system_clock;
    using std::chrono::seconds;
    flush();
    auto lastFlushAt = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    while (running) {
        auto curTimestamp = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
        if (curTimestamp - lastFlushAt >= flushPeriod) {
            flush();
            lastFlushAt = curTimestamp;
        } else { sleep(1); }
    }
}

std::string logger::innerPutTime(struct tm *t) {
    std::string res;
    try {
        if (t) {
            res += std::to_string(1900 + t->tm_year);
            res += padTime(std::to_string(1 + t->tm_mon));
            res += padTime(std::to_string(t->tm_mday));
            res += padTime(std::to_string(t->tm_hour));
            res += padTime(std::to_string(t->tm_min));
            res += padTime(std::to_string(t->tm_sec));
        }
    } catch (const std::exception &e) {
        M_LOG_ERROR << "innerPutTime exception: " << e.what();
        res.clear();
    }
    return res;
}

std::string logger::padTime(const std::string &time) {
    std::string res = "0";
    try {
        if (time.size() == 1) {
            res += time;
            return res;
        } else { return time; }
    } catch (const std::exception &e) {
        M_LOG_ERROR << "pad time exception: " << e.what();
        return time;
    }
}

std::string logger::TimeStampToLocalTime(uint64_t timestamp) {
    std::time_t t(timestamp);
//    return std::put_time(std::localtime(&t), "%F %T");
    return innerPutTime(std::localtime(&t));
}

void logger::initLogger(const std::string &logPath, const std::string &logName,
                        enum LEVEL level, unsigned int userFlushPeriod, const std::string &channelString) {
    stopDefaultLogging();
    flushPeriod = userFlushPeriod;
    std::string tmpLogPath = logPath, tmpLogName = logName;
    if (!tmpLogPath.empty()) {
        while (tmpLogPath.back() == '/')
            if (!tmpLogPath.empty())
                tmpLogPath.pop_back();
    }
    if (tmpLogPath.empty()) tmpLogPath = ".";
    if (tmpLogName.empty()) tmpLogName = "undefined";

    uint64_t TimeStamp = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    std::string timeStr = TimeStampToLocalTime(TimeStamp);
    std::stringstream ss;
    ss << tmpLogPath << "/" << tmpLogName << "_" << timeStr << "_%N.log";
    fileSinkPtr = boost::log::add_file_log(
            // auto_flush can put log to file immediately, but bad to efficiency
//            boost::log::keywords::auto_flush = true,
            boost::log::keywords::file_name = ss.str(),
//            boost::log::keywords::filter = channel == channelString,
            boost::log::keywords::rotation_size = 1024 * 1024 * 1024, /*< rotate files every 1 GB... >*/
            boost::log::keywords::time_based_rotation = boost::log::sinks::file::rotation_at_time_point(0, 0, 0),
            boost::log::keywords::format = (boost::log::expressions::stream
                    << boost::log::expressions::format_date_time<boost::posix_time::ptime>(
                            "TimeStamp", "%Y-%m-%d|%H:%M:%S.%f")
                    //<< "[" << boost::log::trivial::severity
                    << "[" << severity << "]" << boost::log::expressions::smessage));
    boost::log::add_common_attributes();

    if (channelString != "Z") {
        lg = boost::log::sources::severity_channel_logger<LEVEL, std::string>(
                boost::log::keywords::channel = channelString);
    }

    switch (level) {
        case L_DEBUG: {
            M_LOG_SPCL_IMM("set boost log level: DEBUG_LEVEL");
//            boost::log::core::get()->set_filter(severity >= L_DEBUG);
            fileSinkPtr->set_filter(channel == channelString && severity >= L_DEBUG);
            debug = true;
            break;
        }
        case L_INFO: {
            M_LOG_SPCL_IMM("set boost log level: INFO_LEVEL");
//            boost::log::core::get()->set_filter(severity >= L_INFO);
            fileSinkPtr->set_filter(channel == channelString && severity >= L_INFO);
            break;
        }
        case L_WARN: {
            M_LOG_SPCL_IMM("set boost log level: WARN_LEVEL");
//            boost::log::core::get()->set_filter(severity >= L_WARN);
            fileSinkPtr->set_filter(channel == channelString && severity >= L_WARN);
            info = false;
            break;
        }
        case L_ERROR: {
            M_LOG_SPCL_IMM("set boost log level: ERROR_LEVEL");
//            boost::log::core::get()->set_filter(severity >= L_ERROR);
            fileSinkPtr->set_filter(channel == channelString && severity >= L_ERROR);
            info = warn = false;
            break;
        }
        case L_FATAL: {
            M_LOG_SPCL_IMM("set boost log level: FATAL_LEVEL");
//            boost::log::core::get()->set_filter(severity >= L_FATAL);
            fileSinkPtr->set_filter(channel == channelString && severity >= L_FATAL);
            info = warn = error = false;
            break;
        }
        default: {
            M_LOG_SPCL_IMM("level " + std::to_string((int) level) + " not exist, set boost log level FATAL_LEVEL");
//            boost::log::core::get()->set_filter(severity >= L_FATAL);
            fileSinkPtr->set_filter(channel == channelString && severity >= L_FATAL);
            info = warn = error = false;
            break;
        }
    }

    M_LOG_SPCL_IMM("init logger ok, file path: " + ss.str() + ", set log channel: " + channelString);

    if (flushPeriod > 0) {
        M_LOG_SPCL_IMM("auto flush period: " + std::to_string(flushPeriod) + "s");
        running = true;
        autoFlushThread = std::thread(&logger::autoFlush, this);
    }
}

void logger::stopAutoFlush() {
    if (flushPeriod > 0) {
        running = false;
        if (autoFlushThread.joinable()) {
            autoFlushThread.join();
        } else {
            M_LOG_ERROR_IMM("autoFlushThread not joinable!");
        }
    }
}

logger::logger() : debug(false), info(true), warn(true), error(true), fatal(true),
                   defaultSinkPtr(init()), lg(boost::log::keywords::channel = "Z"),
                   running(false), flushPeriod(0) {
}

logger::~logger() {
    stopAutoFlush();
    // todo wait flush over
    sleep(1);
}

logger *logger::getInstance() {
    // need c++11
    static logger instance;
    return &instance;
}

}