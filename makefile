######include zookeeper.mk

#本项目相关变量
ROOT_DIR = .

#编译器
CXX = g++

#目标文件
TARGET = libCppZooKeeper.so
TARGETA = libCppZooKeeper.a

#头文件包含目录，一行一个
INC_DIR +=-I./zookeeper/include/

#静态库文件路径
STATIC_LIBS_DIR = ${ROOT_DIR}/zookeeper/lib
STATIC_LIBS = $(foreach n,$(STATIC_LIBS_DIR), $(wildcard $(n)/libzookeeper_*.a))

#其他库文件，一行一个
#STATIC_LIBS += ${STATIC_LIBS_DIR}/libtinyxml.a

#链接选项
#LDFLAGS = -lpthread
#LDFLAGS += -ldl
#LDFLAGS += -fPIC
LDFLAGS = -fPIC
LDFLAGS += -rdynamic
#LDFLAGS += -lboost_system -lboost_log_setup -lboost_log -lboost_thread -lboost_date_time -lboost_serialization \
#-lboost_regex -lboost_chrono -lpthread
#LDFLAGS += -fprofile-arcs -ftest-coverage
#LDFLAGS += ${STATIC_LIBS}
#LDFLAGS += -pg

#编译选项
CFLAGS += -rdynamic -g -MMD -O3 -Wall -Wextra -fPIC
#CFLAGS += -std=gnu++11
CFLAGS += -std=c++11
CFLAGS += -DBOOST_LOG_DYN_LINK
#CFLAGS += -pg
#CFLAGS += -fprofile-arcs -ftest-coverage
CFLAGS += $(INC_DIR)

#自动搜寻，当前项目的目标文件
#SUBDIR可以指定多个目录，指定的目录下的所有cpp文件会加入编译，比如 SUBDIR = . src
SUBDIR = . boost-log
CURR_SOURCES =$(foreach n,$(SUBDIR), $(wildcard $(n)/*.cpp)) 

#其他外部依赖cpp文件在此处加上
CURR_SOURCES +=

#生成对应的.o文件
CXX_OBJECTS = $(patsubst %.cpp, %.o, ${CURR_SOURCES})

#添加自定义.o，这些.o不由clean删除，没有则无需填写
EXT_OBJECTS +=

#生成依赖关系
DEP_FILES += $(patsubst %.o, %.d, ${CXX_OBJECTS})
 
all: static dynamic
dynamic: $(TARGET)

$(TARGET):  $(CXX_OBJECTS) ${EXT_OBJECTS} ${STATIC_LIBS}
	$(CXX) $(LDFLAGS) -shared -o $@ $^

static: $(TARGETA)

$(TARGETA):$(CXX_OBJECTS)
	ar crvs $@ $^
 
%.o:%.cpp
	${CXX} -c $(CFLAGS) -MT $@ -MF $(patsubst %.cpp, %.d,  $<) -o $@ $<
 
-include ${DEP_FILES}

test:
	@echo ${CURR_SOURCES}
 
clean:
	rm -rf ${TARGET} ${TARGETA} ${CXX_OBJECTS} ${DEP_FILES}

run:
	make

dep:
	./make_dep.sh

install:
	rm -rf ./CppZooKeeperApiHeader
	rm -rf ./CppZooKeeperApiLib
	mkdir -p ./CppZooKeeperApiHeader/boost-log
	cp *.h ./CppZooKeeperApiHeader/
	cp ./boost-log/*.h ./CppZooKeeperApiHeader/boost-log/
	cp -r ./zookeeper/include/zookeeper ./CppZooKeeperApiHeader/
	mkdir -p ./CppZooKeeperApiLib/zookeeper
	cp *.a *.so ./CppZooKeeperApiLib
	cp -r ./zookeeper/lib ./CppZooKeeperApiLib/zookeeper

# mkdir zookeeper
# cd ./zookeeper-3.4.12/src/c
# ./configure --prefix=../../../zookeeper
# make
# make install
# cp -r ./src/hashtable ../../../zookeeper
# cp ./src/zk_adaptor.h ../../../zookeeper
# cp ./src/zk_hashtable.h ../../../zookeeper