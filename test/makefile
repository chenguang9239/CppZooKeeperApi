BOOSTLIB = /usr/local/boost_1_68_0_lib
ZKCPPCLIENTLIB = ../
ZKCCLIENTLIB = ../zookeeper/lib

DLIBS=-Wl,-rpath,$(BOOSTLIB):$(ZKCPPCLIENTLIB)

POS=-L$(BOOSTLIB) -L$(ZKCPPCLIENTLIB) -L$(ZKCCLIENTLIB)

LIBS=-lboost_system -lboost_log_setup -lboost_log -lboost_thread -lboost_date_time -lboost_serialization \
-lboost_regex -lboost_chrono -lpthread \
-lCppZooKeeper
#-lzookeeper_mt -lzookeeper_st

CXXFLAGS += -I../
CXXFLAGS += -std=c++11
CXXFLAGS += -O3
CXXFLAGS += -g
CXXFLAGS += -DBOOST_LOG_DYN_LINK


BIN=test

all: $(BIN)

clean:
	rm -rf *.o $(BIN)

test: test.o
	$(CXX) -o $@ test.o $(LIBS) $(POS) $(DLIBS) -pthread
	@g++ -v