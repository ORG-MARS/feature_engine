# -*- coding: utf-8 -*-
# @file Makefile
# @author lidongming
# @date 2018-08-30 15:48
# @brief

ifeq ($(shell uname -m),x86_64)

ROOT=.
DEPS_DIR=$(ROOT)/deps

# CC=gcc
# CXX=g++
CC=/usr/local/bin/gcc
CXX=/usr/local/bin/g++

THRIFT_PATH=./deps/thrift

CCHECK=../tools/cpplint.py

# Modules
MODULE=feature_engine

# enable when .thrift updated
REGENERATE_THRIFT="OK"
ifdef REGENERATE_THRIFT
  GENERATE_THRIFT=$(shell cd ml-thrift \
    && ../$(THRIFT_PATH)/bin/thrift -r --gen cpp feature.thrift \
    && ../$(THRIFT_PATH)/bin/thrift -r --gen cpp doc_property.thrift \
    && cd -)

	GENERATE_THRIFT_STATUS=

	ifeq ($(GENERATE_THRIFT), $(shell pwd))
		GENERATE_THRIFT_STATUS='SUCCESS'
	endif #ifeq generate thrift files 
endif

# 自动获取Git版本信息,支持-v查看如./feature_engine -v
# RELEASE_VERSION := $(shell sh -c './version/gen_version.sh')

CXXFLAGS += -O3 -DNDEBUG -DFEATURE_CACHE_LRU
INC_DIR += -I$(DEPS_DIR)/jemalloc/include
LIB_INC += $(DEPS_DIR)/jemalloc/lib/libjemalloc.a

#$(info "CXXFLAGS VALUE:" ${CXXFLAGS})

# LDFLAGS= -L./deps/commonlib/lib -lcommonlib -Wl,-rpath=./deps/commonlib/lib
LDFLAGS=
CXXFLAGS +=-g \
		 -pipe \
		 -W \
		 -Wall \
		 -Wextra \
		 -m64 \
		 -std=c++17 \
		 -Wno-invalid-offsetof \
		 -Wno-deprecated \
		 -Wno-deprecated-declarations \
		 -Wno-unused-parameter \
		 -Wno-sign-compare \
		 -Wno-write-strings \
		 -Wno-unused-local-typedefs \
		 -Wno-literal-suffix \
		 -Wno-narrowing \
		 -Wno-parentheses \
		 -Wno-unused-but-set-variable \
		 -Wno-unused-variable \
		 -Wno-char-subscripts \
		 -Wno-implicit-fallthrough \
		 -Wno-register \
		 -ffast-math

INC_DIR +=-I. -I.. -I./server \
		-I$(DEPS_DIR) \
		-I$(DEPS_DIR)/commonlib/lib \
		-I$(DEPS_DIR)/boost/include \
		-I$(DEPS_DIR)/hiredis-vip/include \
		-I$(DEPS_DIR)/libevent/include \
		-I$(DEPS_DIR)/rapidjson \
		-I$(DEPS_DIR)/thrift/include \
		-I$(DEPS_DIR)/thrift/include/thrift \
		-I$(DEPS_DIR)/smalltable/include \
		-I$(DEPS_DIR)/cityhash/include \
		-I$(DEPS_DIR)/sparsehash/include \
		-I$(DEPS_DIR)/libconfig/include \
		-I$(DEPS_DIR)/librdkafka/include \
		-I$(DEPS_DIR)/gflags/include \
		-I$(DEPS_DIR)/glog/include \
		-I$(DEPS_DIR)/gtest/include \
		-I$(DEPS_DIR)/mysql/include \
		-I$(DEPS_DIR)/protobuf/include \
		-I$(DEPS_DIR)/serman/include
	
BOOST_LIBS=$(DEPS_DIR)/boost/lib/libboost_system.a \
	  $(DEPS_DIR)/boost/lib/libboost_atomic.a \
	  $(DEPS_DIR)/boost/lib/libboost_chrono.a \
	  $(DEPS_DIR)/boost/lib/libboost_container.a \
	  $(DEPS_DIR)/boost/lib/libboost_context.a \
	  $(DEPS_DIR)/boost/lib/libboost_contract.a \
	  $(DEPS_DIR)/boost/lib/libboost_coroutine.a \
	  $(DEPS_DIR)/boost/lib/libboost_date_time.a \
	  $(DEPS_DIR)/boost/lib/libboost_exception.a \
	  $(DEPS_DIR)/boost/lib/libboost_fiber.a \
	  $(DEPS_DIR)/boost/lib/libboost_filesystem.a \
	  $(DEPS_DIR)/boost/lib/libboost_iostreams.a \
	  $(DEPS_DIR)/boost/lib/libboost_locale.a \
	  $(DEPS_DIR)/boost/lib/libboost_random.a \
	  $(DEPS_DIR)/boost/lib/libboost_regex.a \
	  $(DEPS_DIR)/boost/lib/libboost_serialization.a \
	  $(DEPS_DIR)/boost/lib/libboost_thread.a

LIB_INC= -Xlinker "-(" \
		$(BOOST_LIBS) \
		$(DEPS_DIR)/commonlib/lib/libcommonlib.a \
		$(DEPS_DIR)/hiredis-vip/lib/libhiredis_vip.a \
		$(DEPS_DIR)/libevent/lib/libevent.a \
		$(DEPS_DIR)/gflags/lib/libgflags.a \
		$(DEPS_DIR)/glog/lib/libglog.a \
		$(DEPS_DIR)/thrift/lib/libthrift.a \
		$(DEPS_DIR)/thrift/lib/libthriftnb.a \
		$(DEPS_DIR)/cityhash/lib/libcityhash.a \
		$(DEPS_DIR)/smalltable/lib/libsmalltable.a \
		$(DEPS_DIR)/protobuf/lib/libprotobuf.a \
		$(DEPS_DIR)/libconfig/lib/libconfig++.a \
		$(DEPS_DIR)/librdkafka/lib/librdkafka++.a \
		$(DEPS_DIR)/librdkafka/lib/librdkafka.a \
		$(DEPS_DIR)/mysql/lib/libmysqlclient.a \
		$(DEPS_DIR)/gtest/lib/libgtest.a \
		$(DEPS_DIR)/serman/lib/libserman.a \
		$(DEPS_DIR)/serman/lib/libppconsul.a \
		$(DEPS_DIR)/serman/lib/libcurl.a \
		$(DEPS_DIR)/serman/lib/libjson11.a \
		-pthread -ldl -lcrypto -lz -lrt -lssl \
		-static-libgcc -static-libstdc++ \
		-lexpat -lm -lc \
		-Xlinker "-)"

######################STRACE########################
ifdef PRINT_BACKSTRACE
CXXFLAGS += -rdynamic -DPRINT_BACKSTRACE
endif

######################TCMALLOC######################
ifdef TCMALLOC
LDFLAGS += -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free
CXXFLAGS += -DUSE_TCMALLOC
INC_DIR += -I$(DEPS_DIR)/gperftools/include
LIB_INC += $(DEPS_DIR)/gperftools/lib/libtcmalloc_and_profiler.a
endif

#########################STAT########################

all: PRE_BUILD bin/$(MODULE) test/unittest bin/feature_client bin/ufs_client bin/consul_client
	@echo "[[32mBUILD[0m][Target:'[32mall[0m']"
	@echo "[[32mRUNNING UNITTEST[0m]"
	# ./test/unittest

	@echo "[[32mmake all done[0m]"

system-check:
	@echo "[[32CHECK DEPENDENCY[0m]"

# 语法规范检查
style:
	python ../tools/cpplint.py --extensions=hpp,cpp --linelength=80 *.cpp

clean:
	# FIXME(lidongming):clean old thrift and pb files when .thrift and .proto updated
	# @rm -rf ml-thrift/gen-cpp
	# @rm -rf proto/*.pb.cc
	# @rm -rf proto/*.pb.h
	@find . -name "*.o" | xargs -I {} rm {}
	@rm -rf bin/*
	@rm -rf test/unittest
.phony:clean

PRE_BUILD:
	@echo "[[32mgenerate thrift status:$(GENERATE_THRIFT_STATUS)[0m]"
	@mkdir -p bin

IGNORE_FILE_PATERN="deps/|main/|*.skeleton.cpp|proto/|test/main.cpp|client/main.cpp|client/ufs_client.cpp|client/consul_main.cpp"
SERVER_OBJS += $(patsubst %.proto,%.pb.o, $(shell find proto -type f -name *.proto))
SERVER_OBJS += $(patsubst %.cpp,%.o, $(shell find . -type f -name *.cpp | egrep -v $(IGNORE_FILE_PATERN)))
SERVER_OBJS += $(patsubst %.cc, %.o, $(shell find . -type f -name *.cc  | egrep -v $(IGNORE_FILE_PATERN)))
SERVER_OBJS += $(patsubst %.c,  %.o, $(shell find . -type f -name *.c   | egrep -v $(IGNORE_FILE_PATERN)))

%.o:%.cpp
	@echo "[[32mBUILD[0m][Target:'[32m$<[0m']"
	@$(CXX) $(CXXFLAGS) $(INC_DIR) -c $< -o $@

%.o:%.cc
	@echo "[[32mBUILD[0m][Target:'[32m$<[0m']"
	@$(CXX) $(CXXFLAGS) $(INC_DIR) -c $< -o $@

%.o:%.c
	@echo "[[32mBUILD[0m][Target:'[32m$<[0m']"
	@$(CXX) $(CXXFLAGS) $(INC_DIR) -c $< -o $@

bin/$(MODULE) : $(SERVER_OBJS) 
	$(CXX) -o $@ $(INC_DIR) $(LDFLAGS) $(CXXFLAGS) \
		main/main.cpp $(SERVER_OBJS) $(LIB_INC)

test/unittest: $(SERVER_OBJS) 
	@$(CXX) -o $@ $(INC_DIR) $(LDFLAGS) $(CXXFLAGS) \
		test/main.cpp $(SERVER_OBJS) $(LIB_INC)
	strip test/unittest

bin/feature_client : $(SERVER_OBJS) 
	$(CXX) -o $@ $(INC_DIR) $(LDFLAGS) $(CXXFLAGS) \
		client/main.cpp $(SERVER_OBJS) $(LIB_INC)
	strip bin/feature_client

bin/ufs_client : $(SERVER_OBJS) 
	$(CXX) -o $@ $(INC_DIR) $(LDFLAGS) $(CXXFLAGS) \
		client/ufs_client.cpp $(SERVER_OBJS) $(LIB_INC)

bin/consul_client : $(SERVER_OBJS) 
	$(CXX) -o $@ $(INC_DIR) $(LDFLAGS) $(CXXFLAGS) \
		client/consul_main.cpp $(BOOST_LIBS) $(SERVER_OBJS) $(LIB_INC) $(LIBS)
	# strip bin/consul_client

# Compile proto files
# PROTOC_CMD=./deps/protobuf/bin/protoc
# FIXME(lidongming):check dependency
PROTOC_CMD=protoc
PROTOS_PATH=./proto

.PRECIOUS:%.pb.cc %.pb.h
%.pb.h %.pb.cc:%.proto
	@echo "[[32mBUILD[0m][Target:'[32m$<[0m']"
	$(PROTOC_CMD) -I $(PROTOS_PATH) --cpp_out=./proto $<

endif #ifeq ($(shell uname -m),x86_64)
