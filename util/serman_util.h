
// File   serman_util.h
// Author huangxiaowei1 
// Date   2018-10-26 22:43:26
// Brief

#ifndef FEATURE_ENGINE_UTIL_SERMAN_UTIL_H_
#define FEATURE_ENGINE_UTIL_SERMAN_UTIL_H_

#include "feature_engine/deps/serman/include/serman.h"
#include "feature_engine/common/common_gflags.h"
#include "feature_engine/deps/gflags/include/gflags/gflags.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

namespace feature_engine {

class SermanUtils {
  public:
    static bool regServer() {
      std::string self_ip = SermanUtils::getSelfServerIp();
      LOG(INFO) <<"consul server_name:"<< FLAGS_consul_register_name
        << ",self_server_ip:" << self_ip
        << ",port:" << FLAGS_port
        << ",ttl_sec:" <<FLAGS_consul_server_check_ttl_sec ;
      if (!serman::ServerManage::getInstance()->regServer(
            FLAGS_consul_register_name,
            self_ip,
            FLAGS_port,
            FLAGS_consul_server_check_ttl_sec)) {
        LOG(FATAL) << "consul regServer error" ;
        return false;
      }
      LOG(INFO) << "register consul success" ;
      return true;
    }
    static bool deleteServer() {
      std::string self_ip = SermanUtils::getSelfServerIp();
      return serman::ServerManage::getInstance()->deleteServer(
          FLAGS_consul_register_name,
          self_ip,
          FLAGS_port
          );
    }
    static std::string getSelfServerIp() {
      std::string self_ip ;
      if (!GetHostIP(self_ip)) {
        return FLAGS_self_server_ip ;
      }
      return self_ip ;
    }
    static bool GetHostIP(std::string& ip) {
      char szIP[16] = {0};
      struct addrinfo *answer, hint, *curr;
      memset(&hint, '\0',sizeof(hint));
      hint.ai_family = AF_INET;
      hint.ai_socktype = SOCK_STREAM;
      int iRet = 0;
      char szHostName[128] = {0};
      iRet = gethostname(szHostName, sizeof(szHostName));
      if (iRet != 0) {
        return false;
      }
      iRet = getaddrinfo(szHostName, NULL, &hint, &answer);
      if (iRet != 0) {
        return false ;
      }
      for (curr = answer; curr != NULL; curr = curr->ai_next){
        inet_ntop(AF_INET,&(((struct sockaddr_in *)(curr->ai_addr))->sin_addr), szIP, 16);
      }
      freeaddrinfo(answer);
      ip.assign(szIP);
      return true ;
    }
};

}
#endif  //  FEATURE_ENGINE_UTIL_SERMAN_UTIL_H_


