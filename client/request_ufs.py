#!encoding=utf-8 import os
import sys
import uuid
import ctypes

import traceback
import locale
import sys
import time
import uuid
from pprint import pprint

sys.path.append("../../../common/interface/gen-py")

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from rec import UserProfileService
from rec.RankingService import *
from rec.ttypes import UIRequest
from rec.ttypes import PageRequest
from rec.ttypes import AbTest
from rec.ttypes import AbTestProfile
from rec.ttypes import RankingServiceResponse
from rec import RankingService

#search_type = SEARCH_TYPE.OPEN_PLATFORM_RECOMMEND
#search_type = SEARCH_TYPE.EXPLOR_APP_RECOMMEND
search_type = SEARCH_TYPE.NEWS_APP_RECOMMEND
page_id = "toutiao"
#page_id = "T1348647909107"
#page_id = "T1457068979049"
#page_id = "T1348648517839" #yule
#page_id = "T1521013416394" #世界杯
#page_id = "T1348648756099video" #caijing  video
#page_id = "T1348648756099" #caijing
#page_id = "T1348649079062video" #tiyu video
#page_id = "T1348649079062" #tiyu
#page_id = "T1348648517839video" #yule video
#page_id = "T1348649580692video" #keji video
#page_id = "T1457068979049" #video
#page_id = "T1348649580692" #keji
#page_id = "T1456112189138"
#page_id = "T1507706790347"
#page_id = "T1507706745537"
#devid = "2CB7A513-84BB-4EC0-9D47-E655D60735FA"
devid = sys.argv[1]

def Error(msg):
    """ Print message to stderr."""
    sys.stderr.write("%s\n" %(msg))
    traceback.print_exc()
    logging.fatal(traceback.format_exc())
    logging.warning(msg)

class RequestClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = None

    def InitClient(self):
        try:
            socket = TSocket.TSocket(self.host, self.port)
            transport = TTransport.TFramedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = UserProfileService.Client(protocol)
            transport.open()
            self.client = client
            return True
        except Exception, e:
            Error(e)
            print ("InitClient failed from %s:%s" %(self.host, self.port))
            return False

    def Search(self, ui_req):
        if None != self.client:
            response = self.client.search(ui_req)
            return response
        else:
            print "client has not been initialized!"
            return None

class RequestBuilder:
    """struct UIRequest {
        1:optional SEARCH_TYPE search_type
        2:optional string sid                //检索id
        3:optional i32 req_num               //请求item条数 @size@ 客户端请求条数
        4:optional UserRequest user_request
        5:optional PageRequest page_request
        6:optional i64 timestamp
        7:optional string session_id        //会话id
        8:optional AbTestProfile abtest_profile

        // 配置用,TODO(bjzhangdongyue) 新版会去掉这个
        1000: optional CHANNEL_TYPE channel_type;  
    }"""
    def __init__(self):
        self.user_request = UserRequest

    def BuildUserProfileServiceRequest(self):
        ui_request = UIRequest()
        ui_request.search_type = search_type
        ui_request.sid = uuid.uuid1().hex
        ui_request.user_request = self.BuildUserRequest()
        ui_request.page_request = PageRequestBuilder.BuildRequest()
        ui_request.abtest_profile = self.BuildAbTestProfile()
        user_profile_request = UserProfileServiceRequest()
        user_profile_request.ui_request = ui_request
        return user_profile_request

    def BuildUserRequest(self):
        """ 请求中用户信息
         struct UserRequest {
             1:optional UID_TYPE uid_type
             2:optional string uid
             3:optional Device device
             4:optional Network network
             5:optional Gps gps
             6:optional App app                  // 新闻客户端app相关信息
             7:optional Location location        // 废弃
             8:optional string user_source       // 用户来源，区分新闻客户端，第三方
         }"""
        user_req = UserRequest()
        user_req.uid_type = UID_TYPE.DEVICE_ID
        #user_req.uid_type = UID_TYPE.PASSPORT
        #user_req.uid = "ogm9zs72vdce0106d8283915cafc7aca9ad2c1e40f@wx.163.com"
        #user_req.uid = "@163.con"
        user_req.uid = "CQk2OWI0ODAwYzk4MDhkMTM1CVMyNVFCRFBEMjJDUlY%3D"
        #user_req.uid = "lyflainey@126.com"
        ##user_req.uid = "869765029145750"
        device_uuid = devid
        #device_uuid = "ogm9zs72vdce0106d8283915cafc7aca9ad2cle40f@wx.163.com"
        user_req.device = self.BuildDeviceInfo(DEVICE_TYPE.MOBILE, device_uuid, OS_TYPE.IOS)
        network = Network()
        network.ipv4= "117.136.97.2"
        #network.ipv4= "223.211.255.255" #广东江门
        #network.ipv4= "128.75.35.227"
        user_req.network = network
        #user_req.gps = self.BuildGpsInfo(GPS_TYPE.GCJ02 , 37.95, 111.27)
        return user_req

    def BuildDeviceInfo(self, device_type, device_uuid, os_type):
        """struct Device {
            1:optional DEVICE_TYPE device_type
            2:optional OS_TYPE os_type
            3:optional Version os_version
            4:optional DeviceID device_id
            5:optional ORIENTATION orientation
            6:optional string vendor            //手机厂商
            7:optional string model             //手机型号 ios https://www.theiphonewiki.com/wiki/Models#iPhone
            8:optional double screen_density    //屏幕像素密度
            9:optional string useragent
            10:optional string cookie
        }"""
        device = Device()
        #device.device_type = device_type
        #device.os_type = os_type
        device_id = DeviceID()
        #device_id.devid = devid
        device_id.uuid = device_uuid
        device.device_id = device_id
        #device_id.mac = '00:05:33:33:ce:d7'
        return device
        
    def BuildGpsInfo(self, gps_type, lat, lng):
        gps = Gps()
        gps.gps_type = gps_type
        gps.longitude = lng
        gps.latitude = lat 
        return gps

    def BuildAbTestProfile(self):
        abtest = AbTest()
        abtest.exp_id = 115
        abtest.exp_name = "ug"
        abtest_list = []
        abtest_list.append(abtest)
        abtests = {ABTEST_MODULE.UFS : abtest_list}
        abtest_profile = AbTestProfile()
        abtest_profile.abtests = abtests
        return abtest_profile

class PageRequestBuilder:
    def __init__(self):
        self.page_request = None

    @staticmethod
    def BuildRequest():
        page_req = PageRequest()
        #page_req.page_type = PAGE_TYPE.CHANNEL
        #page_req.page_id = "T1348647909107"
        page_req.page_id = page_id
        return page_req

def PrintKvItemInfo(item_info, desc, fields, decorate_char='-'):
    if decorate_char:
        print "%s" %(decorate_char*50)
    else:
        print "%s" %("-"*50)
    print (desc)
    if item_info:
        kv_type_item = vars(item_info)
        for field in fields:
            print("%20s: %-20s" %(field, kv_type_item[field]))
    else:
        print("[%s] is NONE!!" %(desc))

if __name__ == '__main__':
    print "start connect"
    #client = RequestClient("10.160.247.71",8019) #kaifaji
    client = RequestClient("10.200.129.170",8019) #测试机
    #client = RequestClient("10.200.166.171",8019) 
    #client = RequestClient("10.200.130.70",8019) 
    #client = RequestClient("10.200.164.151",8019) #117
    #client = RequestClient("10.200.164.152",8019) #118
    #client = RequestClient("10.200.128.163",8019) #58
    #client = RequestClient("10.200.128.170",8019) #65
    #client = RequestClient("10.200.128.171",8019) #66
    #client = RequestClient("10.200.128.172",8019) #67
    #client = RequestClient("10.200.128.175",8019) #70
    #client = RequestClient("10.200.128.177",8019) #71
    #client = RequestClient("10.200.128.178",8019) #72
    #client = RequestClient("10.200.128.179",8019) #73
    #client = RequestClient("10.200.166.171",8019) #178
    #client = RequestClient("10.200.166.172",8019) #179
    #client = RequestClient("10.200.166.173",8019) #180
    #client = RequestClient("10.200.131.34",8019) #42
    #client = RequestClient("10.200.128.158",8019) #53
    if not client.InitClient():
        print "connect failed!"
        sys.exit(-1)

    print "connect!!!!"

    request_builder = RequestBuilder()
    request = request_builder.BuildUserProfileServiceRequest()

    PrintKvItemInfo(request.ui_request, "UI_REQUEST", {'sid', 'search_type', 'user_request'})
    
    count =1 
    while count>0:
        result = client.Search(request)
        time.sleep(1) 
        count = count-1
        if None != result:
            print "has result"
            print("error_code: %-12s" %result.err_code)
            print("msg: %-12s" %result.msg)
            print result
        else:
            print "result is NONE"
