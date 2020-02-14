#!encoding=utf-8
"""
feature_engine客户端
对feature_engine服务端发起请求进行测试
"""
import os
import uuid
import ctypes
import traceback
import time
import json
import string
import logging

import sys
sys.path.append("../ml-thrift/gen-py/")
import pprint
import random
from urlparse import urlparse
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import TSSLSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol
from thrift.protocol.TMultiplexedProtocol import TMultiplexedProtocol

from feature.ttypes import *
from feature import *
from feature import FeatureService
from common_ml import *
from common_ml.ttypes import *
from common_ml.ttypes import UserProfile as mlUserProfile


class FeatureRequestClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = None

    def Init(self):
        try:
            socket = TSocket.TSocket(self.host, self.port)
            transport = TTransport.TFramedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            protocol_multi = TMultiplexedProtocol(protocol,"Feature Service")
            client = FeatureService.Client(protocol_multi)

            transport.open()
            self.client = client
            #print("connect success")
            return True
        except Exception, e:
            print("InitClient exception:%s" % e)  
            return False


    def buildFeatureRequest(self):
        request = FeatureRequest()
        request.rid = "".join(random.choice(string.ascii_letters) for x in range(16))
        print "rid:"+ request.rid
        request.feature_type = FeatureType.ALGO_TF_FEATURE
        request.feature_ids = [50415,50593,50594]
        #request.feature_ids = [1002, 1003, 1026, 3065, 3067, 3071, 3072, 3078, 3081, 3085,3000, 50117,202,220,224,68001,50593,50594]
        #request.feature_ids = [ 1, 2, 3, 50100, 50101, 50104, 50105, 50106, 50107, 50109, 50117, 50118, 50119, 50162, 50163, 50202, 50219, 50220, 50224, 50226, 50232, 50236, 50250, 50251, 50253, 50254, 50256, 50410, 50411, 50412, 50413, 50414, 50457, 50458, 50459, 50460, 62101, 62104, 62105, 62106, 62107, 62109, 62110, 62117, 62118, 62119, 62120, 62162, 63410, 63411, 63412, 63413, 63457, 63458, 63459, 63460, 202, 219, 220, 224, 226, 236, 237, 250, 253, 254, 256, 68001, 68002, 68003, 68004, 68005, 68006, 68007, 68008, 68009, 68010, 68011, 68012, 69001, 69002, 69003, 69004, 69005, 69006, 69007, 69008, 69009, 69010, 69011, 69012, 69013, 69014, 69015, 69016, 69017, 69018, 69019, 69020, 69021, 69022, 69023, 69024, 69025, 69026, 69027, 69028, 69029, 69030, 69031, 69032, 69033, 69034, 69035, 69036, 69070, 69071, 69072, 69073, 69074, 69075, 69076, 69077, 69078, 69079, 69080, 69081, 69082, 69083, 69084, 69085, 69086, 69087]
        #request.feature_ids = [1002,1003, 1004, 1026, 1071, 3000, 3002, 3014, 3018, 3020, 3029, 3044, 3045, 3048, 3051, 3052, 3054, 3055, 3065, 3067, 3071, 3072, 3078, 3081, 3085, 3091, 3100, 3101, 3108]
        doc_info = DocInfo()
        doc_info.docid = "VV8NUCCU5"
        doc_ids = list()
        doc_ids.append(doc_info)
        request.docs = doc_ids
        userinfo = UserInfo()
        userinfo.timestamp = 1542028563
        userinfo.age = 100
        userinfo.location_code = "110000"
        userinfo.gender = "MALE"

        user_realtime_profile = []
        user_profile = mlUserProfile()
        user_profile.type = 0x10014
        #user_profile.type = 18
        user_profile.key = "1111"
        user_profile.score = 0.11
        user_realtime_profile.append(user_profile)
        userinfo.user_realtime_profile = user_realtime_profile

        user_longterm_profile = []
        user_profile = mlUserProfile()
        user_profile.type = 0x10014
        #user_profile.type = 18
        user_profile.key = "2222"
        user_profile.score = 0.22
        user_longterm_profile.append(user_profile)
        userinfo.user_longterm_profile = user_longterm_profile


        click_docs = []
        doc1 = DocAndTs()
        doc1.element = "E0DF6B5F05483MSY"
        doc1.score = 1542028399000
        click_docs.append(doc1)
        userinfo.click_doc = click_docs;
        userinfo.user_stats = dict()
        userinfo.uid= "12345678";
        userinfo.device_id= "12345678";
        request.user_info = userinfo

        request.project =  "toutiao"
        request.business =  "toutiao"
        request.generate_textual_features =  True
        return request


    def features(self, request):
        try:
            if None != self.client:
                print("Features requst is:") + str(request)
                response = self.client.Features(request)
                print("Features response:") + str(response)
                return True
            else:
                print ("client has not been initialized!")
                return None
        except Exception as e:
            print ("client.features exception:")   + str(e)
            #logging.error("client.Features exception:%s" %e)
            return None

if __name__ == "__main__":
    client = FeatureRequestClient("10.200.128.163",11111)
    #client = FeatureRequestClient("10.200.130.70",6701)
    client.Init()
    request = client.buildFeatureRequest()
    client.features(request)
