# -*- coding: utf-8 -*-
'''
Author: Arkind
Python 2.7.6

#https://codesprint.skplanet.com/2014/participation/round2_intro.htm
'''
import urllib
import urllib2
import json
import logging
import sys
import os
import time

logPath, logFileName = "log", "5_23_1"
logging.basicConfig(level=logging.INFO)
FORMAT = "%(asctime)s [%(threadName)-10.10s] [%(levelname)-5.5s]  %(message)s"
logFormatter = logging.Formatter(FORMAT)
logger = logging.getLogger("CSR2")
logger.setLevel(logging.DEBUG)

fileHandler = logging.FileHandler("{0}/{1}.log".format(logPath, logFileName))
fileHandler.setFormatter(logFormatter)
fileHandler.setLevel(logging.INFO)
logger.addHandler(fileHandler)

# consoleHandler = logging.StreamHandler()
# consoleHandler.setFormatter(logFormatter)
# logger.addHandler(consoleHandler)

DATA_PATH = "data/"


class CSR2_API(object):
    def __init__(self, token):
        self.token = token

    def sendReq(self, URL, params=None):
        append_data = json.dumps(params) if not params is None else None
        req = urllib2.Request(URL, append_data)
        req.add_header('X-Auth-Token', self.token)

        r = None
        while True:
            try:
                r = urllib2.urlopen(req)
            except urllib2.HTTPError, error:
                error_code = json.loads(error.read())['code']
                logger.exception("ERROR code %d" % (error_code), exc_info=True)
            result = json.loads(r.readline())
            if 'error' in result:
                if result['error']['code'] == 40010 or \
                   result['error']['code'] == 40011:
                    continue
            break
        logger.info("sendReq" + json.dumps(result))
        return result

    #GET: 턴 시작
    def reqStartNewTurn(self):
        URL = "https://adsche.skplanet.com/api/startNewTurn"
        result = self.sendReq(URL)
        if 'turnNo' in result or \
           'error' in result and result['error']['code'] == 40013:
            return True

        return False

    #기초 데이터 획득
    #GET:광고 목록
    def reqAdList(self):
        URL = "https://adsche.skplanet.com/api/adList"

        if os.path.isfile(AD_PATH):
            return True

        result = self.sendReq(URL)
        if not 'error' in result:
            with open(AD_PATH, "w") as f:
                f.write(json.dumps(result))
            return True
        return False

    #GET:미디어 목록
    def reqMediaList(self):
        URL = "https://adsche.skplanet.com/api/mediaList"

        if os.path.isfile(MEDIA_PATH):
            return True

        result = self.sendReq(URL)
        if not 'error' in result:
            with open(MEDIA_PATH, "w") as f:
                f.write(json.dumps(result))
            return True
        return False

    #POST: 광고 스케쥴링
    def reqSchedule(self, scheduled_data):
        URL = "https://adsche.skplanet.com/api/schedule"

        logger.info(json.dumps(scheduled_data))
        logger.info(urllib.urlencode({"data": json.dumps(scheduled_data)}))
        result = self.sendReq(URL, {"data": scheduled_data})
        time.sleep(0.21)

        SCHEDULE_PATH = DATA_PATH + ("Schedule_%d.json" % current_turn)
        SCHEDULE_REQ_PATH = DATA_PATH + ("Schedule_Req_%d.json" % current_turn)

        prev_schedule = []
        prev_last_timeSeq = -1
        if os.path.isfile(SCHEDULE_PATH):
            with open(SCHEDULE_PATH, "r") as f:
                prev_schedule = \
                    [json.loads(line.strip()) for line in f.readlines()]
                prev_last_timeSeq = prev_schedule[-1]['timeSeq']
        if 'error' in result:
            if result['error']['code'] == 40014 or \
               result['error']['code'] == 40015:
                #턴의 종료
                pass
            else:
                #error상황
                pass

        #turnNo = result['turnNo']
        timeSeq = result['timeSeq']
        logger.info(json.dumps(result))
        if timeSeq > prev_last_timeSeq:
            with open(SCHEDULE_PATH, "a") as f:
                f.write(json.dumps(result) + "\n")
            with open(SCHEDULE_REQ_PATH, "a") as f:
                f.write(json.dumps(scheduled_data) + "\n")
            return True

        return False


class CSR2_Scheduler(object):
    def __init__(self, turn, ad, media):
        self.ad = ad['data']
        self.media = media['data']
        self.turn = turn
        logger.info(media)
        logger.info(ad)

        #load schedule by turn

    def schedule1(self):  # only paid
        def get_next_add(fill_rate, max_impression_count):
            ad_result = []
            for idx, _ad in enumerate(self.ad):
                if _ad["impressionCount"] >= max_impression_count:
                    ad_result.append({
                        "adNo": _ad["adNo"],
                        "putCount": max_impression_count
                    })
                    break
                else:
                    max_impression_count -= _ad["impressionCount"]
                    ad_result.append({
                        "adNo": _ad["adNo"],
                        "putCount": _ad["impressionCount"]
                    })

            return ad_result  # todo

        schedule_result = []
        for idx, _media in enumerate(self.media):
            fill_rate = _media["fillRate"]
            max_ad_count = _media["maxImpressionCountPerRequest"]
            next_add = get_next_add(fill_rate, max_ad_count)

            schedule_result.append({
                "mediaNo": _media["mediaNo"],
                "adPutRequest": next_add,
            })

        return schedule_result

    def schedule2(self):
        pass

    def subtract(self, scheduled):
        #ad - scheduled
        pass

current_turn = 1

AD_PATH = DATA_PATH + ("AdList_%d.json" % current_turn)
MEDIA_PATH = DATA_PATH + ("MediaList_%d.json" % current_turn)

with open("current_turn", "r") as f:
    current_turn = int(f.readline())

tester = CSR2_API("e2e0b5ee4f5f18bd29d17b704223a5de")
if not tester.reqStartNewTurn():
    logger.error("reqStartNewTurn fail")
    sys.exit(0)
if not tester.reqAdList():
    logger.error("reqAdList fail")
    sys.exit(0)
if not tester.reqMediaList():
    logger.error("reqMediaList fail")
    sys.exit(0)


with open(AD_PATH) as f:
    current_ad = json.loads(f.readline().strip())
with open(MEDIA_PATH) as f:
    current_media = json.loads(f.readline().strip())

tester_scheduler = CSR2_Scheduler(current_turn, current_ad, current_media)

next_schedule = tester_scheduler.schedule1()
if tester.reqSchedule(next_schedule):
    tester_scheduler.subtract(next_schedule)
