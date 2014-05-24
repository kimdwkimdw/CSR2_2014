# -*- coding: utf-8 -*-
'''
Author: Arkind
Python 2.7.6

#https://codesprint.skplanet.com/2014/participation/round2_intro.htm
'''
import urllib2
import json
import logging
import sys
import os
import time
import random
import numpy as np

logPath, logFileName = "log", "5_24"
logging.basicConfig(level=logging.INFO)
FORMAT = "%(asctime)s [%(threadName)-10.10s] [%(levelname)-5.5s]  %(message)s"
logFormatter = logging.Formatter(FORMAT)
logger = logging.getLogger("CSR2")
logger.setLevel(logging.DEBUG)

# consoleHandler = logging.StreamHandler()
# consoleHandler.setFormatter(logFormatter)
# logger.addHandler(consoleHandler)

DATA_PATH = "data/"


class CSR2_API(object):
    def __init__(self, token):
        self.token = token
        self.turn = 1

    def sendReq(self, URL, params=None):
        append_data = json.dumps(params) if not params is None else None
        req = urllib2.Request(URL, append_data)
        req.add_header('X-Auth-Token', self.token)

        r = None
        while True:
            try:
                r = urllib2.urlopen(req)
            except urllib2.HTTPError:
                logger.exception("HTTPError", exc_info=True)
                continue
            except urllib2.URLError:
                logger.exception("URLError", exc_info=True)
                continue
            except Exception:
                continue
            result = json.loads(r.readline())
            if 'error' in result:
                if result['error']['code'] == 40010 or \
                   result['error']['code'] == 40011:
                    logger.info("ERROR %d" % str(result['error']['code']))
                    time.sleep(0.25)
                    continue
            break
        time.sleep(0.20)
        logger.info("sendReq" + json.dumps(result))
        return result

    #GET: 턴 시작
    def reqStartNewTurn(self):
        URL = "https://adsche.skplanet.com/api/startNewTurn"
        result = self.sendReq(URL)
        if 'turnNo' in result or \
           'error' in result and result['error']['code'] == 40013:
            return result

        return False

    #기초 데이터 획득
    #GET:광고 목록
    def reqAdList(self, cache=True):
        URL = "https://adsche.skplanet.com/api/adList"
        CURRENT_AD_PATH = AD_PATH % self.turn

        if cache and os.path.isfile(CURRENT_AD_PATH):
            result = None
            with open(CURRENT_AD_PATH, "r") as f:
                result = json.loads(f.readline().strip())
            return result

        result = self.sendReq(URL)
        if not 'error' in result:
            with open(CURRENT_AD_PATH, "w") as f:
                f.write(json.dumps(result))
            return result
        return False

    #GET:미디어 목록
    def reqMediaList(self):
        URL = "https://adsche.skplanet.com/api/mediaList"
        CURRENT_MEDIA_PATH = MEDIA_PATH % self.turn

        if os.path.isfile(CURRENT_MEDIA_PATH):
            result = None
            with open(CURRENT_MEDIA_PATH, "r") as f:
                result = json.loads(f.readline().strip())
            return result

        result = self.sendReq(URL)
        if not 'error' in result:
            with open(CURRENT_MEDIA_PATH, "w") as f:
                f.write(json.dumps(result))
            return result
        return False

    #POST: 광고 스케쥴링
    def reqSchedule(self, scheduled_data):
        URL = "https://adsche.skplanet.com/api/schedule"

        logger.info(json.dumps(scheduled_data))
        result = self.sendReq(URL, {"data": scheduled_data})

        SCHEDULE_PATH = DATA_PATH + ("Schedule_%d.json" % self.turn)
        SCHEDULE_REQ_PATH = DATA_PATH + ("Schedule_Req_%d.json" % self.turn)

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
                return True
            elif result['error']['code'] == 40007:
                raise Exception("error", "40007")
            else:
                #error상황
                return False

        turnNo = result['turnNo']
        timeSeq = result['timeSeq']
        logger.info("SCHEDULE_SUCCESS %d, %d" % (turnNo, timeSeq))
        if timeSeq > prev_last_timeSeq:
            with open(SCHEDULE_PATH, "a") as f:
                f.write(json.dumps(result) + "\n")
            with open(SCHEDULE_REQ_PATH, "a") as f:
                f.write(json.dumps(scheduled_data) + "\n")
            return result['timeSeq']

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
                    self.ad[idx]["impressionCount"] -= max_impression_count
                    break
                elif _ad["impressionCount"] > 0:
                    max_impression_count -= _ad["impressionCount"]
                    ad_result.append({
                        "adNo": _ad["adNo"],
                        "putCount": _ad["impressionCount"]
                    })
                    self.ad[idx]["impressionCount"] = 0
                else:  # 0
                    continue

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

    def schedule2(self):  # TODO:Random
        random.choice
        paid_ad_idx_list = []
        free_ad_idx_list = []
        for idx, _ad in enumerate(self.ad):
            if _ad["impressionCount"] > 0:
                if _ad["adCost"] > 0:
                    paid_ad_idx_list.append(idx)
                else:
                    free_ad_idx_list.append(idx)

        def get_next_add(fill_rate, max_impression_count):
            paid_ad_cnt = int(fill_rate * max_impression_count / 100.0) + 1
            free_ad_cnt = max_impression_count - paid_ad_cnt
            ad_result = []

            ad_count_dict = {}
            while paid_ad_cnt > 0 and len(paid_ad_idx_list) > 0:
                len_paid_ad = xrange(len(paid_ad_idx_list))
                random_choice = np.random.choice(len_paid_ad, paid_ad_cnt)
                random_choice = np.bincount(random_choice)

                for random_idx, v in enumerate(random_choice):
                    selected_idx = paid_ad_idx_list[random_idx]
                    if self.ad[selected_idx]["impressionCount"] >= v:
                        ad_cnt_diff = v
                    else:
                        ad_cnt_diff = self.ad[selected_idx]["impressionCount"]

                    ad_count_dict[selected_idx] = ad_cnt_diff
                    self.ad[selected_idx]["impressionCount"] -= ad_cnt_diff
                    paid_ad_cnt -= ad_cnt_diff

                    if self.ad[selected_idx]["impressionCount"] == 0:
                        paid_ad_idx_list.remove(selected_idx)

            while free_ad_cnt > 0 and len(free_ad_idx_list) > 0:
                len_free_ad = xrange(len(free_ad_idx_list))
                random_choice = np.random.choice(len_free_ad, free_ad_cnt)
                random_choice = np.bincount(random_choice)

                for random_idx, v in enumerate(random_choice):
                    selected_idx = free_ad_idx_list[random_idx]
                    if self.ad[selected_idx]["impressionCount"] >= v:
                        ad_cnt_diff = v
                    else:
                        ad_cnt_diff = self.ad[selected_idx]["impressionCount"]

                    ad_count_dict[selected_idx] = ad_cnt_diff
                    self.ad[selected_idx]["impressionCount"] -= ad_cnt_diff
                    free_ad_cnt -= ad_cnt_diff

                    if self.ad[selected_idx]["impressionCount"] == 0:
                        free_ad_idx_list.remove(selected_idx)

            for ad_idx in ad_count_dict:
                ad_result.append({
                    "adNo": self.ad[ad_idx]["adNo"],
                    "putCount": ad_count_dict[ad_idx]
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

    def validate(self, api):
        logger.info("validate")
        api.reqAdList(False)
        with open(AD_PATH % self.turn) as f:
            new_ad = json.loads(f.readline().strip())['data']

        equality = self.ad == new_ad
        logger.info("self.ad " + str(self.ad))
        if not equality:
            logger.info("new_ad  " + str(new_ad))
            self.ad = new_ad
        return equality

    def subtract(self, scheduled):
        for idx, success_media in enumerate(scheduled):
            success_ad_list = success_media["adPutRequest"]
            for ad_idx, success_ad in enumerate(success_ad_list):
                adNo, putCount = success_ad["adNo"], success_ad["putCount"]
                self.ad[adNo]["impressionCount"] -= putCount

current_turn = 1

AD_PATH = DATA_PATH + "AdList_%d.json"
MEDIA_PATH = DATA_PATH + "MediaList_%d.json"
END_SEQ = 10000


def tryMain(turn):
    log_filename = "{0}/{1}_turn{2}.log".format(logPath, logFileName, turn)
    fileHandler = logging.FileHandler(log_filename)
    fileHandler.setFormatter(logFormatter)
    fileHandler.setLevel(logging.INFO)
    logger.addHandler(fileHandler)

    current_ad, current_media = tester.reqAdList(), tester.reqMediaList()
    if current_ad is False:
        logger.error("reqAdList fail")
        raise Exception("reqAdList fail")
    if current_media is False:
        logger.error("reqMediaList fail")
        raise Exception("reqAdList fail")

    tester_scheduler = CSR2_Scheduler(current_turn, current_ad, current_media)

    i = 0
    while True:
        if i % 100 == 0 and not tester_scheduler.validate(tester):
            logger.error("ERROR 40007---------------")

        next_schedule = tester_scheduler.schedule2()

        while True:
            current_seq = tester.reqSchedule(next_schedule)
            if not current_seq is False:
                break
        if current_seq == END_SEQ:
            break
        i += 1

    # remove log handler
    logger.removeHandler(fileHandler)


tester = CSR2_API("e2e0b5ee4f5f18bd29d17b704223a5de")
with open("current_turn", "r") as f:
    current_turn = int(f.readline())

while True:
    tester.turn = current_turn

    result_startNewTurn = tester.reqStartNewTurn()
    if result_startNewTurn is False:
        logger.error("reqStartNewTurn fail")
        sys.exit(0)
    if 'turnNo' in result_startNewTurn:
        if result_startNewTurn['turnNo'] != current_turn:
            current_turn = result_startNewTurn['turnNo']
            with open("current_turn", "w") as f:
                f.write(str(current_turn))

    tryMain(current_turn)
    break
