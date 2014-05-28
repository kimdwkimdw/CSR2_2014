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
import numpy as np
import pickle

logPath, logFileName = "log", "5_26_real"
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
        self.p_req_time = time.time()
        self.p_compute_time = time.time()

    def sendReq(self, URL, params=None):
        append_data = json.dumps(params) if not params is None else None
        req = urllib2.Request(URL, append_data)
        req.add_header('X-Auth-Token', self.token)

        r = None
        #start_time = time.time()
        while True:
            try:
                cur = time.time()
                to_be_sleep_time = max(0.25 - (cur - self.p_req_time), 0)
                logger.info("TIME %f %f" % (cur - self.p_req_time,
                                            cur - self.p_compute_time))
                time.sleep(to_be_sleep_time)
                self.p_req_time = time.time()
                r = urllib2.urlopen(req)
                self.p_compute_time = time.time()
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
                    logger.info("ERROR %d" % result['error']['code'])
                    #to_be_sleep_time = max(0.2 - (time.time() - start_time),0)
                    #time.sleep(to_be_sleep_time)
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

        SCHEDULE_PATH = DATA_PATH + ("real_Schedule_%d.json" % self.turn)
        SCHEDULE_REQ_PATH = DATA_PATH + \
            ("real_Schedule_Req_%d.json" % self.turn)

        #prev_schedule = []
        prev_last_timeSeq = -1
        # if os.path.isfile(SCHEDULE_PATH):
        #     with open(SCHEDULE_PATH, "r") as f:
        #         prev_schedule = \
        #             [json.loads(line.strip()) for line in f.readlines()]
        #         prev_last_timeSeq = prev_schedule[-1]['timeSeq']
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
            #logger.info("TIMEreqSche %f"% (time.time() - self.p_compute_time))
            return result

        return False


class CSR2_API_Local(object):
    def __init__(self, token):
        self.token = token
        self.turn = 1
        self.p_req_time = time.time()
        self.p_compute_time = time.time()

    def sendReq(self, URL, params=None):
        pass

    #GET: 턴 시작
    def reqStartNewTurn(self):
        pass

    #기초 데이터 획득
    #GET:광고 목록
    def reqAdList(self, cache=True):
        pass

    #GET:미디어 목록
    def reqMediaList(self):
        pass

    #POST: 광고 스케쥴링
    def reqSchedule(self, scheduled_data):
        pass


class CSR2_Scheduler(object):
    def __init__(self, turn, ad, media):
        self.ad = ad['data']
        self.media = media['data']
        self.turn = turn

        self.prev_media_ratio = [[0 for i in range(len(self.media) + 1)]]
        if os.path.isfile("prev_media_ratio_%d.pk" % turn):
            with open("prev_media_ratio_%d.pk" % turn, 'rb') as inp:
                self.prev_media_ratio = pickle.load(inp)

        self.startSeq = 1
        self.startSeq = len(self.prev_media_ratio)
        print self.startSeq

    def schedule1(self):  # only paid
        def get_next_add(fill_rate, max_impression_count):
            #rand_int = np.random.randint(30, 70)
            #max_impression_count = int(max_impression_count * rand_int /100.0)
            ad_result = []
            for idx, _ad in enumerate(self.ad):
            #for idx in [self.req_count]:
                _ad = self.ad[idx]
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
        paid_ad_idx_list = []
        free_ad_idx_list = []
        for idx, _ad in enumerate(self.ad):
            if _ad["impressionCount"] > 0:
                if _ad["adCost"] > 0:
                    paid_ad_idx_list.append(idx)
                else:
                    free_ad_idx_list.append(idx)

        def get_next_add(fill_rate, max_impression_count):
            paid_ad_cnt = int(fill_rate * max_impression_count / 100.0)
            free_ad_cnt = max_impression_count - paid_ad_cnt
            ad_result = []

            ad_count_dict = {}
            while paid_ad_cnt > 0 and len(paid_ad_idx_list) > 0:
                len_paid_ad = xrange(len(paid_ad_idx_list))
                random_choice = np.random.choice(len_paid_ad, paid_ad_cnt)
                random_choice = np.bincount(random_choice)
                to_be_removed = []
                for random_idx, v in enumerate(random_choice):
                    selected_idx = paid_ad_idx_list[random_idx]
                    if self.ad[selected_idx]["impressionCount"] == 0:
                        continue
                    elif self.ad[selected_idx]["impressionCount"] >= v:
                        ad_cnt_diff = v
                    else:
                        ad_cnt_diff = self.ad[selected_idx]["impressionCount"]

                    ad_count_dict[selected_idx] = ad_cnt_diff
                    self.ad[selected_idx]["impressionCount"] -= ad_cnt_diff
                    paid_ad_cnt -= ad_cnt_diff

                    if self.ad[selected_idx]["impressionCount"] == 0:
                        to_be_removed.append(selected_idx)

                for ridx in to_be_removed:
                    paid_ad_idx_list.remove(ridx)

            while free_ad_cnt > 0 and len(free_ad_idx_list) > 0:
                len_free_ad = xrange(len(free_ad_idx_list))
                random_choice = np.random.choice(len_free_ad, free_ad_cnt)
                random_choice = np.bincount(random_choice)
                to_be_removed = []
                for random_idx, v in enumerate(random_choice):
                    selected_idx = free_ad_idx_list[random_idx]
                    if self.ad[selected_idx]["impressionCount"] == 0:
                        continue
                    elif self.ad[selected_idx]["impressionCount"] >= v:
                        ad_cnt_diff = v
                    else:
                        ad_cnt_diff = self.ad[selected_idx]["impressionCount"]

                    ad_count_dict[selected_idx] = ad_cnt_diff
                    self.ad[selected_idx]["impressionCount"] -= ad_cnt_diff
                    free_ad_cnt -= ad_cnt_diff

                    if self.ad[selected_idx]["impressionCount"] == 0:
                        to_be_removed.append(selected_idx)

                for ridx in to_be_removed:
                    free_ad_idx_list.remove(ridx)

            for ad_idx in ad_count_dict:
                if int(ad_count_dict[ad_idx]) == 0:
                    continue
                ad_result.append({
                    "adNo": self.ad[ad_idx]["adNo"],
                    "putCount": int(ad_count_dict[ad_idx]),
                })

            return ad_result  # todo

        schedule_result = []
        for idx, _media in enumerate(self.media):
            fill_rate = _media["fillRate"]
            max_ad_count = _media["maxImpressionCountPerRequest"]
            next_add = get_next_add(fill_rate, max_ad_count)
            if len(next_add) == 0:
                continue
            schedule_result.append({
                "mediaNo": _media["mediaNo"],
                "adPutRequest": next_add,
            })

        return schedule_result

    def schedule3(self):
        paid_ad_idx_list = []
        free_ad_idx_list = []
        for idx, _ad in enumerate(self.ad):
            if _ad["impressionCount"] > 0:
                if _ad["adCost"] > 0:
                    paid_ad_idx_list.append(idx)
                else:
                    free_ad_idx_list.append(idx)

        def get_next_add(fill_rate, max_impression_count):
            max_impression_count = int(max_impression_count * 0.8)
            paid_ad_cnt = int(fill_rate * max_impression_count / 100.0) + 1
            free_ad_cnt = max_impression_count - paid_ad_cnt
            ad_result = []

            ad_count_dict = {}
            for paid_ad_idx in paid_ad_idx_list:
                _ad = self.ad[paid_ad_idx]
                if _ad["impressionCount"] >= paid_ad_cnt:
                    ad_count_dict[paid_ad_idx] = paid_ad_cnt
                    _ad["impressionCount"] -= paid_ad_cnt
                    break
                elif _ad["impressionCount"] > 0:
                    paid_ad_cnt -= _ad["impressionCount"]
                    ad_count_dict[paid_ad_idx] = _ad["impressionCount"]
                    _ad["impressionCount"] = 0
                else:  # 0
                    continue

            for free_ad_idx in free_ad_idx_list:
                _ad = self.ad[free_ad_idx]
                if _ad["impressionCount"] >= free_ad_cnt:
                    ad_count_dict[free_ad_idx] = free_ad_cnt
                    _ad["impressionCount"] -= free_ad_cnt
                    break
                elif _ad["impressionCount"] > 0:
                    free_ad_cnt -= _ad["impressionCount"]
                    ad_count_dict[free_ad_idx] = _ad["impressionCount"]
                    _ad["impressionCount"] = 0
                else:  # 0
                    continue

            for ad_idx in ad_count_dict:
                ad_result.append({
                    "adNo": self.ad[ad_idx]["adNo"],
                    "putCount": int(ad_count_dict[ad_idx]),
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

    def schedule4(self):
        paid_ad_idx_list = []
        paid_ad_cost_list = []
        free_ad_idx_list = []
        for idx, _ad in enumerate(self.ad):
            if _ad["impressionCount"] > 0:
                if _ad["adCost"] > 0:
                    paid_ad_idx_list.append(idx)
                    paid_ad_cost_list.append(_ad["adCost"])
                else:
                    free_ad_idx_list.append(idx)

        def get_next_add(fill_rate, max_impression_count):
            if len(paid_ad_idx_list) == 0 or len(free_ad_idx_list) == 0:
                return []
            paid_ad_cnt = int(fill_rate * max_impression_count / 100.0)
            free_ad_cnt = max_impression_count - paid_ad_cnt
            ad_result = []

            ad_count_dict = {}
            while paid_ad_cnt > 0 and len(paid_ad_idx_list) > 0:
                len_paid_ad = xrange(len(paid_ad_idx_list))
                prob = np.array(paid_ad_cost_list) / \
                    np.float(sum(paid_ad_cost_list))
                random_choice = np.random.choice(len_paid_ad,
                                                 paid_ad_cnt,
                                                 p=prob)
                random_choice = np.bincount(random_choice)
                to_be_removed = []
                for random_idx, v in enumerate(random_choice):
                    selected_idx = paid_ad_idx_list[random_idx]
                    if self.ad[selected_idx]["impressionCount"] == 0:
                        continue
                    elif self.ad[selected_idx]["impressionCount"] >= v:
                        ad_cnt_diff = v
                    else:
                        ad_cnt_diff = self.ad[selected_idx]["impressionCount"]

                    ad_count_dict[selected_idx] = ad_cnt_diff
                    self.ad[selected_idx]["impressionCount"] -= ad_cnt_diff
                    paid_ad_cnt -= ad_cnt_diff

                    if self.ad[selected_idx]["impressionCount"] == 0:
                        to_be_removed.append(selected_idx)

                for ridx in to_be_removed:
                    _idx = paid_ad_idx_list.index(ridx)
                    paid_ad_idx_list.pop(_idx)
                    paid_ad_cost_list.pop(_idx)

            while free_ad_cnt > 0 and len(free_ad_idx_list) > 0:
                len_free_ad = xrange(len(free_ad_idx_list))
                random_choice = np.random.choice(len_free_ad, free_ad_cnt)
                random_choice = np.bincount(random_choice)
                to_be_removed = []
                for random_idx, v in enumerate(random_choice):
                    selected_idx = free_ad_idx_list[random_idx]
                    if self.ad[selected_idx]["impressionCount"] == 0:
                        continue
                    elif self.ad[selected_idx]["impressionCount"] >= v:
                        ad_cnt_diff = v
                    else:
                        ad_cnt_diff = self.ad[selected_idx]["impressionCount"]

                    ad_count_dict[selected_idx] = ad_cnt_diff
                    self.ad[selected_idx]["impressionCount"] -= ad_cnt_diff
                    free_ad_cnt -= ad_cnt_diff

                    if self.ad[selected_idx]["impressionCount"] == 0:
                        to_be_removed.append(selected_idx)

                for ridx in to_be_removed:
                    free_ad_idx_list.remove(ridx)

            if (paid_ad_cnt > 0 and len(paid_ad_idx_list) == 0) or \
               (free_ad_cnt > 0 and len(free_ad_idx_list) == 0):
                return []

            for ad_idx in ad_count_dict:
                if int(ad_count_dict[ad_idx]) == 0:
                    continue
                ad_result.append({
                    "adNo": self.ad[ad_idx]["adNo"],
                    "putCount": int(ad_count_dict[ad_idx]),
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

    def schedule5(self):
        paid_ad_idx_list = []
        free_ad_idx_list = []
        for idx, _ad in enumerate(self.ad):
            if _ad["impressionCount"] > 0:
                if _ad["adCost"] > 0:
                    paid_ad_idx_list.append(idx)
                else:
                    free_ad_idx_list.append(idx)

        def get_next_add(fill_rate, max_impression_count, mediaNo):

            if len(paid_ad_idx_list) == 0 or len(free_ad_idx_list) == 0:
                return []
            paid_ad_cnt = int(fill_rate * max_impression_count / 100.0)
            free_ad_cnt = max_impression_count - paid_ad_cnt
            ad_result = []

            ad_count_dict = {}

            for paid_ad_idx in paid_ad_idx_list:
                _ad = self.ad[paid_ad_idx]
                if _ad["impressionCount"] >= paid_ad_cnt:
                    ad_count_dict[paid_ad_idx] = paid_ad_cnt
                    _ad["impressionCount"] -= paid_ad_cnt
                    paid_ad_cnt = 0
                    break
                elif _ad["impressionCount"] > 0:
                    paid_ad_cnt -= _ad["impressionCount"]
                    ad_count_dict[paid_ad_idx] = _ad["impressionCount"]
                    _ad["impressionCount"] = 0
                else:  # 0
                    continue
            if paid_ad_cnt > 0:
                free_ad_cnt -= int(paid_ad_cnt * (1.0 / fill_rate - 1))

            for free_ad_idx in free_ad_idx_list:
                _ad = self.ad[free_ad_idx]
                if _ad["impressionCount"] >= free_ad_cnt:
                    ad_count_dict[free_ad_idx] = free_ad_cnt
                    _ad["impressionCount"] -= free_ad_cnt
                    break
                elif _ad["impressionCount"] > 0:
                    free_ad_cnt -= _ad["impressionCount"]
                    ad_count_dict[free_ad_idx] = _ad["impressionCount"]
                    _ad["impressionCount"] = 0
                else:  # 0
                    continue

            for ad_idx in ad_count_dict:
                if int(ad_count_dict[ad_idx]) == 0:
                    continue
                ad_result.append({
                    "adNo": self.ad[ad_idx]["adNo"],
                    "putCount": int(ad_count_dict[ad_idx]),
                })

            return ad_result  # todo

        schedule_result = []
        for idx, _media in enumerate(self.media):
            fill_rate = _media["fillRate"]
            max_ad_count = _media["maxImpressionCountPerRequest"]
            next_add = get_next_add(fill_rate, max_ad_count, _media["mediaNo"])

            schedule_result.append({
                "mediaNo": _media["mediaNo"],
                "adPutRequest": next_add,
            })

        return schedule_result

    def real_schedule1(self):
        paid_ad_idx_list = []
        free_ad_idx_list = []
        for idx, _ad in enumerate(self.ad):
            if _ad["impressionCount"] > 0:
                if _ad["adCost"] > 0:
                    paid_ad_idx_list.append(idx)
                else:
                    free_ad_idx_list.append(idx)

        def get_next_add(fill_rate, max_impression_count, mediaNo):
            #paid_ad_cnt = int(fill_rate * max_impression_count / 100.0)
            #free_ad_cnt = max_impression_count - paid_ad_cnt
            ad_result = []

            real_paid_ad_cnt = 0
            target_ad_list = self.media_table[mediaNo - 1]
            for target_ad, imp_count in target_ad_list:
                imp_count /= 10000
                _ad = self.ad[target_ad - 1]
                ad_result.append({
                    "adNo": _ad["adNo"],
                    "putCount": imp_count
                })
                _ad["impressionCount"] -= imp_count
                real_paid_ad_cnt += imp_count

            if len(ad_result) == 0:
                return []

            real_free_ad_cnt = int(real_paid_ad_cnt
                                   * (100 / fill_rate - 1) + 0.5)
            for idx in free_ad_idx_list:
                _ad = self.ad[idx]
                if _ad["impressionCount"] >= real_free_ad_cnt:
                    ad_result.append({
                        "adNo": _ad["adNo"],
                        "putCount": real_free_ad_cnt
                    })
                    _ad["impressionCount"] -= real_free_ad_cnt
                    break
                elif _ad["impressionCount"] > 0:
                    real_free_ad_cnt -= _ad["impressionCount"]
                    ad_result.append({
                        "adNo": _ad["adNo"],
                        "putCount": _ad["impressionCount"]
                    })
                    _ad["impressionCount"] = 0
                else:  # 0
                    continue

            return ad_result

        schedule_result = []
        for idx, _media in enumerate(self.media):
            fill_rate = _media["fillRate"]
            max_ad_count = _media["maxImpressionCountPerRequest"]
            next_add = get_next_add(fill_rate, max_ad_count, _media["mediaNo"])

            schedule_result.append({
                "mediaNo": _media["mediaNo"],
                "adPutRequest": next_add,
            })

        return schedule_result

    def real_training(self):
        paid_ad_idx_list = []
        for idx, _ad in enumerate(self.ad):
            if _ad["impressionCount"] > 0:
                if _ad["adCost"] > 0:
                    paid_ad_idx_list.append(idx)

        def get_next_add(fill_rate, max_impression_count, mediaNo):
            ad_result = []

            target_idx = (self.startSeq + mediaNo - 1) % 16
            if target_idx >= 8:
                target_idx += 4  # 9-12구간 보정

            _ad = self.ad[target_idx]
            putCount = 1000
            if max_impression_count >= 1e4 and target_idx >= 12:
                putCount = 10000
            if _ad["impressionCount"] < putCount:
                putCount = _ad["impressionCount"]

            ad_result.append({
                "adNo": _ad["adNo"],
                "putCount": putCount
            })
            _ad["impressionCount"] -= putCount

            return ad_result

        schedule_result = []
        for idx, _media in enumerate(self.media):
            fill_rate = _media["fillRate"]
            max_ad_count = _media["maxImpressionCountPerRequest"]
            next_add = get_next_add(fill_rate, max_ad_count, _media["mediaNo"])

            schedule_result.append({
                "mediaNo": _media["mediaNo"],
                "adPutRequest": next_add,
            })

        return schedule_result

    def real_schedule2(self):
        paid_ad_idx_list = []
        free_ad_idx_list = []
        for idx, _ad in enumerate(self.ad):
            if _ad["impressionCount"] > 0:
                if _ad["adCost"] > 0:
                    paid_ad_idx_list.append(idx)
                else:
                    free_ad_idx_list.append(idx)

        def get_next_add(fill_rate, max_impression_count, mediaNo, seqNum):
            req_item_list = [(item[1], item[4]) for item in self.new_rank_table
                             if item[0] == mediaNo
                             and seqNum >= item[2] and seqNum <= item[3]]
            ad_result = []
            real_paid_ad_cnt = 0
            for idx, imp_count in req_item_list:
                _ad = self.ad[idx - 1]
                ad_result.append({
                    "adNo": _ad["adNo"],
                    "putCount": imp_count
                })
                _ad["impressionCount"] -= imp_count
                real_paid_ad_cnt += imp_count

            real_free_ad_cnt = int(real_paid_ad_cnt *
                                   (100 / fill_rate - 1) + 0.5)
            for idx in free_ad_idx_list:
                _ad = self.ad[idx]
                if _ad["impressionCount"] >= real_free_ad_cnt:
                    ad_result.append({
                        "adNo": _ad["adNo"],
                        "putCount": real_free_ad_cnt
                    })
                    _ad["impressionCount"] -= real_free_ad_cnt
                    break
                elif _ad["impressionCount"] > 0:
                    real_free_ad_cnt -= _ad["impressionCount"]
                    ad_result.append({
                        "adNo": _ad["adNo"],
                        "putCount": _ad["impressionCount"]
                    })
                    _ad["impressionCount"] = 0
                else:  # 0
                    continue

            print (real_paid_ad_cnt /
                  (float(real_free_ad_cnt) + real_paid_ad_cnt))
            return ad_result

        schedule_result = []
        for idx, _media in enumerate(self.media):
            fill_rate = _media["fillRate"]
            max_ad_count = _media["maxImpressionCountPerRequest"]
            next_add = get_next_add(fill_rate, max_ad_count,
                                    _media["mediaNo"], self.startSeq)

            schedule_result.append({
                "mediaNo": _media["mediaNo"],
                "adPutRequest": next_add,
            })

        return schedule_result

    def real_schedule3(self):
        #self.startSeq 를 바탕으로 schedule
        #prev_media_ratio
        paid_ad_idx_list = []
        free_ad_idx_list = []
        for idx, _ad in enumerate(self.ad):
            if _ad["impressionCount"] > 0:
                if _ad["adCost"] > 0:
                    paid_ad_idx_list.append(idx)
                else:
                    free_ad_idx_list.append(idx)

        def get_next_add(fill_rate, max_impression_count, mediaNo, seqNum):
            prev_ratio_ll = self.prev_media_ratio[-10:]
            media_ratio_detail_max = np.max([ratio_elem[mediaNo]
                                             for ratio_elem in prev_ratio_ll])
            media_ratio_detail_min = np.min([ratio_elem[mediaNo]
                                             for ratio_elem in prev_ratio_ll])
            media_ratio = self.prev_media_ratio[-1][mediaNo]
            if media_ratio >= 0.04:
                if media_ratio_detail_max > 0.060 \
                   and media_ratio_detail_min >= 0.050:
                    req_item_list = range(1, 5) + \
                        range(5, 9) + \
                        range(13, 17) + \
                        range(17, 21)
                else:
                    req_item_list = range(5, 9) + \
                        range(13, 17) + \
                        range(1, 5) + \
                        range(17, 21)
            elif media_ratio >= 0.02:
                req_item_list = range(13, 17) + \
                    range(17, 21) + \
                    range(5, 9)
                if seqNum > 9000:
                    req_item_list = range(13, 17) + \
                        range(1, 5) + \
                        range(17, 21) + \
                        range(5, 9)
            else:
                req_item_list = range(17, 21)

            ad_result = []
            real_paid_ad_cnt = int(max_impression_count * fill_rate / 100.0)
            if media_ratio <= 0.0006:
                real_paid_ad_cnt = int(real_paid_ad_cnt * 0.9)
            elif media_ratio <= 0.0011:
                real_paid_ad_cnt = int(real_paid_ad_cnt)

            for ad_num in req_item_list:
                _ad = self.ad[ad_num - 1]
                if _ad["impressionCount"] >= real_paid_ad_cnt:
                    ad_result.append({
                        "adNo": _ad["adNo"],
                        "putCount": real_paid_ad_cnt
                    })
                    _ad["impressionCount"] -= real_paid_ad_cnt
                    break
                elif _ad["impressionCount"] > 0:
                    real_paid_ad_cnt -= _ad["impressionCount"]
                    ad_result.append({
                        "adNo": _ad["adNo"],
                        "putCount": _ad["impressionCount"]
                    })
                    _ad["impressionCount"] = 0
                else:  # 0
                    continue

            real_free_ad_cnt = int(real_paid_ad_cnt *
                                   (100 / fill_rate - 1) + 0.5)
            for idx in free_ad_idx_list:
                _ad = self.ad[idx]
                if _ad["impressionCount"] >= real_free_ad_cnt:
                    ad_result.append({
                        "adNo": _ad["adNo"],
                        "putCount": real_free_ad_cnt
                    })
                    _ad["impressionCount"] -= real_free_ad_cnt
                    break
                elif _ad["impressionCount"] > 0:
                    real_free_ad_cnt -= _ad["impressionCount"]
                    ad_result.append({
                        "adNo": _ad["adNo"],
                        "putCount": _ad["impressionCount"]
                    })
                    _ad["impressionCount"] = 0
                else:  # 0
                    continue

            return ad_result

        schedule_result = []
        for idx, _media in enumerate(self.media):
            fill_rate = _media["fillRate"]
            max_ad_count = _media["maxImpressionCountPerRequest"]
            next_add = get_next_add(fill_rate, max_ad_count,
                                    _media["mediaNo"], self.startSeq)

            schedule_result.append({
                "mediaNo": _media["mediaNo"],
                "adPutRequest": next_add,
            })

        return schedule_result

    def validate(self, api):
        logger.info("validate")
        api.reqAdList(False)
        with open(AD_PATH % self.turn) as f:
            new_ad = json.loads(f.readline().strip())["data"]

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

    def calculate_media_ratio(self, scheduled, request_result):
        request_result = request_result["data"] + []
        media_ratio = [0 for i in range(len(self.media) + 1)]
        for elem in scheduled:
            mediaNo = elem["mediaNo"]
            result = [req_elem for req_elem in request_result
                      if req_elem["mediaNo"] == mediaNo][0]
            elem["adPutRequest"].sort(key=lambda x: x["adNo"])
            result["adClickResult"].sort(key=lambda x: x["adNo"])
            ratio = 0
            for click_result, put_request in zip(result["adClickResult"],
                                                 elem["adPutRequest"]):
                if put_request["putCount"] > 0:
                    ratio += click_result["clickCount"] \
                        / float(put_request["putCount"])
            if len(result["adClickResult"]) > 0:
                ratio /= float(len(result["adClickResult"]))
            media_ratio[mediaNo] = ratio

        return media_ratio

AD_PATH = DATA_PATH + "real_AdList_%d.json"
MEDIA_PATH = DATA_PATH + "real_MediaList_%d.json"
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

        next_schedule = tester_scheduler.real_schedule3()
        if len(next_schedule) == 0:
            break
        while True:
            current_result = tester.reqSchedule(next_schedule)
            current_seq = current_result["timeSeq"]
            if not current_seq is False:
                tester_scheduler.prev_media_ratio.append(
                    tester_scheduler.calculate_media_ratio(next_schedule,
                                                           current_result))
                if i % 100 == 0:
                    with open("prev_media_ratio_%d.pk" % turn, "wb") as output:
                        pickle.dump(tester_scheduler.prev_media_ratio, output,
                                    pickle.HIGHEST_PROTOCOL)
                break
        if current_seq == END_SEQ:
            break
        i += 1
        tester_scheduler.startSeq += 1
        #break
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
            tester.turn = current_turn
            with open("current_turn", "w") as f:
                f.write(str(current_turn))

    tryMain(current_turn)
    break
