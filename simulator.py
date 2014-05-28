# -*- coding: utf-8 -*-
import pickle
import json
import numpy as np


class Simulator(object):
    def __init__(self, ad, media, rank_table):
        self.ad = ad["data"]
        self.media = media["data"]
        self.rank_table = rank_table + []
        self.prev_media_ratio = [[0 for i in range(len(self.media) + 1)]]
        self.startSeq = 1

    def request(self, scheduled_result):
        #  by self.rank_table
        req_seq = self.startSeq
        return_list = []

        for schedule_item in scheduled_result:
            media_no = schedule_item["mediaNo"]
            click_ratio = [rank_item[3] for rank_item in self.rank_table
                           if rank_item[0] == media_no
                           and req_seq >= rank_item[1]
                           and req_seq <= rank_item[2]][0]
            return_item = []
            for put_item in schedule_item["adPutRequest"]:
                return_item.append({
                    "clickCount": int(click_ratio * put_item["putCount"]),
                    "adNo": put_item["adNo"]})
            return_list.append({
                "mediaNo": media_no,
                "adClickResult": return_item})

        return {"turnNo": 1, "data": return_list, "timeSeq": self.startSeq}

    def schedule(self):
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
            media_ratio_detail_max = np.max([ratio_elem[mediaNo] for ratio_elem in self.prev_media_ratio[-3:]])
            media_ratio_detail_min = np.min([ratio_elem[mediaNo] for ratio_elem in self.prev_media_ratio[-3:]])
            media_ratio = self.prev_media_ratio[-1][mediaNo]
            if media_ratio >= 0.04:
                if media_ratio_detail_max >= 0.060 \
                   and media_ratio_detail_min >= 0.050:
                    req_item_list = range(1, 5) + \
                        range(5, 9) + \
                        range(13, 17) + \
                        range(17, 21)
                else:
                    req_item_list = range(5, 9) + \
                        range(13, 17) + \
                        range(17, 21)
            elif media_ratio >= 0.02:
                req_item_list = range(13, 17) + \
                    range(17, 21) + \
                    range(5, 9)
            else:
                req_item_list = range(17, 21)

            ad_result = []
            real_paid_ad_cnt = int(max_impression_count * fill_rate / 100.0)
            if media_ratio <= 0.0006:
                real_paid_ad_cnt = int(real_paid_ad_cnt * 0.7)
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

    def calculate_cost(self, request_result):
        result = request_result["data"] + []
        cost = 0

        for req_elem in result:
            #media_no = req_elem["mediaNo"]
            for click_elem in req_elem["adClickResult"]:
                cost += self.ad[click_elem["adNo"] - 1]["adCost"] \
                    * int(click_elem["clickCount"])
        return cost

    def simulate(self):
        e_cost = 0
        while self.startSeq <= 10000:
            scheduled_result = self.schedule()
            request_result = self.request(scheduled_result)

            profit = self.calculate_cost(request_result)
            e_cost += profit
            print e_cost
            if (self.startSeq % 3700 == 0):
                break

            #calculate
            self.prev_media_ratio.append(
                self.calculate_media_ratio(scheduled_result, request_result))
            self.startSeq += 1
        print self.ad

DATA_PATH = "data/"
AD_PATH = DATA_PATH + "AdList_%d - Copy.json"
MEDIA_PATH = DATA_PATH + "MediaList_%d - Copy.json"
# SCHEDULE_PATH = DATA_PATH + "Schedule_%d.json"
# SCHEDULE_REQ_PATH = DATA_PATH + "Schedule_Req_%d.json"
SCHEDULE_PATH = DATA_PATH + "real_Schedule_%d.json"
SCHEDULE_REQ_PATH = DATA_PATH + "real_Schedule_Req_%d.json"


def read_json_list(PATH):
    return_list = []
    with open(PATH) as f:
        for idx, line in enumerate(f.readlines()):
            return_list.append(json.loads(line.strip()))
    return return_list


def loadSchedule(turn):
    CURRENT_SCHEDULE_PATH = SCHEDULE_PATH % turn
    CURRENT_SCHEDULE_REQ_PATH = SCHEDULE_REQ_PATH % turn

    s_list = read_json_list(CURRENT_SCHEDULE_PATH)
    s_req_list = read_json_list(CURRENT_SCHEDULE_REQ_PATH)

    return (s_list, s_req_list)


def loadAdMedia(turn):
    CURRENT_AD_PATH = AD_PATH % turn
    CURRENT_MEDIA_PATH = MEDIA_PATH % turn

    ad_list = read_json_list(CURRENT_AD_PATH)
    media_list = read_json_list(CURRENT_MEDIA_PATH)

    return (ad_list[0], media_list[0])

with open("rank_table.pk", 'rb') as inp:
    rank_table = pickle.load(inp)


ad, media = loadAdMedia(1)

simulator = Simulator(ad, media, rank_table)
simulator.simulate()
