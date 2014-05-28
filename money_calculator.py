import json

DATA_PATH = "data/"
AD_PATH = DATA_PATH + "real_AdList_%d.json"
MEDIA_PATH = DATA_PATH + "real_MediaList_%d.json"
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


def calculateMapping(schedule, schedule_req):
    d = {}
    for elem, elem1 in zip(schedule, schedule_req):
        elem['data'].sort(key=lambda x: x['mediaNo'])
        for r_elem, r_elem_req in zip(elem['data'], elem1):
            r_elem['adClickResult'].sort(key=lambda x: x['adNo'])
            r_elem_req['adPutRequest'].sort(key=lambda x: x['adNo'])

            for clickResult, putRequest in zip(r_elem['adClickResult'],
                                               r_elem_req['adPutRequest']):
                key = (r_elem['mediaNo'], clickResult['adNo'])
                d.setdefault(key, []).append((clickResult['clickCount'],
                                              putRequest['putCount'],
                                              elem['timeSeq']))
    return d


d = {}
# import numpy as np
# for key in d:
#     ll = [elem[0] / float(elem[1]) for elem in d[key]]
#     v.setdefault(key[1], []).append((np.average(ll), np.std(ll), key[0]))

# for key in v:
#     v[key].sort(key=lambda x: x[0], reverse=True)
#     #d[(1,2)] media, ad = [(1000,300 )], put,click

import pickle
with open("rd3.pk", 'rb') as inp:
    rd3 = pickle.load(inp)

import pickle

with open("rd3.pk", "wb") as output:
    pickle.dump(rd3, output, pickle.HIGHEST_PROTOCOL)


def get_vbymedia(d):
    vbymedia = {}
    for key in d:
        ll = [elem[0] / float(elem[1]) for elem in d[key] if elem[1] > 0]
        vbymedia.setdefault(key[0], []).append((np.average(ll),
                                                np.std(ll),
                                                key[1]))
    for key in vbymedia:
        vbymedia[key].sort(key=lambda x: x[0], reverse=True)
        #d[(1,2)] media, ad = [(1000,300 )], put,click
    return vbymedia

'''
def get_result(vbymedia, a1=None, m1=None):

    vmediatable = dict()
    for media in vbymedia:
        vmediatable[media] = [(m[2], m[0] * ad1[m[2] - 1]["adCost"])
                                  for m in vbymedia[media]
                                  if (not m[2] in range(9, 13))]
        vmediatable[media].sort(key=lambda x: x[1], reverse=True)
        # print media, "\t",
        # for vv in vmediatable[media]:
        #     print ("(%d %.4f)," % vv),
        # print

    if a1 is None:
        a1 = [25e6 for i in range(4)] + \
            [50e6 for i in range(4)] + \
            [100e6 for i in range(4)] + \
            [250e6 for i in range(4)] + \
            [500e6 for i in range(4)]

    if m1 is None:
        m1 = [int(2e3*1e4*0.8) for i in range(10)] + \
            [int(5e3*1e4*0.85) for i in range(10)] + \
            [int(10e3*1e4*0.90) for i in range(10)] + \
            [int(20e3*1e4*0.95) for i in range(10)]

m1 = [int(2e3*0.8) for i in range(10)] + \
    [int(5e3*0.85) for i in range(10)] + \
    [int(10e3*0.90) for i in range(10)] + \
    [int(20e3*0.95) for i in range(10)]

    m_table = [vmediatable[i+1]+[] for i in range(40)]

    m_result_table = [ [] for i in range(40)]
    m_result_table2 = [ [] for i in range(40)]
    expected_cost = 0
    for i in range(40*16):

        c_max, c_idx = -1, -1
        for m_idx in range(40):
            if len(m_table[m_idx]) > 0 and m_table[m_idx][0][1] > c_max:
                c_max = m_table[m_idx][0][1]
                c_idx = m_idx
        ad_idx, exp_v = m_table[c_idx].pop(0)
        if a1[ad_idx-1] == 0 or m1[c_idx] == 0:
            continue

        #m_result_table[c_idx].append(ad_idx)
        m_result_table2[c_idx].append((ad_idx, exp_v))
        if m1[c_idx] >= a1[ad_idx-1]:
            expected_cost += exp_v * a1[ad_idx-1]
            m1[c_idx] -= a1[ad_idx - 1]
            m_result_table[c_idx].append((ad_idx, a1[ad_idx-1]))
            a1[ad_idx-1] = 0
        else:  # m1[c_idx] < a1[ad_idx-1]:
            expected_cost += exp_v * m1[c_idx]
            a1[ad_idx-1] -= m1[c_idx]
            m_result_table[c_idx].append((ad_idx, m1[c_idx]))
            m1[c_idx] = 0
    return vmediatable, m_result_table, m_result_table2, expected_cost
'''

rschedule3, rschedule_req3 = loadSchedule(3)
rd3 = calculateMapping(rschedule3, rschedule_req3)
rvbymedia3 = get_vbymedia(rd3)


import matplotlib.pyplot as plt
import numpy as np


def find_group_number(group, value):
    for idx, elem in enumerate(group):
        if elem[0] <= value and value <= elem[1]:
            return idx


def find_group(bin_count_arr, bin_size=1):
    def check_zero(arr, idx):
        for ii in range(idx, idx + bin_size):
            if arr[ii] != 0:
                return False
        return True

    g = []
    found_none_zero = False
    start = -1
    for i in range(0, len(bin_count_arr), bin_size):
        if found_none_zero is False:
            if check_zero(bin_count_arr, i) is False:
                start = i
                found_none_zero = True
        else:
            if check_zero(bin_count_arr, i) is True:
                g.append((start, i - 1))
                found_none_zero = False
            if i + bin_size >= len(bin_count_arr):
                g.append((start, len(bin_count_arr)))
                found_none_zero = False
    return g


x = []
y = []
plt.plot(x, y, 'bo')
plt.show()


def draw_plot(rd_elem):
    y = np.array([v[0] for v in rd_elem])
    x = np.array(range(len(y)))
    plt.plot(x, y, 'bo')
    plt.show()

inflection_points = {}
for key in rd3:
    if key[1] == 9:
        continue
    #print key, len(rd3[key])
    tmp1 = np.bincount([v[0] for v in rd3[key] if v[1] == rd3[key][300][1]])
    bin_size = 1
    while True:
        group = find_group(tmp1, bin_size)
        find_diff_point = []
        prev_group = -1
        for i in range(len(rd3[key])):
            now_group = find_group_number(group, rd3[key][i][0])
            #print now_group,
            if prev_group != now_group and not now_group is None:
                find_diff_point.append((rd3[key][i][2],
                                        np.average(group[now_group]) / float(rd3[key][i][1]),
                                        now_group))
            prev_group = now_group

        if len(find_diff_point) > 5:
            bin_size += 1
        else:
            print key, len(find_diff_point), bin_size
            inflection_points[key] = (find_diff_point, bin_size)
            break

def make_figures(table):
    '''
    for j in range(1,40+1):
        for i in range(1,20+1):
            key = (j,i)
            if key in table and i != 9:
                plt.plot([v[2] for v in table[key]],
                         [v[0] for v in table[key]])
        plt.savefig("plot4/media_size_%02d.jpg" % j)
        plt.close()
    '''
    for j in range(1,40+1):
        for i in range(1,20+1):
            key = (j,i)
            if key in table and i != 9:
                plt.plot([v[2] for v in table[key] if v[1]>0],
                         [v[0]/float(v[1]) for v in table[key] if v[1]>0])
        plt.savefig("plot4/media_ratio_%02d.jpg" % j)
        plt.close()

prob = [[],
        [(7500, (0.04, 0.06)), (10000, (0.021, 0.029))],  # 1
        [(2500, (0.04, 0.06)), (10000, (0.021, 0.029))],  # 2
        [(2500, (0.04, 0.06)), (10000, (0.021, 0.029))],  # 3
        [(2500, (0.04, 0.06)), (5000, (0.021, 0.029)), (10000, (0.0010, 0.0015))],  # 4
        [(2500, (0.04, 0.06)), (10000, (0.021, 0.029))],  # 5
        [(5000, (0.04, 0.06)), (7500, (0.021, 0.029)), (10000, (0.0010, 0.0015))],  # 6
        [(5000, (0.04, 0.06)), (7500, (0.021, 0.029)), (10000, (0.0010, 0.0015))],  # 7
        [(2500, (0.04, 0.06)), (7500, (0.021, 0.029)), (10000, (0.0010, 0.0015))],  # 8
        [(5000, (0.04, 0.06)), (10000, (0.021, 0.029))],  # 9
        [(10000, (0.04, 0.06))],  # 10

        [(2500, (0.021, 0.029)), (10000, (0.0009, 0.0011))],  # 11
        [(5000, (0.021, 0.029)), (10000, (0.0009, 0.0011))],  # 12
        [(2500, (0.021, 0.029)), (10000, (0.0009, 0.0011))],  # 13
        [(7500, (0.021, 0.029)), (10000, (0.0009, 0.0011))],  # 14
        [(2500, (0.021, 0.029)), (10000, (0.0009, 0.0011))],  # 15
        [(2500, (0.021, 0.029)), (10000, (0.0009, 0.0011))],  # 16
        [(2500, (0.021, 0.029)), (10000, (0.0009, 0.0011))],  # 17
        [(5000, (0.021, 0.029)), (10000, (0.0009, 0.0011))],  # 18
        [(5000, (0.021, 0.029)), (10000, (0.0009, 0.0011))],  # 19
        [(5000, (0.021, 0.029)), (10000, (0.0009, 0.0011))],  # 20

        [(7500, (0.0009, 0.0011)), (10000, (0.04, 0.06))],  # 21
        [(5000, (0.0009, 0.0011)), (10000, (0.04, 0.06))],  # 22
        [(7500, (0.0009, 0.0011)), (10000, (0.0005, 0.0006))],  # 23
        [(7500, (0.0009, 0.0011)), (10000, (0.0005, 0.0006))],  # 24
        [(5000, (0.0009, 0.0011)), (7500, (0.04, 0.06)), (10000, (0.021, 0.029))],  # 25
        [(10000, (0.0009, 0.0011))],  # 26
        [(7500, (0.0009, 0.0011)), (10000, (0.0005, 0.0006))],  # 27
        [(5000, (0.0009, 0.0011)), (10000, (0.04, 0.06))],  # 28
        [(7500, (0.0009, 0.0011)), (10000, (0.04, 0.06))],  # 29
        [(5000, (0.0009, 0.0011)), (7500, (0.04, 0.06)), (10000, (0.021, 0.029))],  # 30

        [(2500, (0.0009, 0.0011)), (2882, (0.04, 0.06)), (4498, (0.052, 0.078)), (7500, (0.04, 0.06)), (10000, (0.021, 0.029))],  # 31
        [(2500, (0.0009, 0.0011)), (2882, (0.04, 0.06)), (4498, (0.052, 0.078)), (10000, (0.04, 0.06))],  # 32
        [(7500, (0.0009, 0.0011)), (10000, (0.04, 0.06))],  # 33
        [(7500, (0.0009, 0.0011)), (10000, (0.04, 0.06))],  # 34
        [(2500, (0.0009, 0.0011)), (2882, (0.04, 0.06)), (4498, (0.052, 0.078)), (7500, (0.04, 0.06)), (10000, (0.0009, 0.0011))],  # 32
        [(5000, (0.0009, 0.0011)), (10000, (0.04, 0.06))],  # 36
        [(2500, (0.0009, 0.0011)), (2882, (0.04, 0.06)), (4498, (0.052, 0.078)), (5000, (0.04, 0.06)), (10000, (0.021, 0.029))],  # 37
        [(7500, (0.0009, 0.0011)), (10000, (0.04, 0.06))],  # 38
        [(2882, (0.00005, 0.00005)), (4498, (0.0006, 0.0007)), (10000, (0.0005, 0.0005))],  # 39
        [(2500, (0.0009, 0.0011)), (2882, (0.04, 0.06)), (4498, (0.052, 0.078)), (7500, (0.04, 0.06)), (10000, (0.021, 0.029))],  # 40
        ]

rank_table = []
for i in range(1, 41):
    start = 1
    for elem in prob[i]:
        rank_table.append((i, start, elem[0], (elem[1][0] + elem[1][1]) / 2.0))
        start = elem[0] + 1
rank_table.sort(key=lambda x: x[3], reverse=True)

a1 = [25e6 for i in range(4)] + \
    [50e6 for i in range(4)] + \
    [0 for i in range(4)] + \
    [250e6 for i in range(4)] + \
    [500e6 for i in range(4)]
a_v1 = [200 for i in range(4)] + \
    [100 for i in range(4)] + \
    [0 for i in range(4)] + \
    [10 for i in range(4)] + \
    [1 for i in range(4)]

m1 = [int(2e3*0.8) for i in range(10)] + \
    [int(5e3*0.85) for i in range(10)] + \
    [int(10e3*0.90) for i in range(10)] + \
    [int(20e3*0.95) for i in range(10)]

new_rank_table = []
e_cost = 0
a_idx = 0
for rank_elem in rank_table:
    media_idx = rank_elem[0] - 1
    media_per_count = m1[media_idx]
    if rank_elem[3] <= 0.0005:
        media_per_count = int(m1[media_idx] * 0.46641)
    imp_count = media_per_count * (rank_elem[2] - rank_elem[1] + 1)
    new_rank_elem = []
    start, end = rank_elem[1], rank_elem[2]

    def append_new_rank_elem(media, ad, start, end, per_count):
        new_rank_elem.append((media,  # media
                              ad,  # ad
                              int(start),
                              int(end),
                              int(per_count),
                              rank_elem[3]
                              ))
        return a_v1[ad - 1] * rank_elem[3] * (end - start + 1) * per_count

    while imp_count > 0:
        if a1[a_idx] >= imp_count:
            a1[a_idx] -= imp_count
            imp_count = 0
            per_count = media_per_count
            e_cost += append_new_rank_elem(rank_elem[0], a_idx + 1, start, end,
                                           per_count)
        else:
            turn_number = a1[a_idx] // media_per_count
            per_count = media_per_count
            e_cost += append_new_rank_elem(rank_elem[0], a_idx + 1, start,
                                           start + turn_number - 1, per_count)
            imp_count -= turn_number * per_count
            start += turn_number

            remaindar = a1[a_idx] - (a1[a_idx] // media_per_count) \
                * media_per_count

            e_cost += append_new_rank_elem(rank_elem[0], a_idx + 1, start, start,
                                           remaindar)
            imp_count -= remaindar
            next_remaindar = media_per_count - remaindar
            a1[a_idx] = 0
            imp_count -= next_remaindar

            while next_remaindar > 0:
                a_idx += 1
                if a1[a_idx] >= next_remaindar:
                    e_cost += append_new_rank_elem(rank_elem[0], a_idx + 1,
                                                   start, start,
                                                   next_remaindar)
                    a1[a_idx] -= next_remaindar
                    next_remaindar = 0
                    break
                else:
                    e_cost += append_new_rank_elem(rank_elem[0], a_idx + 1,
                                                   start, start,
                                                   a1[a_idx])
                    next_remaindar -= a1[a_idx]
                    a1[a_idx] = 0
            start += 1

        if a1[a_idx] == 0:
            a_idx += 1
    new_rank_table.extend(new_rank_elem)

print len(new_rank_table)

with open("new_rank_table.pk", "wb") as output:
    pickle.dump(new_rank_table, output, pickle.HIGHEST_PROTOCOL)


def get_next_add(fill_rate, max_impression_count, mediaNo, seqNum):
    return [ item for item in new_rank_table if item[0]==mediaNo and seqNum>= item[2] and seqNum <= item[3]]

##################################################

a1_validate = {}
eee_cost = 0
for j in range(1,10000+1):
    gggg = [ get_next_add(0.7,1500,i,j)[0] for i in range(1,40+1)]
    for item in gggg:
        a1_validate.setdefault(item[1], 0)
        a1_validate[item[1]] += item[4]
    eee_cost += sum([item[4]*a_v1[item[1]-1] for item in gggg])
