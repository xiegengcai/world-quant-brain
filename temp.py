# -*- coding: utf-8 -*-

from datetime import datetime
import functools
from itertools import combinations
import os
import wqb

import pandas as pd

import factory
from simulator import Simulator

import utils

def fundament_fac(profit_fields,size_fields,ops):
    alpha_list =[]
    for pro_field in profit_fields:
        for size_field in size_fields:
            base_data = f'{pro_field}/{size_field}'
            for ts_op in ops:
                alpha_list += factory.ts_factory(ts_op,base_data)
    return alpha_list

def analyst_fac(fields):

    act_fields = fields[fields['id'].str.contains(r"actual", case=False, na=False)].to_dict('records')
    est_fields = fields[fields['id'].str.contains(r"est_", case=False, na=False)].to_dict('records')
    alpha_list = []

    for act_field in act_fields: 
        for est_field in est_fields:
            alpha_list.append(f'group_zscore(subtract(group_zscore({act_field['id']}, industry),group_zscore({est_field['id']},industry)), industry)')
        
    return alpha_list

def vector_neut_template(fields):
    alpha_list = []
    days = [3,6,18,60]
    for field in fields:
        # alpha_list.append(f"IR = abs(ts_mean(returns,252)/ts_std_dev(returns,252));regression_neut (vector_neut(ts_zscore(vec_max(({field}, 126))/close, 126),ts_median(cap, 126) ),IR)")
        # for day in days:
        alpha_list.append(f"group_neutralize(vector_neut(-ts_delta({field},3),abs(ts_mean(returns,252)/ts_std_dev(returns,252))),subindustry)")
        alpha_list.append(f"vector_neut(-{field} * ts_std_dev({field}, 20),abs(ts_mean(returns,252)/ts_std_dev(returns,252)))")
    return alpha_list

if __name__ == '__main__':

    # files=os.listdir('D:/Downloads/xiaowu')
    # for filename in files:
    #     if filename.__contains__('xw ('):
    #         new_filename=filename.replace('xw (', '').replace(')', '') 
    #         os.rename(f'D:/Downloads/xiaowu/{filename}', f'D:/Downloads/xiaowu/{new_filename}')


    # filter = wqb.FilterRange.from_str('[0.01, 0.7]')
    
    # print(filter.to_params('aaa'))

    wqbs= wqb.WQBSession((utils.load_credentials("~/.brain_credentials.txt")), logger=wqb.wqb_logger(name='logs/wqb_' + datetime.now().strftime('%Y%m%d')))

    # alpha_list=utils.submitable_alphas(
    #     wqbs,
    #     start_time='2025-05-16T00:00:00-05:00',
    #     end_time=f'{datetime.strftime(datetime.now(), "%Y-%m-%d")}T23:59:59-05:00',
    #     limit=10000, order='is.sharpe',others=['color!=RED']
    # )
    # failed_set = {alpha['id'] for alpha in alpha_list 
    #           if any(check['result'].upper() == 'FAIL' 
    #                 for check in alpha['is']['checks'])}
    # print(f'{alpha_list[0]["grade"]}, {alpha_list[-1]["grade"]}')
    # alpha_list = sorted(alpha_list, key=functools.cmp_to_key(utils.sort_by_grade))
    # print(f'{alpha_list[0]["grade"]}, {alpha_list[-1]["grade"]}')
    # alpha_list = []
    dataset_id = 'model53'

    neutralization_list = ['MARKET','INDUSTRY','SUBINDUSTRY']  # , 'SUBINDUSTRY']
    group_ops = ["group_neutralize", "group_rank", "group_zscore"]
    ts_ops = ['ts_arg_max', 'ts_av_diff', 'ts_count_nans', 'ts_delay', 'ts_delta', 'ts_mean', 'ts_rank', 'ts_zscore']
    # "group_scale",
    #  'ts_arg_min','ts_decay_linear', 'ts_product','ts_quantile','ts_scale', 'ts_std_dev','ts_sum',
    # , 'MARKET', 'SECTOR'
    # fields = doubao_fields
    size_fields = ['mdl77_25saleicap','mdl77_2deepvaluefactor_ebitdaev','mdl77_2400_vefcomtt','mdl77_2ad']
    fields = utils.get_dataset_fields(
        wqbs, 
        dataset_id=dataset_id
    )
    fields = pd.DataFrame(fields)
    # fields = fields[fields['description'].str.contains(r"Profit|Income|Earning|Revenue", case=False, na=False)].to_dict('records')
    
    fields = fields[fields['type'] == "MATRIX"]["id"].tolist()

    # fields = factory.process_datafields(fields)
    # print(fields)

    # fields = []
    # with open("./可以尝试的字段.txt", "r") as f:
    #     for line in f.readlines():
    #         fields.append(line.strip())

    # alpha_list =factory.generate_sim_data(dataset_id, factory.first_order_factory(fields, ts_ops))
    # days = [5,20,60,250]
    # alpha_list = [] 
    # for i,j in combinations(fields,2):
    #     for op in ts_ops:
    #         for day in days:
    #             alpha_list.append(f'{op}({i}-{j},{day})')
    print(f"共{len(fields)}个字段，前三个如下：\n{fields[:3]}")
    alpha_list = vector_neut_template(fields)
    
    # alpha_list = analyst_fac(fields)
    alpha_list = factory.generate_sim_data(dataset_id, alpha_list)
    
    print(f"共{len(alpha_list)}个表达式，前三个表达式如下：\n{alpha_list[:3]}")

    Simulator(wqbs, "./results/alpha_ids.csv", False, 30).simulate_alphas(alpha_list)
