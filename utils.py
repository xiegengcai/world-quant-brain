# -*- coding: utf-8 -*-
import asyncio
import hashlib
import json
from os.path import expanduser

from collections import defaultdict
import time
from typing import Iterable

import pandas as pd
import wqb

def filter_alphas(
    wqbs: wqb.WQBSession,
    status: str = 'UNSUBMITTED',
    region: str=None,
    delay: int=None,
    universe: str=None,
    sharpeFilterRange: wqb.FilterRange = None,
    fitnessFilterRange: wqb.FilterRange = None,
    dateCreatedFilterRange: wqb.FilterRange = None,
    turnoverFilterRange: wqb.FilterRange = None,
    order: str = 'is.sharpe',
    others: Iterable[str] = None,
    limit: int = 2000,
    log_name: str = None,
) -> list:
    """获取alpha列表"""
    if log_name is None:
        log_name = f"{self.__class__.__name__}#{filter_alphas.__name__}"
    
    list = []
    page_size = 100
    offset = 0
    while True:
        
        resp = wqbs.filter_alphas_limited(
            status=status,
            region=region,
            delay=delay,
            universe=universe,
            sharpe=sharpeFilterRange,
            fitness=fitnessFilterRange,
            turnover=turnoverFilterRange,
            date_created=dateCreatedFilterRange,
            order=order,
            others=others,
            limit=page_size,
            offset=offset,
            log=log_name
        )

        data = resp.json()
        if offset == 0:
            print(f"本次查询条件下共有{data['count']}条数据...")
        list.extend(data['results'])
        if len(list) >= limit:
            break
        # 小于本次查询数量limit
        if len(data['results']) < page_size:
            break
        offset += page_size
    return list

def submitable_alphas(wqbs: wqb.WQBSession, start_time:str, end_time:str, limit:int = 50, order:str='dateCreated', others:Iterable[str]=None) -> list:
    """可提交的alpha"""
    list = filter_alphas(
        wqbs,
        status='UNSUBMITTED',
        region='USA',
        delay=1,
        universe='TOP3000',
        sharpeFilterRange=wqb.FilterRange.from_str('[1.125, inf)'),
        fitnessFilterRange=wqb.FilterRange.from_str('[1, inf)'),
        turnoverFilterRange=wqb.FilterRange.from_str('[0.01, 0.7]'),
        dateCreatedFilterRange=wqb.FilterRange.from_str(f'[{start_time}, {end_time}]'),
        order=order,
        others=others,
        limit=limit,
        log_name="utils#submitable_alphas"
    )
    # 过滤掉不合格的Alpha
    list = [alpha for alpha in list if alpha['grade'].upper() != 'INFERIOR']
    if len(list) == 0:
        print('没有可提交的 Alpha...')
        return list
    failed_ids = []
    with open('./results/check_fail_ids.csv', 'r') as f:
        failed_ids = set(line.strip() for line in f)
    # 过滤掉已处理的 Alpha
    list = [alpha for alpha in list if alpha['id'] not in failed_ids]
    return list

def filter_failed_alphas(wqbs:wqb.WQBSession, alpha_list: list) -> list:
    """过滤掉有FAIL指标的的alpha"""
    list = []
    failed_ids = []
    lines = []
    print('过滤掉有FAIL指标的Alpha...')
    for alpha in alpha_list:
        checks = alpha['is']['checks']
        fail = False
        for check in checks:
            if check['result'] == 'FAIL':
                wqbs.patch_properties(alpha_id=alpha['id'], color="RED", log=f'utils#filter_failed_alphas')
                fail = True
                failed_ids.append(alpha['id'])
                break
        if fail:
            lines.append(f"{alpha['id']}\n")
            continue
        list.append(alpha)
    
    if len(failed_ids) > 0:
        save_lines_to_file( './results/check_fail_ids.csv', lines)
        # batch_ids = [failed_ids[i:i + 20] for i in range(0,len(failed_ids), 20)]
        # for ids in batch_ids:
        #     data = []
        #     for id in ids:
        #         data.append({"id": id,"color":"RED"})
        #     wqbs.patch(f'{wqb.WQB_API_URL}/alphas', json=data)
        

    return list

def is_favorable(wqbs: wqb.WQBSession, alpha_id:str, improve:int=0) -> bool:
    """
    判断 Alpha 是可收藏的
    判断标准：
    1. 该alpha的提交before和after的Change(名次)是否上升大于improve
    """
    resp = wqbs.get(f'{wqb.WQB_API_URL}/competitions/IQC2025S2/alphas/{alpha_id}/before-and-after-performance')
    retry_after = float(resp.headers.get("Retry-After", 0))
    if retry_after > 0:
        time.sleep(retry_after)
        return is_favorable(wqbs,alpha_id,improve)
    
    score = resp.json()['score']
    score_diff = score['after'] - score['before']
    print(f"Alpha {alpha_id} 的Change(名次)为: {score_diff}")
    return score_diff > improve

def load_credentials(credentials_file: str):
    """从文件加载凭据"""
    try:
        with open(expanduser(credentials_file)) as f:
            credentials = json.load(f)
        return credentials[0], credentials[1]
    except Exception as e:
        print(f"Failed to load credentials: {str(e)}")
        raise

def hash(alpha):
    """生成稳定的哈希值"""
    alpha_string = f"{alpha['regular']}{json.dumps(alpha['settings'], sort_keys=True)}"
    return hashlib.md5(alpha_string.encode('utf-8')).hexdigest()

def save_lines_to_file(dest_file: str, lines: list):
    """保存内容到文件"""
    with open(dest_file, 'a') as f:
        f.writelines(lines)

    print(f"✅ {len(lines)} 行已保存")


def prune(next_alpha_recs, prefix, keep_num):
    # prefix is the datafield prefix, fnd6, mdl175 ...
    # keep_num is the num of top sharpe same-datafield alpha
    output = []
    num_dict = defaultdict(int)
    for rec in next_alpha_recs:
        exp = rec[1]
        field = exp.split(prefix)[-1].split(",")[0]
        sharpe = rec[2]
        if sharpe < 0:
            field = "-%s"%field
        if num_dict[field] < keep_num:
            num_dict[field] += 1
            decay = rec[-1]
            exp = rec[1]
            output.append([exp,decay])
    return output

# def filter_correlation(self_corr:SelfCorrelation, alpha_list: list, threshold:float=0.7) -> list:
#     list=[]
#     lines = []
#     print(f'过滤相关性大于{threshold}的Alpha...')
#     os_alpha_ids, os_alpha_rets = self_corr.load_data()
#     # os_alpha_ids, os_alpha_rets =self.correlation.load_data()
#     for alpha in alpha_list:
#         try:
#             ret = self_corr.calc_self_corr(
#                 alpha_id=alpha['id'],
#                 os_alpha_rets=os_alpha_rets
#                 ,os_alpha_ids=os_alpha_ids
#             )
#             if ret < threshold:
#                 lines.append(f"{alpha['id']}\n")
#                 list.append(alpha)
#         except Exception as e:
#             print(f'计算alpha {alpha["id"]} 自相关性失败: {e}')
#     save_lines_to_file('./results/correlation.txt', lines)
#     return list


def get_dataset_fields(
    wqbs: wqb.WQBSession, 
    dataset_id: str,
    region: str="USA", 
    delay: int=1, 
    universe: str="TOP3000",
    order:str='-alphaCount',
    offset:int=0
) -> list:
    """
    获取数据集的字段
    """

    dataset_fields = []
    kwargs = {"order": order}
    resps = wqbs.search_fields(
        region=region,
        delay=delay,
        universe=universe,
        dataset_id=dataset_id,
        offset=offset,
        log="utils#get_dataset_fields",
        **kwargs
    )
    for idx, resp in enumerate(resps, start=1):
        # print(f"正在获取第 {idx} 页数据集字段...")
        data = resp.json()
        dataset_fields.extend(data['results'])
    return dataset_fields

def check(wqbs: wqb.WQBSession, batch_num:int, alpha_ids: list, out_put_path: str, local_check: bool = True, log: str = ''):
    """检查alpha"""
    # total = len(alpha_ids)
    failed_alpha_ids = []
    patch_data = []
    start_time = time.time()
    self_corr = None
    # if local_check:
    #     self_corr = SelfCorrelation(wqbs, data_path='./results')
    for alpha_id in alpha_ids:
        # if local_check:
        #     color_data=_local_check(wqbs, alpha_id, failed_alpha_ids, self_corr)
        # else:
        color_data=server_check(wqbs, alpha_id, failed_alpha_ids, log=log)
        patch_data.append(color_data)
        
    
    if len(failed_alpha_ids) > 0:
        alpha_ids = list(set(alpha_ids) - set(failed_alpha_ids))
        for id in failed_alpha_ids:
            patch_data.append({'id': id, 'color': 'RED'})

    patch_resp = wqbs.patch(f'{wqb.WQB_API_URL}/alphas', json=patch_data)
    
    if patch_resp.status_code == 200:
        # 写回文件
        fail_lines = [f'{id}\n' for id in failed_alpha_ids]
        pass_lines = [f'{id}\n' for id in alpha_ids]
        save_lines_to_file(f'{out_put_path}/check_fail_ids.csv', fail_lines)
        save_lines_to_file(f'{out_put_path}/check_pass_ids.csv', pass_lines)
        print(f"✅ 第{batch_num}批{len(patch_data)} 个Alpha检查完成...")
    else:
        print(f"❌ 第{batch_num}批{len(patch_data)} 个Alpha检查失败...")

    end_time = time.time()
    print(f"第{batch_num}批耗时: {(end_time - start_time):.2f}秒")

def server_check(wqbs: wqb.WQBSession, alpha_id: str, failed_alpha_ids:list, max_tries: int = range(600), log: str = '') -> dict:
    """服务器检查alpha"""
    color = 'GREEN' # 绿色
    try:
        resp = asyncio.run(
            wqbs.check(
                alpha_id,
                max_tries=max_tries,
                on_start=lambda vars: print(vars['url']),
                on_finish=lambda vars: print(vars['resp']),
                # on_success=lambda vars: print(vars['resp']),
                # on_failure=lambda vars: print(vars['resp']),
                log=log
            )
        )
        data = resp.json()
        is_check = data['is']['checks']
        results = [(j['result']).upper() for j in is_check]
        # 全为pass
        if len(results) == 8 and len(set(results)) == 1 and 'PASS' in results:
            if not is_favorable(wqbs, alpha_id,20):
                color = 'PURPLE' # 紫色
        else:
            color='RED'
            failed_alpha_ids.append(alpha_id)
        return {
            'id': alpha_id, 'color':color
        }
    except Exception as e:
        print(f'检查alpha {alpha_id} 失败: {e}')
        return None
    

# def _local_check(wqbs: wqb.WQBSession, alpha_id: str, failed_alpha_ids:list, self_corr:SelfCorrelation,threshold:float=0.7)-> dict:
#     color = 'GREEN' # 绿色
#     try:
#         self_corr_val = self_corr.calc_self_corr(alpha_id)
#         print(f'alpha {alpha_id} 自相关性: {(self_corr_val):.2f}')
#         if self_corr_val > threshold:
#             color='RED'
#             failed_alpha_ids.append(alpha_id)
#         else:
#             if not is_favorable(wqbs, alpha_id, 20):
#                 color = 'PURPLE' # 紫色
#         return {
#             'id': alpha_id, 'color':color
#         }
#     except Exception as e:
#         print(f'计算alpha {alpha_id} 自相关性失败: {e}')
#         return None
    
def get_pnl_data(wqbs: wqb.WQBSession, alpha_id: str) -> pd.DataFrame:
    """获取alpha的pnl数据"""
    # 可能会被限流
    resp = wqbs.get(f"{wqb.WQB_API_URL}/alphas/{alpha_id}/recordsets/pnl")
    retry_after = float(resp.headers.get(wqb.RETRY_AFTER, 0))
    if retry_after > 0:
        time.sleep(retry_after)
        return get_pnl_data(wqbs, alpha_id)

    return resp.json()
    # df = pd.DataFrame(pnl['records'], columns=[item['name'] for item in pnl['schema']['properties']])
    # df = df.rename(columns={'date':'Date', 'pnl':alpha_id})
    # df = df[['Date', alpha_id]]
    # return df

def sort_by_grade(alpha_first: dict, alpha_second: dict) -> int:
    """根据alpha的grade排序"""
    first_grade = alpha_first['grade'].upper()
    second_grade = alpha_second['grade'].upper()
    if first_grade == second_grade:
        return 0
    if first_grade == 'INFERIOR':
        return 1
    if first_grade == 'AVERAGE':
        return 1
    return -1