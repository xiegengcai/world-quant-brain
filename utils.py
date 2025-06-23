# -*- coding: utf-8 -*-
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
        if not data:
            print(f"没有符合条件的数据...")
            break
        if offset == 0 and data != {}:
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

def is_favorable(wqbs: wqb.WQBSession, alpha_id:str,  iqc_id='IQC2025S2', improve:int=0) -> bool:
    """
    判断 Alpha 是可收藏的
    判断标准：
    1. 该alpha的提交before和after的Change(名次)是否上升大于improve
    """
    score_diff = wqbs.get_performance(alpha_id=alpha_id, iqc_id=iqc_id)

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

def get_pnl_data(wqbs: wqb.WQBSession, alpha_id: str) -> pd.DataFrame:
    """获取alpha的pnl数据"""
    # 可能会被限流
    resp = wqbs.get(f"{wqb.WQB_API_URL}/alphas/{alpha_id}/recordsets/pnl")
    retry_after = float(resp.headers.get(wqb.RETRY_AFTER, 0))
    if retry_after > 0:
        time.sleep(retry_after)
        return get_pnl_data(wqbs, alpha_id)

    return resp.json()

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