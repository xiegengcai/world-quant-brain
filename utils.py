# -*- coding: utf-8 -*-
import hashlib
import json
from os.path import expanduser

from collections import defaultdict
import time
from typing import Iterable

import wqb

def submitable_alphas(wqbs: wqb.WQBSession, limit:int=100, order:str='dateCreated', offset:int=0,others:Iterable[str]=None) -> list:
    """可提交的alpha"""
    resp = wqbs.filter_alphas_limited(
        status='UNSUBMITTED',
        region='USA',
        delay=1,
        universe='TOP3000',
        sharpe=wqb.FilterRange.from_str('[1.58, inf)'),
        fitness=wqb.FilterRange.from_str('[1, inf)'),
        turnover=wqb.FilterRange.from_str('(-inf, 0.7]'),
        others=others,
        order=order,
        limit=limit,
        offset=offset,
        log="utils#submitable_alphas"
    )
    retry_after = float(resp.headers.get("Retry-After", 0))
    # 增加重试
    if retry_after > 0:
        time.sleep(retry_after)
        return submitable_alphas(wqbs=wqbs, limit=limit, order=order, offset=offset)
    alpha_list = resp.json()['results']
    print(f'共{len(alpha_list)}个Alpha待提交')
    return alpha_list

def filter_failed_alphas(alpha_list: list) -> list:
    """过滤掉有FAIL指标的的alpha"""
    list = []
    for alpha in alpha_list:
        checks = alpha['is']['checks']
        fail = False
        for check in checks:
            if check['result'] == 'FAIL':
                fail = True
                break
        if fail:
            continue
        list.append(alpha)
    return list

def is_favorable(wqbs: wqb.WQBSession, alpha_id:str, improve:int=0) -> bool:
    """
    判断 Alpha 是可收藏的
    判断标准：
    1. 该alpha的提交before和after的Change(名次)是否上升大于improve
    """
    resp = wqbs.get(f'{wqb.WQB_API_URL}/competitions/IQC2025S1/alphas/{alpha_id}/before-and-after-performance')
    retry_after = float(resp.headers.get("Retry-After", 0))
    if retry_after > 0:
        time.sleep(retry_after)
        return is_favorable(wqbs=wqbs,alpha_id=alpha_id, improve=improve)
    
    score = resp.json()['score']
    return score['after'] - score['before'] > improve

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