# -*- coding: utf-8 -*-
# 导入官方库
from datetime import datetime
import json
import os
import time

# 导入第三方库
import wqb
import matplotlib.pyplot as plt
import pandas as pd

from self_correlation import SelfCorrelation
import utils

class RobustTester(object):
    def __init__(self, wqbs: wqb.WQBSession, start_time:str,end_time:str, out_put_path: str, is_consultant:bool=True, batch_size:int=30):
        self.wqbs = wqbs
        self.start_time = start_time
        self.end_time = end_time
        self.out_put_path = out_put_path
        self.is_consultant = is_consultant
        self.batch_size = batch_size
    
    def locate_alpha(self, alpha:dict):
        
        sharpe = alpha["is"]["sharpe"]
        fitness = alpha["is"]["fitness"]
        turnover = alpha["is"]["turnover"]
        margin = alpha["is"]["margin"]
        decay = alpha["settings"]["decay"]
        delay = alpha["settings"]["delay"]
        exp = alpha['regular']['code']
        universe=alpha["settings"]["universe"]
        truncation=alpha["settings"]["truncation"]
        neutralization=alpha["settings"]["neutralization"]
        region=alpha["settings"]["region"]

        triple = [alpha['id'], sharpe, turnover, fitness, margin, exp, region, universe, neutralization, decay, delay, truncation]
        return triple
    
    def build_by_alphabuild_by_alpha(self, alpha:dict):
        triple = self.locate_alpha(alpha)
        # 初始化对照组 alpha_json 列表
        alpha_line = []
        # 将 alpha 信息列表解包并赋值给对应的变量
        [alpha_id, sharpe, turnover, fitness, margin, exp, region,  universe, neutralization, decay, delay, truncation] = alpha_line
        # 根据 decay 的值选择不同的 decay_tem 列表
        decay_tem_list = [decay - 5, decay + 5] if decay >= 5 else [decay + 10, decay + 20]
        # 初始化 neutralization_tem 列表
        neutralization_tem_list = ['SUBINDUSTRY', 'INDUSTRY', 'SECTOR', 'MARKET', 'CROWDING']
        # 使用列表推导式生成 simulation_data
        alpha_line.extend(
            {
                'type': 'REGULAR',
                'settings': {
                    'instrumentType': 'EQUITY',
                    'region': region,
                    'universe': universe,
                    'delay': delay,
                    'decay': decay_tem,
                    'neutralization': neutralization_tem,
                    'truncation': truncation,
                    'pasteurization': 'ON',
                    'unitHandling': 'VERIFY',
                    'nanHandling': 'ON',
                    'language': 'FASTEXPR',
                    'visualization': False,
                    'testPeriod': "P0Y",
                    'maxTrade': 'ON'
                },
                'regular': exp
            }
            for decay_tem in decay_tem_list
            for neutralization_tem in neutralization_tem_list
        )
        print(f"👨‍💻 共生成了 {len(alpha_line)} 因子表达式.")

    def 
    
    def run_test(self):
        alpha_list = utils.submitable_alphas(self.wqbs, self.start_time, self.end_time, limit=500, others=['color!=RED'])
        self_corr = SelfCorrelation(self.wqbs, data_path='./results')
        self_corr.load_data(tag='SelfCorr')
        alpha_list = [alpha for alpha in alpha_list if self_corr.calc_self_corr(alpha['id']) < 0.7]

        for alpha in alpha_list:
            
