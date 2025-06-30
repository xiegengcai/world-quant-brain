# -*- coding: utf-8 -*-
# 导入官方库
import asyncio
from datetime import datetime
import json
import os
import time

# 导入第三方库
import constants
import wqb
import matplotlib.pyplot as plt
import pandas as pd

from AlphaMapper import AlphaMapper
from simulator import Simulator
import utils

class RobustTester:
    def __init__(self, wqbs: wqb.WQBSession, out_put_path: str, data_path:str='./db'):
        self.wqbs = wqbs
        self.out_put_path = out_put_path
        self.mapper = AlphaMapper(data_path)
    
    def locate_alpha(self, alpha_id:str):
        alpha = asyncio.run(self.wqbs.locate_alpha(alpha_id)).json()
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

        return [
            alpha_id
            , sharpe
            , turnover
            , fitness
            , margin
            , exp
            , region
            , universe
            , neutralization
            , decay
            , delay
            , truncation
        ]

    def build_sim_data_list(self, alpha_id:str)-> list:
        """构建模拟数据"""
        # 初始化对照组 alpha_json 列表
        alpha_line = []

        # 获取目标 alpha 信息
        [alpha_id
            , sharpe
            , turnover
            , fitness
            , margin
            , exp
            , region
            , universe
            , neutralization
            , decay
            , delay
            , truncation] = self.locate_alpha(alpha_id)
        # 根据 decay 的值选择不同的 decay_tem 列表
        decay_tem_list = [decay - 5, decay + 5] if decay >= 5 else [decay + 10, decay + 20]
        # 初始化 neutralization_tem 列表
        neutralization_tem_list = ['SUBINDUSTRY', 'INDUSTRY', 'SECTOR', 'MARKET']

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
        return alpha_line
    

    def get_alpha_data(self, alpha_id_ori:str, alpha_ids:list):
        """获取模拟运行结果"""
        # 初始化对比alpha表现的dataframe
        df_list = pd.DataFrame(columns=['alpha_id', 'neutralization', 'decay', 'sharpe', 'fitness', 'turnover', 'margin'])
        # 截取目标alpha信息中需要对比的部分
        # new_row = [alpha_id_ori, neutralization, decay, sharpe, fitness, turnover, margin]
        # 确保所有变量都已定义且不为None
        new_row = [
            alpha_id_ori if 'alpha_id_ori' in locals() else '', 
            neutralization if 'neutralization' in locals() else 0,
            decay if 'decay' in locals() else 0,
            sharpe if 'sharpe' in locals() else 0,
            fitness if 'fitness' in locals() else 0,
            turnover if 'turnover' in locals() else 0,
            margin if 'margin' in locals() else 0
        ]
        # 将信息写入对比df
        df_list.loc[len(df_list)] = new_row
        print(df_list)

        # 遍历location列表获取对照组alpha的表现
        for alpha_id in alpha_ids:
            tem = self.locate_alpha(alpha_id) # 获取alpha_id
            [alpha_id, sharpe, turnover, fitness, margin, exp, region, universe, neutralization, decay, delay, truncation] = tem
            new_row = [alpha_id, neutralization, decay, sharpe, fitness, turnover, margin]
            df_list.loc[len(df_list)] = new_row  # 直接赋值（确保 df 已初始化列名）

        # dataframe去重
        df_list = df_list.drop_duplicates(subset="alpha_id", keep="first")
        return df_list
    
    def paint(self, alpha_id_ori:str,df_list:list):
        """绘制对比图"""
        # 初始化存储PnL的dataframe
        df1 = pd.DataFrame()
        # 遍历alpha_id获取PnL并存入dataframe
        for alpha_id in df_list['alpha_id'].unique():
            print(alpha_id)
            json_data = utils.get_pnl_data(self.wqbs, alpha_id)['records']
            df = pd.DataFrame(json_data)
            df=df.iloc[:,0:2]
            df.columns = ['date', alpha_id]
            df.set_index('date', inplace=True)
            df1 = pd.merge(df1, df, left_index=True, right_index=True, how='outer')
        df1.index = pd.to_datetime(df1.index)
        # 如果需要可以查看这个df
        # df1
        # 设置matplotlib
        # 设置matplotlib
        plt.rcParams['font.sans-serif'] = ["Microsoft YaHei", "Arial Unicode MS"]  # 兼容win和mac的字体
        plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题
        # 绘制所有列（自动分配颜色和标签）
        ax = df1.plot(
            figsize=(14, 7),
            linewidth=2,
            title='多时间序列对比',
            grid=True,
            alpha=0.8,
            fontsize=12
        )
        # 添加图例和标签
        ax.set_xlabel('日期', fontsize=12)
        ax.set_ylabel('数值', fontsize=12)
        ax.legend(loc='upper left', frameon=True)
        # print(alpha_id_ori)
        plt.xticks(rotation=45)
        plt.tight_layout()
        # plt.show()
        plt.savefig(f'{self.out_put_path}/{alpha_id_ori}_pnl.png')
        # 对dataframe进行回测
        # df_sorted = df_list.sort_values("neutralization")
        # df_multiindex = df_sorted.set_index(["neutralization", "decay"])
        # df_multiindex.to_csv(f'{self.out_put_path}/{alpha_id_ori}_pnl.csv')

    def run(self, sharpe: float=1.25, fitness: float=1.0):
        """运行"""
        # 获取所有alpha表达式
        metrics={constants.IS_SHARPE: sharpe, constants.IS_FITNESS: fitness}
        page = 0
        # 一次不要跑太多
        page_size = 5
        while True:
            alpha_list = self.mapper.get_alphas(status=constants.ALPHA_STATUS_CHECKED, metrics=metrics, page=page, page_size=page_size)
            self.do_run(alpha_list)

    
    def do_run(self, alpha_list:list): 
        """运行"""
        # self_corr = SelfCorrelation(self.wqbs, data_path='./results')
        # self_corr.load_data()
        # alpha_list = [alpha for alpha in alpha_list if self_corr.calc_self_corr(alpha['id']) < 0.6]
        # print(f"过滤自相关大于0.6的数据后剩余{len(alpha_list)}个alpha表达式")
        if len(alpha_list) == 0:
            return
        
        
        simulator = Simulator(wqbs, 2)
        for alpha in alpha_list:
            alpha_id_ori = ''
            if type(alpha) == str:
                alpha_id_ori = alpha
                alpha = self.mapper.get_alpha({'alpha_id': alpha})
            if alpha is None or (alpha.has_key('status') and alpha['status'] != constants.ALPHA_STATUS_CHECKED):
                print (f"alpha {alpha_id_ori} is not checked")
                continue
            alpha_id_ori = alpha['alpha_id']
            sim_data_list = self.build_sim_data_list(alpha_id_ori)
            for alpha in sim_data_list:
                alpha['parent_id'] = alpha_id_ori
            # 入库
            self.mapper.bath_save(sim_data_list,step=alpha['step'], parent_id=alpha_id_ori)
            alpha_ids = []
            alpha_ids = simulator.do_simulate(sim_data_list, alpha_ids)
            df_list = self.get_alpha_data(alpha_id_ori, alpha_ids)
            self.paint(alpha_id_ori, df_list)

if  __name__ == "__main__":
    wqbs= wqb.WQBSession((utils.load_credentials('~/.brain_credentials.txt')))
    tester = RobustTester(wqbs, './results')
    start_time=datetime.fromisoformat('2025-06-22T00:00:00-05:00')
    end_time=datetime.fromisoformat('2025-06-22T00:00:00-05:00')
    alpha_list = utils.submitable_alphas(wqbs, start_time, end_time, limit=500)
    alpha_list=['dzndQVJ']
    tester.run(alpha_list)
   