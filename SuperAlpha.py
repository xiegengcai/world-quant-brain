# -*- coding: utf-8 -*-

from itertools import product
import wqb
import dataset_config
from simulator import Simulator
import factory
import utils

class SuperAlpha:
    """
    超级Alpha
    """
    def __init__(
        self
        , wqbs:wqb.WQBSession
        , dataset_id:str
    ):
        """
        初始化
        :param wqbs: wqb session
        :param dataset_id: 数据集id
        """
        self.wqbs = wqbs
        self.dataset_id = dataset_id
        # self.super_states = ["stats.returns", "stats.pnl", "stats.turnover", "stats.drawdown", "stats.hold_pnl", "stats.hold_shares","stats.hold_value", "stats.long_count", "stats.long_value","stats.short_count", "stats.short_value", "stats.trade_pnl", "stats.trade_shares" ,"stats.trade_value"]
        #ts操作符，换成自己的能用的
        self.combo_ops = [
            # "combo_a",
            "signed_power","ts_rank","ts_arg_min", "ts_arg_max",
            # "ts_max_diff", "ts_returns",
            "ts_zscore", "ts_delta",  "ts_sum", "ts_delay",
            # "ts_ir", 
            "ts_std_dev", "ts_mean", "ts_scale", 
            # "ts_kurtosis",  
            "ts_quantile","last_diff_value","ts_av_diff","ts_product"]

        self.days = [5, 22, 66, 120, 240]

    def single_data_set_alphas(self):
        """
        生成super的combo表达式
        :return: super的combo表达式
        """
        combinations = product(self.combo_ops, self.days)

        dataset = dataset_config.get_dataset_config(self.dataset_id)
        api_settings = dataset['api_settings']
        dataset_fields = utils.get_dataset_fields(
            self.wqbs,
            region=api_settings['region'],
            delay=api_settings['delay'],
            universe=dataset['universe'],
            dataset_id=self.dataset_id
        )
        api_settings['universe'] = dataset['universe']
        expressions = []
        for ts_op, window in combinations:
            if ts_op == "combo_a":
                continue
            for field in dataset_fields:
                expr = f"{ts_op}({field['id']}, {window})"
                expressions.append({
                    'type': 'REGULAR',
                    'settings':api_settings
                    , 'regular':expr
                })
        return expressions

    
    def generate_super_alphas(self):
        """
        生成super的combo表达式
        :return: super的combo表达式
        """
        combinations = product(self.combo_ops, self.days)
        api_settings = dataset_config.default_settings
       
        expr_list =[]
        with open('可以尝试的字段.txt', 'r') as f:
            for line in f.readlines():
                expr_list.append(line)
        expressions = []
        for ts_op, window in combinations:
            if ts_op == "combo_a":
                continue
            for field in expr_list:
                expr = f"{ts_op}({field.strip()}, {window})"
                expressions.append({
                    'type': 'REGULAR',
                    'settings':api_settings
                    , 'regular':expr
                })
        return expressions

    #super的combo表达式生成
    # def ts_super(self):
    #     combinations = product(
    #         self.super_states, 
    #         self.combo_ops, self.days)
    #     # combo_a_modes = ['algo1', 'algo2', 'algo3']
        
    #     expressions = []
    #     # # 先处理 combo_a
    #     # for mode in combo_a_modes:
    #     #     expr = f"combo_a(alpha, nlength=250, mode='{mode}')"
    #     #     expressions.append(expr)
        
    #     # 再处理其他操作符
    #     for super_combo, ts_op, window in combinations:
    #         # 跳过 combo_a
    #         if ts_op == "combo_a":
    #             continue
    #         expr = f"stats=generate_stats(alpha);{ts_op}({super_combo}, {window})"
    #         expressions.append(expr)
        
    #     return expressions

if  __name__ == "__main__":
    wqbs= wqb.WQBSession((utils.load_credentials('~/.brain_credentials.txt')), logger=wqb.wqb_logger())
 
    super_alpha = SuperAlpha(wqbs, 'analyst4')
    # expressions = super_alpha.single_data_set_alphas()
    expressions = super_alpha.generate_super_alphas()
    simulator = Simulator(wqbs, './results/alpha_ids.csv', False, 30)
    simulator.simulate_alphas(expressions)
    # print(expressions)
    # simulator = Simulator(wqbs, expressions)
    # simulator.simulate_with_available("available.txt")