# -*- coding: utf-8 -*-

from collections import defaultdict
from datetime import datetime
from itertools import filterfalse
import random
from typing import Iterable
import wqb
import dataset_config
import factory
from simulator import Simulator
import utils
from self_correlation import SelfCorrelation

group_ops = ["group_neutralize", "group_rank", "group_zscore"]

third_ops ='trade_when'

class Improvement:
    """
    Alpha改进器
    """
    def __init__(
            self
            , wqbs:wqb.WQBSession
            , dataset_id:str
            , begin_time:datetime
            , end_time:datetime
            , region:str='USA'
            , limit:int=100
    ):
        """
        初始化
        :param wqbs: wqb session
        :param begin_time: 开始时间
        :param end_time: 结束时间
        :param dataset_id: 数据集id
        :param region: 所在地区
        :param limit: 查询待提升原始数据量
        """
        self.wqbs = wqbs
        self.dataset = dataset_config.get_dataset_config(dataset_id)
        self.begin_time = begin_time
        self.end_time = end_time
        self.region = region
        self.limit = limit
        self.correlation = SelfCorrelation(wqbs=wqbs)


    def get_alpha_order(self, alpha_expr) -> int:
        """
        获取alpha是几阶
        """
        if alpha_expr.startswith(third_ops):
            return 3
        else:
            for op in group_ops:
                if alpha_expr.startswith(op) or alpha_expr.startswith(f'-{op}'):
                    return 2

        return 1
    
    def filtered_alphas(self,alpha_list:list, order):
        if alpha_list is None:
            return alpha_list

        filtered_alphas = [
            alpha for alpha in alpha_list 
            if self.get_alpha_order(alpha.get('regular', {}).get('code')) == order
            # 使用 .get() 避免 KeyError
        ]
        return filtered_alphas

    def get_alphas(self
        , sharpe:float=1.2
        , fitness:float=1.0
        , others:Iterable[str]=None
    ) -> list:
        """
        获取alpha列表
        :param sharpe: 夏普比率
        :param fitness: fitness
        :param others: 其他条件
        :return: alpha列表
        """
        date_created_range=wqb.FilterRange.from_str(f"[{self.begin_time.isoformat()}, {self.end_time.isoformat()})")
        list = utils.filter_alphas(
            wqbs=self.wqbs,
            status='UNSUBMITTED',
            region=self.region,
            delay=1,
            universe='TOP3000',
            sharpeFilterRange=wqb.FilterRange.from_str(f'[{sharpe}, inf)'),
            fitnessFilterRange=wqb.FilterRange.from_str(f'[{fitness}, inf)'),
            dateCreatedFilterRange=date_created_range,
            order='is.sharpe',
            others=others,
            log_name=f"{self.__class__}#get_alphas"
        )
    
        if len(list) >= self.limit:
            return list[:self.limit]
        
        
        # 不够
        for i in range(len(others)):
            others[i]=others[i].replace('%3C', '%3E%3D-')
        # 获取负值
        print(f"没有足够的Alpha, 反转条件{others}...")

        list.extend(
            utils.filter_alphas(
                wqbs=self.wqbs,
                status='UNSUBMITTED',
                region=self.region,
                delay=1,
                universe='TOP3000',
                sharpeFilterRange=wqb.FilterRange.from_str(f'(-inf,{-sharpe}]'),
                fitnessFilterRange=wqb.FilterRange.from_str(f'(-inf,{-fitness}]'),
                dateCreatedFilterRange=date_created_range,
                order='-is.sharpe',
                others=others,
                log_name=f"{self.__class__}#get_alphas"
            )
        )

        if len(list) >= self.limit:
            return list[:self.limit]
        
        return list
        

    def first_improve(
            self
            , sharpe:float=1.25
            , fitness:float=0.75) -> list:
        """
        第一次改进
        :param sharpe: 改进后alpha的夏普比率
        :param fitness: 改进后alpha的fitness
        :return: 改进后的alpha
        """
        
        alphas = self.get_alphas(sharpe, fitness, others=['is.sharpe%3C1.4'])
        if len(alphas) == 0:
            print("没有Alpha可以改进...")
            return alphas
        alphas = self.filtered_alphas(alphas, 1)

        # 过滤自相关
        # alphas = self.correlation.filter_correlation(alphas,threshold=0.7)
        # if len(alphas) == 0:
        #     print("没有自相关小于0.68的Alpha...")
        #     return alphas
        fo_tracker = self.handle_alphas(alphas, sharpe)
        
        fo_layer = self.prune(fo_tracker, 5)
        
        sim_data_list = []
        settings = dataset_config.get_api_settings(self.dataset['id'])
        for expr, decay in fo_layer:
            for alpha in factory.get_group_second_order_factory([expr], group_ops, self.region):
                # 更新decay
                settings["decay"] = decay
                sim_data_list.append({
                    'type': 'REGULAR',
                    'settings': settings,
                    'regular': alpha
                })
       # 为什么要打乱顺序？
        random.shuffle(sim_data_list)
        print(f'第一次改进后有{len(sim_data_list)}个Alpha')
        print(f'前三个如下：\n{sim_data_list[:3]}')
        return sim_data_list

    def second_improve(
        self
        , sharpe:float=1.4
        , fitness:float=0.8
    ) -> list:
        """
        第二次改进
        :param begin_time: 开始时间
        :param end_time: 结束时间
        :param sharpe: 改进后alpha的夏普比率
        :param fitness: 改进后alpha的fitness
        :return: 改进后的alpha
        """
        alphas = self.get_alphas(sharpe, fitness, others=['is.sharpe%3C1.58'])
        if len(alphas) == 0:
            print(f"没有Alpha可以改进...")
            return alphas
        third_alpahs = [
            alpha for alpha in alphas 
            if alpha.get('regular', {}).get('code').startswith(third_ops)
        ]
        alphas = self.filtered_alphas(alphas, 2)

        
        # 过滤自相关
        # list = self.correlation.filter_correlation(alphas,0.68)
        # if len(list) == 0:
        #     print("没有自相关小于0.68的Alpha...")
        #     return list
        fo_tracker = self.handle_alphas(alphas, sharpe)
        
        so_layer = self.prune(fo_tracker, 5)
        sim_data_list = []
        settings = dataset_config.get_api_settings(self.dataset['id'])
        
        skip_cout = 0
        for expr, decay in so_layer:
            for alpha in factory.trade_when_factory(third_ops, expr):
                if alpha in third_alpahs:
                    skip_cout += 1
                    continue
                # 更新decay
                settings["decay"] = decay
                sim_data_list.append({
                    'type': 'REGULAR',
                    'settings': settings,
                    'regular': alpha
                })
        
        print(f'共跳过{skip_cout}个alpha')
        # 为什么要打乱顺序？
        # random.shuffle(sim_data_list)
        print(f'第二次改进后有{len(sim_data_list)}个Alpha')
        print(f'前三个如下：\n{sim_data_list[:3]}')
        return sim_data_list

    def handle_alphas(self, alphas: list, sharpe) -> list:
        """
        处理数据
        """
        output = []
        for alpha in alphas:
            longCount = alpha["is"]["longCount"]
            shortCount = alpha["is"]["shortCount"]
            #if (sharpe > 1.2 and sharpe < 1.6) or (sharpe < -1.2 and sharpe > -1.6):
            if (longCount + shortCount) > 100:
                longCount = alpha["is"]["longCount"]
                shortCount = alpha["is"]["shortCount"]
                alpha_id = alpha["id"]
                dateCreated = alpha["dateCreated"]
                sharpe = alpha["is"]["sharpe"]
                fitness = alpha["is"]["fitness"]
                turnover = alpha["is"]["turnover"]
                margin = alpha["is"]["margin"]
                
                decay = alpha["settings"]["decay"]
                exp = alpha['regular']['code']
                if sharpe <= -sharpe:
                    exp = "-%s"%exp
                rec = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
                # print(rec)
                if turnover > 0.7:
                    rec.append(decay*4)
                elif turnover > 0.6:
                    rec.append(decay*3+3)
                elif turnover > 0.5:
                    rec.append(decay*3)
                elif turnover > 0.4:
                    rec.append(decay*2)
                elif turnover > 0.35:
                    rec.append(decay+4)
                elif turnover > 0.3:
                    rec.append(decay+2)
                output.append(rec)
        
        return output

    def prune(self, next_alpha_recs, keep_num):
        # prefix is the datafield prefix, fnd6, mdl175 ...
        # keep_num is the num of top sharpe same-datafield alpha
        prefix = self.dataset['id']
        if self.dataset['field_prefix'] is not None:
            prefix = self.dataset['field_prefix']
        output = []
        num_dict = defaultdict(int)
        for rec in next_alpha_recs:
            exp = rec[1]
    
            if prefix in exp:
                idx = exp.index(prefix)
                exp_tmp = exp[idx:-1]
                field = exp_tmp.split(",")[0]
            else:
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

if  __name__ == "__main__":
    wqbs= wqb.WQBSession((utils.load_credentials('~/.brain_credentials.txt')), logger=wqb.wqb_logger(name='logs/wqb_' + datetime.now().strftime('%Y%m%d')))
   
    improvement = Improvement(
            wqbs
        , dataset_id='model77'
        , begin_time=datetime.fromisoformat('2025-06-13T00:00:00-05:00')
        , end_time=datetime.fromisoformat('2025-06-15T23:59:59-05:00')
        ,limit=20000
    )
    simulator = Simulator(wqbs, "./results/alpha_ids.csv", False)
    list=improvement.first_improve()
    list=list[3000:]
    if  len(list) > 0:
        simulator.simulate_alphas(list)
    list=improvement.second_improve()
    if  len(list) > 0:
        simulator.simulate_alphas(list)
