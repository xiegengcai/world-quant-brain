# -*- coding: utf-8 -*-

from collections import defaultdict
from datetime import datetime
import random
import wqb
import dataset_config
import factory

class Improvement:
    """
    Alpha改进器
    """
    def __init__(
            self
            , wqbs:wqb.WQBSession
            , dataset_id:str
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
        self.dataset_id = dataset_id
        self.region = region
        self.limit = limit

    def get_alphas(
        self
        , date_created_range: wqb.FilterRange
        , sharpe:float=1.2
        , fitness:float=1.0
    ) -> list:
        """
        获取alpha列表
        :param date_created_range: alpha创建时间范围
        :param sharpe: 夏普比率
        :param fitness: fitness
        :return: alpha列表
        """
        resp = self.wqbs.filter_alphas_limited(
            status='UNSUBMITTED',
            region=self.region,
            delay=1,
            universe='TOP3000',

            sharpe=wqb.FilterRange.from_str(f'[{sharpe}, inf)'),
            fitness=wqb.FilterRange.from_str(f'[{fitness}, inf)'),
            date_created=date_created_range,
            order='is.sharpe',
            limit=self.limit,
            log=f"{self}#get_alphas"
        )
        alpha_list = resp.json()['results']
        if len(alpha_list) == self.limit:
            return alpha_list
        
        # 不够
        resp = self.wqbs.filter_alphas_limited(
            status='UNSUBMITTED',
            region=self.region,
            delay=1,
            universe='TOP3000',
            sharpe=wqb.FilterRange.from_str(f'(-inf,{-sharpe}]'),
            fitness=wqb.FilterRange.from_str(f'(-inf,{-fitness}]'),
            date_created=date_created_range,
            order='-is.sharpe',
            limit=self.limit,
            log=f"{self}#get_alphas"
        )
        alpha_list.extend(resp.json()['results'])
        return alpha_list
    
    def first_improve(
            self
            , begin_time:datetime
            , end_time:datetime
            , sharpe:float=1.2, fitness:float=1.0) -> list:
        """
        第一次改进
        :param begin_time: 开始时间
        :param end_time: 结束时间
        :param sharpe: 改进后alpha的夏普比率
        :param fitness: 改进后alpha的fitness
        :return: 改进后的alpha
        """
        date_created_range=wqb.FilterRange.from_str(f"[{begin_time.isoformat()}, {end_time.isoformat()})")
        list = self.get_alphas(date_created_range, sharpe, fitness)
        if len(list) == 0:
            print(f"没有Alpha可以改进...")
            return
        
        fo_tracker = self.handle_alphas(list, sharpe)
        
        fo_layer = self.prune(fo_tracker, self.dataset_id, 5)
        group_ops = ["group_neutralize", "group_rank", "group_zscore"]
        sim_data_list = []
        settings = dataset_config.get_api_settings(self.dataset_id)
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
        , begin_time:datetime
        , end_time:datetime
        , sharpe:float=1.4
        , fitness:float=1.0
    ) -> list:
        """
        第二次改进
        :param begin_time: 开始时间
        :param end_time: 结束时间
        :param sharpe: 改进后alpha的夏普比率
        :param fitness: 改进后alpha的fitness
        :return: 改进后的alpha
        """
        date_created_range=wqb.FilterRange.from_str(f"[{begin_time.isoformat()}, {end_time.isoformat()})")
        list = self.get_alphas(date_created_range, sharpe, fitness)
        if len(list) == 0:
            print(f"没有Alpha可以改进...")
            return
        
        fo_tracker = self.handle_alphas(list, sharpe)
        
        so_layer = self.prune(fo_tracker, self.dataset_id, 5)
        sim_data_list = []
        settings = dataset_config.get_api_settings(self.dataset_id)
        for expr, decay in so_layer:
            for alpha in factory.trade_when_factory("trade_when", expr, self.region):
                # 更新decay
                settings["decay"] = decay
                sim_data_list.append({
                    'type': 'REGULAR',
                    'settings': settings,
                    'regular': alpha
                })
        # 为什么要打乱顺序？
        random.shuffle(sim_data_list)
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
                if sharpe < -sharpe:
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

    def prune(self, next_alpha_recs, prefix, keep_num):
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