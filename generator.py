# -*- coding: utf-8 -*-


from collections import defaultdict
import pandas as pd
import constants
import wqb

import dataset_config
import factory
import utils
from AlphaMapper import AlphaMapper

class Generator:
    """
    Alpha生成器
    """
    def __init__(
            self
            , wqbs:wqb.WQBSession
            , db_path:str='./db'
        ):
        """
        初始化
        :param wqbs: wqb session
        :param dataset_id: 数据集id
        """
        self.wqbs = wqbs
        self.mapper = AlphaMapper(db_path)

    def process_datafields(self, df, data_type):
        """处理数据字段"""
        if data_type == "matrix":
            datafields = df[df['type'] == "MATRIX"]["id"].tolist()
        elif data_type == "vector":
            datafields = self.get_vec_fields(df[df['type'] == "VECTOR"]["id"].tolist())

        tb_fields = []
        for field in datafields:
            tb_fields.append("winsorize(ts_backfill(%s, 120), std=4)"%field)
        return tb_fields
    
    def get_vec_fields(self,fields):
        vec_ops = ["vec_avg", "vec_sum"]
        vec_fields = []
    
        for field in fields:
            for vec_op in vec_ops:
                if vec_op == "vec_choose":
                    vec_fields.append("%s(%s, nth=-1)"%(vec_op, field))
                    vec_fields.append("%s(%s, nth=0)"%(vec_op, field))
                else:
                    vec_fields.append("%s(%s)"%(vec_op, field))
    
        return(vec_fields)
    
    def generate_first_with_fields(self, fields:list):
        print(f"📋 根据指定字段列表构建...")
        first_order = factory.first_order_factory(fields, factory.ts_ops)
        print(f'📋 构建结束，共{len(first_order)}个表达式, 前五个表达式如下: \n{first_order[:5]}')
        sim_data_list =  factory.generate_sim_data('', first_order)
        print(f'📋 生成结束，共{len(sim_data_list)}个alpha...')
        print(f'📋 开始保存alpha...')
        self.mapper.bath_save(sim_data_list)
        print(f'📋 保存结束...')
    
    def generate_first(self, dataset_id:str):
        print(f"📋 获取数据集{dataset_id}字段列表...")
        # 1. 获取数据集字段
        fields = utils.get_dataset_fields(self.wqbs, dataset_id)
        print(f'📋 数据集{dataset_id}共{len(fields)}个字段...')
        # 2. 处理数据字段
        df = pd.DataFrame(fields)
        print(f'📋 开始处理字段...')
        pc_fields = self.process_datafields(df, "matrix")
        print(f'📋 处理结束，共{len(pc_fields)}个字段...')
        print(f'📋 开始构建表达式...')
        # 3. 构建表达式
        first_order = factory.first_order_factory(pc_fields, factory.ts_ops)
        print(f'📋 构建结束，共{len(first_order)}个表达式, 前五个表达式如下: \n{first_order[:5]}')
        prefix= dataset_config.get_dataset_config(dataset_id)['field_prefix']
        sim_data_list =  factory.generate_sim_data(dataset_id, first_order)
        print(f'📋 生成结束，共{len(sim_data_list)}个alpha...')
        print(f'📋 开始保存alpha...')
        self.mapper.bath_save(sim_data_list,field_prefix=prefix)
        print(f'📋 保存结束...')

    def generate_second(self, group_ops:list,sharpe: float=1.2, fitness: float=1.0, self_corr: float=0.6):
        """
        查询一阶生成二阶alpha
        :param sharpe: sharpe系数
        :param fitness: fitness系数
        """
        page = 0

        while True:
            alphas = self.mapper.get_alphas(
                status=constants.ALPHA_STATUS_SYNC
                ,metrics={constants.IS_SHARPE: sharpe, constants.IS_FITNESS: fitness}
                , self_corr=0.6
                , step=1
                , page=page
            )
            if len(alphas) == 0:
                print(f'没有符合条件[sharpe={sharpe}, fitness={fitness}, self_corr={self_corr}]的一阶的alpha...')
                break
            fo_tracker = self.handle_alphas(alphas, sharpe)
            fo_layer = self.prune(fo_tracker, 5)
            sim_data_list = self._generate_second(group_ops, fo_layer)
            print(f'📋 生成结束，共{len(sim_data_list)}个alpha...')
            print(f'📋 开始保存alpha...')
            self.mapper.bath_save(sim_data_list,step=2)
            page += 1
        
    def _generate_second(self, group_ops:list,fo_layer):
        sim_data_list = []
        settings = dataset_config.default_settings
        for expr, decay in fo_layer:
            for alpha in factory.get_group_second_order_factory([expr], group_ops, self.region):
                # 更新decay
                settings["decay"] = decay
                sim_data_list.append({
                    'type': 'REGULAR',
                    'settings': settings,
                    'regular': alpha
                })
        # random.shuffle(sim_data_list)

        return sim_data_list
    
    def generate_third(self, third_op:str, sharpe: float=1.4, fitness: float=1.0, self_corr: float=0.6):
        """
        查询二阶生成三阶alpha
        :param sharpe: sharpe系数
        :param fitness: fitness系数
        """
        page = 0
        while True:
            alphas = self.mapper.get_alphas(
                status=constants.ALPHA_STATUS_SYNC
                , metrics={constants.IS_SHARPE: sharpe, constants.IS_FITNESS: fitness}
                , self_corr=0.6
                , step=2
                , page=page
            )
            if len(alphas) == 0:
                print(f'没有符合条件[sharpe={sharpe}, fitness={fitness}, self_corr={self_corr}]的一阶的alpha...')
                break
            fo_tracker = self.handle_alphas(alphas, sharpe)
            fo_layer = self.prune(fo_tracker, 5)
            sim_data_list = self._generate_second(third_op,fo_layer)
            print(f'📋 生成结束，共{len(sim_data_list)}个alpha...')
            print(f'📋 开始保存alpha...')
            self.mapper.bath_save(sim_data_list,step=3)
            page += 1

    def _generate_third(self, third_op:str, fo_layer):
        sim_data_list = []
        settings = dataset_config.default_settings
        for expr, decay in fo_layer:
           for alpha in factory.trade_when_factory(third_op, expr):
                # 更新decay
                settings["decay"] = decay
                sim_data_list.append({
                    'type': 'REGULAR',
                    'settings': settings,
                    'regular': alpha
                })
        # random.shuffle(sim_data_list)
        return sim_data_list
    def handle_alphas(self, alphas: list, sharpe) -> list:
        """
        处理数据
        """
        output = []
        for alpha in alphas:
            longCount = alpha["longCount"]
            shortCount = alpha["shortCount"]
            #if (sharpe > 1.2 and sharpe < 1.6) or (sharpe < -1.2 and sharpe > -1.6):
            if (longCount + shortCount) > 100:
                longCount = alpha["longCount"]
                shortCount = alpha["shortCount"]
                alpha_id = alpha["id"]
                dateCreated = alpha["dateCreated"]
                sharpe = alpha["sharpe"]
                fitness = alpha["fitness"]
                turnover = alpha["turnover"]
                margin = alpha["margin"]
                
                decay = alpha["settings"]["decay"]
                exp = alpha['regular']
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
        output.append(alpha['field_prefix'])
        return output

    def prune(self,next_alpha_recs, keep_num, prefix:str=''):
        # prefix is the datafield prefix, fnd6, mdl175 ...
        # keep_num is the num of top sharpe same-datafield alpha
        # 放在最后
        prefix = next_alpha_recs[-1]
        output = []
        num_dict = defaultdict(int)
        for rec in next_alpha_recs:
            exp = rec[1]
    
            if prefix != '' and prefix in exp:
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
