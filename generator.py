# -*- coding: utf-8 -*-


import pandas as pd
import wqb

import dataset_config
import factory
import utils

class Generator:
    """
    Alpha生成器
    """
    def __init__(
            self
            , wqbs:wqb.WQBSession
            ,dataset_id:str
        ):
        """
        初始化
        :param wqbs: wqb session
        :param dataset_id: 数据集id
        """
        self.wqbs = wqbs
        self.dataset_id = dataset_id
        self.settings = dataset_config.get_api_settings(self.dataset_id )

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
    
    def generate(self) -> list:
        print(f"📋 获取数据集{self.dataset_id}字段列表...")
        # 1. 获取数据集字段
        fields = utils.get_dataset_fields(self.wqbs, self.dataset_id)
        print(f'📋 数据集{self.dataset_id}共{len(fields)}个字段...')
        # 2. 处理数据字段
        df = pd.DataFrame(fields)
        print(f'📋 开始处理字段...')
        pc_fields = self.process_datafields(df, "matrix")
        print(f'📋 处理结束，共{len(pc_fields)}个字段...')
        print(f'📋 开始构建表达式...')
        # 3. 构建表达式
        first_order = factory.first_order_factory(pc_fields, factory.ts_ops)
        print(f'📋 构建结束，共{len(first_order)}个表达式, 前五个表达式如下: \n{first_order[:5]}')
        return factory.generate_sim_data(self.dataset_id, first_order)
