# -*- coding: utf-8 -*-

import constants
from datetime import datetime
import SqliteHelper
import utils
import json


class AlphaMapper:
    def __init__(self, base_path:str):
        self.db = SqliteHelper.Connect(f"{base_path}/quant_brain.db")
        """
        hash_id:simulate_data哈希值
        location_id:回测完成后获取查询进度位于ID
        alpha_id:回测完成后获取
        simulate_data:回测数据
        performance:回测完成后获取
        self_corr:自相关性
        step: 1:一阶 2:二阶 3:三阶
        status: 参阅 constants.py
        parent_id: 鲁棒测试模拟回测使用的对照alpha_id
        """
        self.db.table('t_alpha').create({
            'id': 'INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT',
            'hash_id': 'TEXT NOT NULL UNIQUE',
            'location_id': 'TEXT DEFAULT NULL',
            'alpha_id': 'TEXT DEFAULT NULL',
            'type': 'TEXT NOT NULL DEFAULT "REGULAR"',
            'parent_id': 'TEXT DEFAULT NULL',
            'step': 'INTEGER NOT NULL',
            'field_prefix': 'TEXT DEFAULT ""',
            'regular':'TEXT DEFAULT NULL',
            'settings': 'TEXT NOT NULL',
            'performance': 'INTEGER  DEFAULT 0',
            'self_corr':'REAL DEFAULT 0',
            'sharpe':'REAL DEFAULT 0',
            'turnover':'REAL DEFAULT 0',
            'returns':'REAL DEFAULT 0',
            'drawdown':'REAL DEFAULT 0',
            'margin':'REAL DEFAULT 0',
            'fitness':'REAL DEFAULT 0',
            'margin':'REAL DEFAULT 0',
            'shortCount':'REAL DEFAULT 0',
            'grade': 'TEXT DEFAULT NULL',
            'status': 'TEXT NOT NULL',
            'description': 'TEXT DEFAULT NULL',
            'created_at': 'TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP',
            'updated_at': 'TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP',

        })

    
    def __del__(self):
        self.db.close()

    
    def bath_save(self, simulate_data_list:list, field_prefix:str='', step:int=1):
        """"
        批量插入数据
        """
        table_data = []
        now = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        for simulate_data in simulate_data_list:
            hash_id = utils.hash(simulate_data)
            if self.is_exist(hash_id):
                print(f'数据已存在,跳过: {hash_id}')
                continue
            table_data.append({
                'hash_id': hash_id,
                'step': step,
                'type': simulate_data['type'],
                'field_prefix': field_prefix,
                'settings': json.dumps(simulate_data['settings']),
                'regular': simulate_data['regular'],
                'status': constants.ALPHA_STATUS_INIT,
                'created_at' : now,
                'updated_at' : now,
            })

        self.db.table('t_alpha').data(table_data).add()

    def get_alpha(self, alpha:dict):
        """
        获取alpha数据
        """
        alphas = self.db.table('t_alpha').where(alpha).find(1)
        if len(alphas) > 0:
            return alphas[0]
        return None

    def get_alphas(
            self
            , begin_date:str=None
            , end_date:str=None
            , status:str=constants.ALPHA_STATUS_INIT
            , self_corr:float=None
            , step:int=0
            , metrics:dict=None 
            , page_size:int=100
            , page:int=0) -> list:
        """
        获取alpha数据
        @param begin_date: 开始日期
        @param end_date: 结束日期
        @param status: 状态
        @param self_corr: 自相关性
        @param step: 阶数
        @param metrics: 指标数据
        @param page_size: 每页数量
        @param page: 页码
        """
        where = f'status = "{status}"'

        if begin_date:
            where += f' and created_at >= "{begin_date}"'
        if end_date:
            where += f' and created_at < "{end_date}"'
        if self_corr:
            where += f' and self_corr <= {self_corr}'
        if step > 0:
            where += f' and step = {step}'
        # 指标数据
        if metrics:
            for key, value in metrics.items():
                where += f' and ({key} >= {value} or {key} <= -{value})'

        return self.db.table('t_alpha').where(where).order('created_at asc').find(page_size, page)
        

    def updateById(self, id:str, alpha:dict):
        """
        更新数据状态
        """
        alpha['updated_at'] = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        self.db.table('t_alpha').where(f'id = {id}').save(alpha)

    def updateByHashId(self, hash_id:str, alpha:dict):
        """
        更新数据状态
        """
        alpha['updated_at'] = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        self.db.table('t_alpha').where(f"hash_id = '{hash_id}'").save(alpha)

    def updateByLocationId(self, location_id:str, alpha:dict):
        """
        更新数据状态
        """
        alpha['updated_at'] = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        self.db.table('t_alpha').where(f"location_id = '{location_id}'").save(alpha)

    def updateByAlphaId(self, alpha_id:str, alpha:dict):
        """
        更新数据状态
        """
        alpha['updated_at'] = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        self.db.table('t_alpha').where(f"alpha_id = '{alpha_id}'").save(alpha)

    def is_exist(self, hash_id:str) -> bool:
        """
        判断数据是否存在
        """
        return self.get_alpha({'hash_id': hash_id}) is not None