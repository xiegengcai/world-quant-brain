# -*- coding: utf-8 -*-

import asyncio
from datetime import datetime
import time
import constants
import wqb

from self_correlation import SelfCorrelation
import utils
from AlphaMapper import AlphaMapper

class Checker:
    def __init__(
            self
            , wqbs:wqb.WQBSession
            , dateCreatedFilterRange:wqb.FilterRange
            , is_consultant:bool=True
            , batch_size:int=30
            , db_path:str='./db'
    ):
        self.wqbs = wqbs
        self.dateCreatedFilterRange = dateCreatedFilterRange
        self.is_consultant = is_consultant
        self.batch_size = batch_size
        self.mapper = AlphaMapper(db_path)

    def __del__(self):
        self.mapper.__del__()

    def check(self):
        page = 0
        start_time = time.time()
        while True:
            alphas = self.mapper.get_alphas(status=constants.ALPHA_STATUS_SIMUATED,page=page,page_size=1000)
            if len(alphas) == 0:
                break
            page += 1
            # 按照batch_size分批
            batch_alpha_ids = [alphas[i:i+self.batch_size] for i in range(0,len(alphas),self.batch_size)]
            # concurrency = 10 if self.is_consultant else 3
            total_batch_num = len(batch_alpha_ids)
            print(f'本次检查共{total_batch_num}批, 每批{self.batch_size}个...')
            batch_num = 1
        
            for batch_list in batch_alpha_ids:
                print(f'正在检查{batch_num}/{total_batch_num}批...')
                self.do_check(
                    self.wqbs,
                    batch_num=batch_num,
                    alpha_ids=batch_list,
                    local_check=True,
                    # max_tries=100,
                    log=f'{self.__class__}#check'
                )
                batch_num += 1
        end_time = time.time()
        print("总耗时: {:.2f}秒".format(end_time - start_time))

    def do_check(self, batch_num:int, alpha_ids: list, local_check: bool = True, log: str = ''):
        """检查alpha"""
        # alpha_list = []
        start_time = time.time()
        self_corr = None
        if local_check:
            self_corr = SelfCorrelation(wqbs, data_path='./results')
        for alpha_id in alpha_ids:
            if local_check:
                alpha=self._local_check(alpha_id, self_corr)
            else:
                alpha=self.server_check(wqbs, alpha_id, log=log)
            if alpha is None:
                continue
            alpha['status'] = 3
            # alpha_list.append(alpha)
            self.mapper.updateByAlphaId(alpha_id, alpha)
        end_time = time.time()
        print(f"第{batch_num}批耗时: {(end_time - start_time):.2f}秒")
        # return alpha_list

    def server_check(self, alpha_id: str, max_tries: int = range(600), log: str = '') -> dict:
        """服务器检查alpha"""
        try:
            resp = asyncio.run(
                self.wqbs.check(
                    alpha_id,
                    max_tries=max_tries,
                    on_start=lambda vars: print(vars['url']),
                    on_finish=lambda vars: print(vars['resp']),
                    # on_success=lambda vars: print(vars['resp']),
                    # on_failure=lambda vars: print(vars['resp']),
                    log=log
                )
            )
            data = resp.json()
            is_check = data['is']['checks']
            self_corr_val = None
            for check in is_check:
                if check['name'] == 'SELF_CORRELATION':
                    self_corr_val = check['value']
                    break
            
            perf = self.wqbs.get_performance(alpha_id=alpha_id)
            print(f'alpha {alpha_id} 自相关性: {(self_corr_val):.2f}, {alpha_id} 性能: {perf}')
            return {'performance': perf, 'self_corr':self_corr_val}
        except Exception as e:
            print(f'检查alpha {alpha_id} 失败: {e}')
            return None
    

    def _local_check(self, alpha_id: str, self_corr:SelfCorrelation)-> dict:
        try:
            self_corr_val = self_corr.calc_self_corr(alpha_id)
            perf = self.wqbs.get_performance(alpha_id=alpha_id)
            print(f'alpha {alpha_id} 自相关性: {(self_corr_val):.2f}, {alpha_id} 性能: {perf}')
            return {'performance': perf, 'self_corr':self_corr_val}
        except Exception as e:
            print(f'计算alpha {alpha_id} 自相关性失败: {e}')
            return None
        
if __name__ == '__main__':
    
    wqbs= wqb.WQBSession((utils.load_credentials("~/.brain_credentials.txt")), logger=wqb.wqb_logger(name='logs/wqb_' + datetime.now().strftime('%Y%m%d')))
    Checker(
        wqbs, 
        wqb.FilterRange.from_str('[2025-06-21T00:00:00-05:00, 2025-06-22T23:59:59-05:00]'), 
        False
    ).check()