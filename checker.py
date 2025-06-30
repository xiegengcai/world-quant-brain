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
        # , concurrency: int = 8
        , batch_size:int=30
        , db_path:str="./db"
    ):
        """
        Args:
            wqbs: wqb.WQBSession
            concurrency: 并发数
            batch_size: 批量检查的alpha数量, 即多少个完成后更新一次数据库
            db_path: 数据库路径
        """
        self.wqbs = wqbs
        # self.concurrency = concurrency
        self.batch_size = batch_size
        self.mapper = AlphaMapper(db_path)


    def check(self,check_mod:int=1,sharpe: float=1.2, fitness: float=1.0):
        """
        检查alpha
        Args:
            check_mod: 检查模式, 1: 本地检查, 2: 服务器检查
            sharpe: sharpe阈值
            fitness: fitness阈值
        """
        page = 0
        # 指标太低无需检查
        metrics={constants.IS_SHARPE: sharpe, constants.IS_FITNESS: fitness}
        where = f'status = "{constants.ALPHA_STATUS_SYNC}"'
        for key, value in metrics.items():
            where += f' and ({key} >= {value} or {key} <= -{value})'
        count = self.mapper.count(where)
        if count == 0:
            print("没有需要检查的alpha了")
            return
        print(f'共有{count}个alpha待检查...')
        start_time = time.time()
        success_count = 0
        if check_mod == 1:
            self_corr = SelfCorrelation(self.wqbs)
        while True:
            batch_num = page + 1
            alphas = self.mapper.get_alphas(
                status=constants.ALPHA_STATUS_SYNC,
                metrics=metrics,
                page=page,page_size=self.batch_size
            )
            if len(alphas) == 0:
                print("没有需要检查的alpha了")
                break

            print(f'正在检查{batch_num}批, 共{len(alphas)}个alpha...')
            if check_mod == 1:
                success_count += self._local_check(page+1, alphas, self_corr)
            else:
                success_count += self.server_check(page+1, alphas, max_tries=range(600), log=f'{self.__class__}#check')
            print(f'第{batch_num}批耗时: {(time.time() - start_time):.2f}秒')
            page += 1
        end_time = time.time()
        print(f'检查结束,成功{success_count},失败{count-success_count}...')
        print("总耗时: {:.2f}秒".format(end_time - start_time))

    def server_check(self, batch_num:int, alphas: list, max_tries: int = range(600), log: str = '') -> int:
        """服务器检查alpha"""
        success_count = 0
        start_time = time.time()
        for alpha in alphas:
            alpha_id = alpha['alpha_id']
            try:
                resp = asyncio.run(
                    self.wqbs.concurrent_check(
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
                
                # perf = self.wqbs.get_performance(alpha_id=alpha_id)
                # print(f'alpha {alpha_id} 自相关性: {(self_corr_val):.2f}, {alpha_id} 性能: {perf}')
                # self.mapper.updateById(alpha['id'],  {'performance': perf, 'self_corr':self_corr_val, 'status':constants.ALPHA_STATUS_CHECKED})
                print(f'alpha {alpha_id} 自相关性: {(self_corr_val):.2f}')
                self.mapper.updateById(alpha['id'],  {'self_corr':self_corr_val, 'status':constants.ALPHA_STATUS_CHECKED})
                success_count += 1
            except Exception as e:
                print(f'检查alpha {alpha_id} 失败: {e}')
        end_time = time.time()
        print(f"第{batch_num}批耗时: {(end_time - start_time):.2f}秒")
        return success_count
    

    def _local_check(self, batch_num:int, alphas: list, self_corr:SelfCorrelation)-> int:
        """本地检查alpha"""
        success_count = 0
        start_time = time.time()
        for alpha in alphas:
            alpha_id = alpha['alpha_id']
            try:
                self_corr_val = self_corr.calc_self_corr(alpha_id)
                # perf = self.wqbs.get_performance(alpha_id=alpha_id)
                # print(f'alpha {alpha_id} 自相关性: {(self_corr_val):.2f}, {alpha_id} 性能: {perf}')
                # self.mapper.updateById(alpha['id'],  {'performance': perf, 'self_corr':self_corr_val, 'status':constants.ALPHA_STATUS_CHECKED})
                print(f'alpha {alpha_id} 自相关性: {(self_corr_val):.2f}')
                self.mapper.updateById(alpha['id'],  {'self_corr':self_corr_val, 'status':constants.ALPHA_STATUS_CHECKED})
                success_count += 1
            except Exception as e:
                print(f'计算alpha {alpha_id} 自相关性失败: {e}')
        end_time = time.time()
        print(f"第{batch_num}批耗时: {(end_time - start_time):.2f}秒")
        return success_count
                
        
if __name__ == '__main__':
    
    wqbs= wqb.WQBSession((utils.load_credentials("~/.brain_credentials.txt")), logger=wqb.wqb_logger(name='wqb_' + datetime.now().strftime('%Y%m%d')))
    Checker(
        wqbs, 
    ).check(1)