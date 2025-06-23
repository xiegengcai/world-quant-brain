# -*- coding: utf-8 -*-

import asyncio
import constants
import utils
import wqb
from AlphaMapper import AlphaMapper

class Simulator:
    def __init__(self,  wqbs: wqb.WQBSession, concurrency: int = 8, total_size:int=10000, db_path:str="./db"):
        self.wqbs = wqbs
        self.concurrency = concurrency
        self.total_size = total_size
        self.batch_size = self.concurrency * 5
        self.mapper = AlphaMapper(db_path)
    def __del__(self):
        self.mapper.__del__()

    def simulate(self):
        """回测"""
        page = 0
        while True:
            alphas = self.mapper.get_alphas(page_size=self.total_size, page=page)
            if len(alphas) == 0:
                break
            self.do_simulate(alphas)
            page += 1
    
    def do_simulate(self, alphas:list):
        """回测
        return: 
            list of success alpha, list of failed alpha
        """
        # 构造数据
        alpha_list = []
        for alpha in alphas:
            alpha_list.append({
                'type': alpha['type'],
                'settings': alpha['settings'],
                'regular': alpha['regular']
            })
        failed_count = 0
        partitions = [alpha_list[i:i + self.batch_size] for i in range(0,len(alpha_list), self.batch_size)]
        total_batch = len(partitions)
        
        print(f'分{total_batch}批次，每批次{self.batch_size}个{self.concurrency}线程并发回测...')
        for list in partitions:

            resps = asyncio.run(
                self.wqbs.concurrent_simulate(
                    list,
                    self.concurrency,
                    return_exceptions=True,
                    on_nolocation=lambda vars: print(vars['target'], vars['resp'], sep='\n'),
                    on_start=lambda vars: print(vars['url']),
                    on_finish=lambda vars: print(vars['resp']),
                    on_success=lambda vars: print(vars['resp']),
                    # on_failure=lambda vars: print(vars['resp']),
                    log=f'{self.__class__}#simulate'
                )
            )
            for idx, resp in enumerate(resps, start=0):
                try:
                    if not resp.ok: # 如果回测失败
                        failed_count +=1
                        continue
                    data = resp.json()
                    hash_id = utils.hash({'regular': data['regular'],'settings':data['settings']})
                    self.mapper.updateByHashId(hash_id, {
                        'location_id':data['id']
                        , 'alpha_id':data['alpha']
                        , 'status':constants.ALPHA_STATUS_SIMUATED
                    }) # 更新alpha
                except Exception as e:
                    print(f'回测 {alpha_list[idx]} 失败: {e}')
        success_count = len(alpha_list) - failed_count
        print(f'批次 {success_count}/{total_batch} ✅成功：{success_count} 个，❌失败：{failed_count} 个...')    