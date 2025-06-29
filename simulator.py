# -*- coding: utf-8 -*-

import asyncio
import json
import constants
import utils
import wqb
from AlphaMapper import AlphaMapper

class Simulator:
    def __init__(self,  wqbs: wqb.WQBSession, concurrency: int = 8, batch_size:int=30, db_path:str="./db"):
        """
        Args:
            wqbs: wqb.WQBSession
            concurrency: 并发数
            batch_size: 批量回测的alpha数量, 即多少个回测完成后更新一次数据库
            db_path: 数据库路径
        """
        self.wqbs = wqbs
        self.concurrency = concurrency
        self.batch_size = batch_size
        self.mapper = AlphaMapper(db_path)

    def simulate(self):
        """回测"""
        count = self.mapper.count(f'status = "{constants.ALPHA_STATUS_INIT}"')
        print(f'共有{count}个alpha待回测...')
        page = 0
        while True:
            alphas = self.mapper.get_alphas(page_size=self.batch_size, page=page)
            total = len(alphas)
            if total== 0:
                break
            print(f'第{page+1}批次{total}个用{self.concurrency}并发回测...')
            failed_count = self.do_simulate(alphas)
            print(f'第{page+1}批次{total}个, ✅成功：{total-failed_count} 个，❌失败：{failed_count} 个...')
            page += 1
        print(f'同步结束,成功{count-failed_count},失败{failed_count}...')
    
    def do_simulate(self, alphas:list, alpha_ids:list = None)->int:
        """回测
        return: 
            list of success alpha, list of failed alpha
        """
        # 构造数据
        alpha_list = []
        for alpha in alphas:
            settings = alpha['settings'].replace("'", '"')
            alpha_list.append({
                'type': alpha['type'],
                'settings': json.loads(settings),
                'regular': alpha['regular']
            })

        failed_count = 0
        
        resps = asyncio.run(
                self.wqbs.concurrent_simulate(
                    alpha_list,
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
                if resp is None or (hasattr(resp, 'status_code') and resp.status_code != 200): # 如果回测失败
                    failed_count +=1
                    print(f'回测 {alpha_list[idx]} 失败: status_code={resp.status_code}')
                    continue
                data = resp.json()
                hash_id = alphas[idx]['hash_id']
                alpha_ids.append(data['alpha'])
                self.mapper.updateByHashId(hash_id, {
                    'location_id':data['id']
                    , 'alpha_id':data['alpha']
                    , 'status':constants.ALPHA_STATUS_SIMUATED
                }) # 更新alpha
            except Exception as e:
                failed_count +=1
                print(f'回测 {alpha_list[idx]} 失败: {e}')
            
        return failed_count
           