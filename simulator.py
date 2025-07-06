# -*- coding: utf-8 -*-

import asyncio
import json

from requests import Response
import constants
import utils
import wqb
from AlphaMapper import AlphaMapper

class Simulator:
    def __init__(self,  wqbs: wqb.WQBSession, concurrency: int = 8, db_path:str="./db"):
        """
        Args:
            wqbs: wqb.WQBSession
            concurrency: 并发数
            batch_size: 批量回测的alpha数量, 即多少个回测完成后更新一次数据库
            db_path: 数据库路径
        """
        self.wqbs = wqbs
        self.concurrency = concurrency
        self.batch_size = self.concurrency * 10
        self.mapper = AlphaMapper(db_path)

    def simulate(self):
        """回测"""
        count = self.mapper.count(f'status = "{constants.ALPHA_STATUS_INIT}"')
        print(f'共有{count}个alpha待回测...')
        page = 517
        success_count = 0
        while True:
            batch_num = page +1
            alphas = self.mapper.get_alphas(page_size=self.batch_size, page=page)
            total = len(alphas)
            if total== 0:
                break
            print(f'第{batch_num}批次{total}个用{self.concurrency}并发回测...')
            batch_success = self.do_simulate(alphas)
            success_count += batch_success
            print(f'第{batch_num}批次{total}个, ✅成功：{batch_success} 个，❌失败：{total-batch_success} 个...')
            page += 1
        print(f'同步结束,成功{success_count}个,失败{count-success_count}...')
    
    def do_simulate(self, alphas:list) -> int:
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

        success_count = 0

        if self.concurrency >= 3:
            multi_alphas = wqb.to_multi_alphas(alpha_list, 10)
            resps = asyncio.run(
                self.wqbs.concurrent_simulate(
                    multi_alphas,
                    self.concurrency,
                    return_exceptions=True,
                    on_nolocation=lambda vars: print(vars['target'], vars['resp'], sep='\n'),
                    on_start=lambda vars: print(vars['url']),
                    # on_finish=lambda vars: print(vars['resp']),
                    # on_success=lambda vars: print(vars['resp']),
                    on_failure=lambda vars: print(vars['resp']),
                    # tags=['MultiAlpha'],
                    log=f'{self.__class__}#simulate'
                )
            )
            # index = 0
            for idx, resp in enumerate(resps, start=0):
                try:
                    if resp and resp.status_code // 100 != 2:
                        continue
                    resp_json = json.loads(resp.text)
                    children_ids = resp_json.get("children", [])
                    if len(children_ids) == 0:
                        continue
            
                    for index, child_id in enumerate(children_ids, start=0):
                    # for child_id in children_ids:
                        try:
                            # 获取子模拟状态
                            child_simulation_url = (
                                f"{wqb.URL_SIMULATIONS}/{child_id}"  # 构建子模拟 URL
                            )
                            child_resp = asyncio.run(
                                self.wqbs.retry(
                                    wqb.GET, child_simulation_url, max_tries=range(60)
                                )
                            )
                            # 获取子模拟状态
                            if child_resp.status_code // 100 == 2:
                                success_count += self.deal_resp(child_resp, alpha_list[idx*10+index])
                        except Exception as e:
                            print(f"child_resp异常{e}")
                except Exception as e:
                    print(f"外层响应异常{e}")
                        
        else:
            resps = asyncio.run(
                self.wqbs.concurrent_simulate(
                    alpha_list,
                    self.concurrency,
                    return_exceptions=True,
                    on_nolocation=lambda vars: print(vars['target'], vars['resp'], sep='\n'),
                    on_start=lambda vars: print(vars['url']),
                    # on_finish=lambda vars: print(vars['resp']),
                    # on_success=lambda vars: print(vars['resp']),
                    on_failure=lambda vars: print(vars['resp']),
                    # tags=['Alpha'],
                    log=f'{self.__class__}#simulate'
                )
            )
            for idx, resp in enumerate(resps, start=0):
                success_count += self.deal_resp(resp, alpha_list[idx])

        return success_count

    def deal_resp(self, resp:Response, alpha:dict) -> int:
        """处理回测结果"""
        try:
            if resp.status_code // 100 != 2:
                return
            data = json.loads(resp.text)
            hash_id = utils.hash(alpha)
            print(f'{data['id']}回测成功:alpha_id={data["alpha"]}, hash_id={hash_id}')
            self.mapper.updateByHashId(hash_id, {
                'location_id':data['id']
                , 'alpha_id':data['alpha']
                , 'status':constants.ALPHA_STATUS_SIMUATED
            })
            return 1
        except Exception as e:
            print(f'回测 {alpha} 失败: {e}')
            return 0


            
           