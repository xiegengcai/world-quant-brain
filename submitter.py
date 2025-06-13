# -*- coding: utf-8 -*-

import asyncio
from datetime import datetime
import functools
import json
import pandas as pd
import wqb

from self_correlation import SelfCorrelation
import utils

class Submitter:
    def __init__(self, wqbs: wqb.WQBSession,begin_time:str,end_time:str, submit_num:int=2, checkRank:bool=False, improve:int=0):
        """
        Args:
            wqbs: WQBSession
            submit_num: 提交个数
            improve: 提升幅度
        """
        self.wqbs = wqbs
        self.begin_time = begin_time
        self.end_time = end_time
        self.submit_num = submit_num
        self.checkRank = checkRank
        self.improve = improve
        self.correlation = SelfCorrelation(wqbs=wqbs)

    def submit_fail(self, alpha_id: str, checks:list):
        fail_checks = []
        for check in checks:
            if check['result'] == 'FAIL':
                fail_checks.append(check)
        df = pd.DataFrame(fail_checks)
        self.wqbs.patch_properties(alpha_id,color='RED', log=f'{self.__class__}#submit_fail')
        # print(f'标记:{resp.status_code}')
        print(f'❌ Alpha {alpha_id} 提交失败: \n{df}')
    
    async def _submit(
        self,
        alpha_id: str,
        max_tries: int = 10000,
        log: str | None = '',
        retry_log: str | None = None
    ) -> bool:
        """
        提交 Alpha
        提交返回403就是失败(服务器check不通过)
        """
        start_time = datetime.now()
        print(f'Alpha {alpha_id} 提交中...')
        url = f'{wqb.WQB_API_URL}/alphas/{alpha_id}/submit'
        resp = self.wqbs.post(url, allow_redirects=False)
        if resp.status_code == 303:
            print(f' Alpha {alpha_id} 其它线程提交中...')
            return False
        if resp.status_code == 403:
            self.submit_fail(alpha_id, checks=resp.json()['is']['checks'])
            return False
        # print(f'url: {resp.headers[wqb.LOCATION]}')

        
        resp = await self.wqbs.retry(
            wqb.GET, url, max_tries=max_tries, log=retry_log
        )
        end_time = datetime.now()
        print(f'Alpha {alpha_id} 提交耗时: {(end_time - start_time).seconds}秒')
        if resp.status_code == 404:
            print(f'❌  Alpha {alpha_id} 提交超时')
            return False
        if resp.status_code == 403:
            self.submit_fail(alpha_id, checks=resp.json()['is']['checks'])
            return False
        if resp.status_code == 200:
            print(f'✅ Alpha {alpha_id} 提交成功！')
            return True
        print(f'❌ Alpha {alpha_id} 提交失败！status_code: {resp.status_code}')
        return False
    

                
    
    def submit(self, limit:int=500):
        alphas = utils.submitable_alphas(
            self.wqbs,
            start_time=self.begin_time,
            end_time=self.end_time,
            limit=limit, order='is.sharpe',others=['color!=RED','grade!=INFERIOR']
        )

        if len(alphas) == 0:
            print('没有可提交的 Alpha...')

            return 
        # 过滤掉有FAIL指标的的alpha
        alphas =  utils.filter_failed_alphas(self.wqbs, alphas)

        # alphas = [
        #     i for i in alphas
        #     if "PENDING" not in [j['result'].upper() for j in i['is']['checks']]
        # ]
        # 排序
        alphas = sorted(alphas, key=functools.cmp_to_key(utils.sort_by_grade))
        # if len(alphas) >= self.submit_num:
        #     print(f'提交所有指标都PASS的Alpha...')
        #     alphas = alphas[:self.submit_num]

        # 自相关性过滤
        # alphas = self.correlation.filter_correlation(alphas, 0.5)
        print(f'过滤后共 {len(alphas)} 个 Alpha 可提交...')
        alpha_ids = []
        for alpha in alphas:
            is_data = alpha['is']
            if is_data['returns'] <= is_data['turnover']:
                continue
            if self.checkRank:
            
                if not utils.is_favorable(wqbs=self.wqbs, alpha_id=alpha['id'], improve=self.improve):
                    print(f'❌ Alpha {alpha["id"]} 不符合提升{self.improve}名次！')
                    continue
                alpha_ids.append(alpha['id'])
                print(f'✅ Alpha {alpha["id"]} 可提升{self.improve}名次！')
            else:
                alpha_ids.append(alpha['id'])
        self.submit_new(alpha_ids)


    def submit_new(self, alpha_ids:list)->list[str]:
        start_time = datetime.now()
        if len(alpha_ids) == 0:
            print('没有可提交的 Alpha...')
        processed_ids = []
        sussess_ids = []
        for alpha_id in alpha_ids:
            if asyncio.run(
                self._submit(
                    alpha_id,
                    log=f'{self.__class__}#submit'
                )
            ):
                sussess_ids.append(alpha_id)
            processed_ids.append(alpha_id)
            if len(sussess_ids) >= self.submit_num:
                break
        print(f'共提交 {len(sussess_ids)} 个 Alpha: {sussess_ids}')
        # 做一次增量下载
        self.correlation.download_data(flag_increment=True)
        print(f'总耗时: {(datetime.now() - start_time).seconds}秒')
        return processed_ids

if __name__ == '__main__':
    wqbs= wqb.WQBSession((utils.load_credentials("~/.brain_credentials.txt")), logger=wqb.wqb_logger(name='logs/wqb_' + datetime.now().strftime('%Y%m%d')))
    pass_res = json.load(open('pass_res.json'))
    # 根据相关性排序（修复部分）
    sorted_stage1_results = sorted(pass_res.items(), key=lambda x: x[1], reverse=False)
    # 过滤
    sorted_stage1_results = [result for result in sorted_stage1_results if float(result[1]) <= 0.5]
    if len(sorted_stage1_results) == 0:
        print('过滤后没有相关性低于等于0.5的Alpha。')
        exit(0)
    alpha_ids = [i[0] for i in sorted_stage1_results]
    
    # 提交
    processed_ids=Submitter(wqbs, begin_time=None, end_time=None, submit_num=4).submit_new(alpha_ids)
    # 删除已处理id
    pass_res = {k: v for k, v in pass_res.items() if k not in set(processed_ids)}
    
    json.dump(pass_res, open('pass_res.json', 'w'), indent=4)
