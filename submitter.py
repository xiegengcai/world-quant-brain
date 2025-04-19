# -*- coding: utf-8 -*-

import asyncio
import pandas as pd
import wqb

import utils

class Submitter:
    def __init__(self, wqbs: wqb.WQBSession, submit_num:int=2, checkRank:bool=False, improve:int=0):
        """
        Args:
            wqbs: WQBSession
            submit_num: 提交个数
            improve: 提升幅度
        """
        self.wqbs = wqbs
        self.submit_num = submit_num
        self.checkRank = checkRank
        self.improve = improve

    def submit_fail(self, alpha_id: str, checks:list):
        fail_checks = []
        for check in checks:
            if check['result'] == 'FAIL':
                fail_checks.append(check)
        df = pd.DataFrame(fail_checks)
        print(f'❌ Alpha {alpha_id} 提交失败: \n{df}')
    
    async def _submit(
        self,
        alpha_id: str,
        max_tries: int = 100,
        log: str | None = '',
        retry_log: str | None = None
    ) -> bool:
        """
        提交 Alpha
        提交返回403就是失败(服务器check不通过)
        """
        url = f'{wqb.WQB_API_URL}/alphas/{alpha_id}/submit'
        resp = self.wqbs.post(url)
        if resp.status_code == 403:
            self.submit_fail(alpha_id, checks=resp.json()['is']['checks'])
            return False
        print(f'url: {resp.headers[wqb.LOCATION]}')
        
        resp = await self.wqbs.retry(
            wqb.GET, url, max_tries=max_tries, log=retry_log
        )
        
        if resp.status_code == 403:
            self.submit_fail(alpha_id, checks=resp.json()['is']['checks'])
            return False
        print(f'✅ Alpha {alpha_id} 提交成功！')
        return True
    
    def submit(self, limit:int=500, offset:int=0, sussess_count:int=0):
        alphas = utils.submitable_alphas(wqbs=self.wqbs, limit=limit, order='is.sharpe', offset=offset)

        if len(alphas) == 0:
            print('没有可提交的 Alpha...')
            return 
        
        # 过滤
        alphas = utils.filter_failed_alphas(alphas)
        print(f'过滤后共 {len(alphas)} 个 Alpha 可提交...')
        for alpha in alphas:
            if self.checkRank:
            
                if not utils.is_favorable(wqbs=self.wqbs, alpha_id=alpha['id'], improve=self.improve):
                    print(f'❌ Alpha {alpha["id"]} 不符合提升{self.improve}名次！')
                    continue
            
                print(f'✅ Alpha {alpha["id"]} 可提升{self.improve}名次！')
            if asyncio.run(
                self._submit(
                    alpha_id=alpha['id'],
                    # on_start=lambda vars: print(vars['url']),
                    # on_finish=lambda vars: print(vars['resp']),
                    # on_success=lambda vars: print(vars['resp']),
                    # on_failure=lambda vars: print(vars['resp']),
                    log='AutoSubmit#submit'
                )
            ):
                sussess_count += 1

            if sussess_count > self.submit_num:
                break
        return True

    def run(self):
        limit = 100
        offset = 0
        sussess_count = 0
        while True:
            if not self.submit(limit=limit, offset=offset, sussess_count=sussess_count):
                break
            if  sussess_count > self.submit_num:
                break
            offset += limit

        print(f'共提交 {sussess_count} 个 Alpha...')


    