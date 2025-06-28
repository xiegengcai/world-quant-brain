# -*- coding: utf-8 -*-
import asyncio
import constants
import wqb
from AlphaMapper import AlphaMapper
class Synchronizer:
    def __init__(self, wqbs: wqb.WQBSession,db_path:str="./db"):
        self.wqbs = wqbs
        self.mapper = AlphaMapper(db_path)

    def run(self):
        """
        开始同步
        """
        count = self.mapper.count(f'status = "{constants.ALPHA_STATUS_SIMUATED}"')
        print(f'共有{count}个alpha待同步...')
        page = 0
        page_size = 10
        while True:
            alphas = self.mapper.get_alphas(status=constants.ALPHA_STATUS_SIMUATED, page_size=page_size, page=page)
            total = len(alphas)
            if total== 0:
                break
            print(f'第{page+1}批次{total}个开始同步...')
            failed_count += self.sync(alphas)
            print(f'第{page+1}批次{total}个, ✅成功：{total-failed_count} 个，❌失败：{failed_count} 个...')
            page += 1
        print(f'同步结束,成功{count-failed_count},失败{failed_count}...')

    def sync(self, alphas:list) -> int:
        failed_count = 0
        for alpha in alphas:
            self.sync_alpha(alpha)
        return failed_count

    def sync_alpha(self, alpha:dict) -> int:
        alpha_id=alpha['alpha_id']
        resp = asyncio.run(self.wqbs.locate_alpha(
            alpha_id=alpha_id,
            log=f'{self.__class__}#sync'
        ))
        try:
            if resp is None or (hasattr(resp, 'status_code') and resp.status_code != 200):
                print(f'同步 {alpha['alpha_id']} 失败: {resp}')
                return 1
            data = resp.json()
            is_data = data['is']
            err = ''.join([f"{check['name']}:{check['result']}" for check in is_data['checks'] if check['result'] != 'PASS'])
            update_alpha = {
                'status': constants.ALPHA_STATUS_SYNC
                , constants.IS_FITNESS: is_data[constants.IS_FITNESS]
                , constants.IS_DRAWDOWN: is_data[constants.IS_DRAWDOWN]
                , constants.IS_LONGCOUNT: is_data[constants.IS_LONGCOUNT]
                , constants.IS_SHARPE: is_data[constants.IS_SHARPE]
                , constants.IS_SHORTCOUNT: is_data[constants.IS_SHORTCOUNT]
                , constants.IS_MARGIN: is_data[constants.IS_MARGIN]
                , constants.IS_TURNOVER: is_data[constants.IS_TURNOVER]
                , constants.IS_RETURNS: is_data[constants.IS_RETURNS]
                , 'grade':data['grade']
                , 'description':err
            }
            self.mapper.updateById(alpha['id'], update_alpha)
            return 0
        except Exception as e:
            print(f'同步 {alpha_id} 失败: {e}')
            return 1