# -*- coding: utf-8 -*-

import constants
import wqb
from AlphaMapper import AlphaMapper

class Submitter:
    def __init__(self, wqbs: wqb.WQBSession,self_corr_threshold:float=0.6, submit_num:int=2, db_path:str="./db"):
        """
        Args:
            wqbs: WQBSession
            submit_num: 提交个数
            improve: 提升幅度
        """
        self.wqbs = wqbs
        self.self_corr_threshold = self_corr_threshold
        self.submit_num = submit_num
        self.mapper = AlphaMapper(db_path)
    def __del__(self):
        self.mapper.__del__()

    def submit(self):
        success = 0
        page = 0
        with True:
            # 1. 获取所有[status=3, self_corr<={self.self_corr_threshold}]的alpha
            alpha_list = self.mapper.get_alphas(status=constants.ALPHA_STATUS_CHECKED, self_corr=self.self_corr_threshold,page=page)
            if len(alpha_list) == 0:
                print(f'没有[status=3, self_corr<={self.self_corr_threshold}]的alpha...')
                return
            # 2. 提交
            for alpha in alpha_list:
                resp = self.wqbs.submit(alpha_id=alpha['alpha_id'], log=f'{self.__class__}#submit')
                # 3. 更新状态
                if resp.ok:
                    success += 1
                    self.mapper.updateById(alpha['id'], {'status': constants.ALPHA_STATUS_SUBMITTED})
                # 4. 提交个数达到要求
                if success >= self.submit_num:
                    break

            page +=1
       