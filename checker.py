# -*- coding: utf-8 -*-

import asyncio
from datetime import datetime
import time
import wqb

import utils

class Checker:
    def __init__(self, wqbs:wqb.WQBSession, dateCreatedFilterRange:wqb.FilterRange, out_put_path: str, is_consultant:bool=True, batch_size:int=30):
        self.wqbs = wqbs
        self.dateCreatedFilterRange = dateCreatedFilterRange
        self.out_put_path = out_put_path
        self.is_consultant = is_consultant
        self.batch_size = batch_size

    def check(self):
        alphas = utils.filter_alphas(
            self.wqbs,
            sharpeFilterRange=wqb.FilterRange.from_str('[1.25, inf)'),
            fitnessFilterRange=wqb.FilterRange.from_str('[1.0, inf)'),
            dateCreatedFilterRange=self.dateCreatedFilterRange,
            order='dateCreated',
            others=['color!=RED'],
            log_name=f'{self.__class__}#check'
        )
        alphas = [
            alpha for alpha in alphas if not alpha.get('color', None)
        ]
        alpha_ids = []
        failed_alpha_ids = []
        for alpha in alphas:
            is_check = alpha['is']['checks']
            results = [(j['result']).upper() for j in is_check]
            if "FAIL" in results:
                failed_alpha_ids.append(alpha['id'])
                continue
            alpha_ids.append(alpha['id'])

        failed_id_file =f'{self.out_put_path}/check_fail_ids.csv'
        with open(failed_id_file, 'r') as f:
            loc_failed_ids = [line.strip() for line in f.readlines() if line.strip()]

            failed_alpha_ids = list(set(failed_alpha_ids) - set(loc_failed_ids))

        if len(failed_alpha_ids) > 0:

            fail_id_batch = [failed_alpha_ids[i:i+self.batch_size] for i in range(0,len(failed_alpha_ids),self.batch_size)]
            for batch_list in fail_id_batch:
                path_data = []
                for alpha_id in batch_list:
                    path_data.append({'id':alpha_id, 'color':'RED'})
                self.wqbs.patch(f'{wqb.WQB_API_URL}/alphas', json=path_data)
        

        alpha_ids_file = f'{self.out_put_path}/check_pass_ids.csv'
        with open(alpha_ids_file, 'r') as f:
            loc_alpha_ids = [line.strip() for line in f.readlines() if line.strip()]
            alpha_ids = list(set(alpha_ids) - set(loc_alpha_ids))
        print(f'过滤后剩余{len(alpha_ids)}个Alpha...')
        # 按照batch_size分批
        # 按照batch_size分批
        batch_alpha_ids = [alpha_ids[i:i+self.batch_size] for i in range(0,len(alpha_ids),self.batch_size)]
        # concurrency = 10 if self.is_consultant else 3
        total_batch_num = len(batch_alpha_ids)
        print(f'本次检查共{total_batch_num}批, 每批{self.batch_size}个...')
        batch_num = 1
        start_time = time.time()
       
        for batch_list in batch_alpha_ids:
            print(f'正在检查{batch_num}/{total_batch_num}批...')
            utils.check(
                self.wqbs,
                batch_num=batch_num,
                alpha_ids=batch_list,
                out_put_path=self.out_put_path,
                local_check=False,
                # max_tries=100,
                log=f'{self.__class__}#check'
            )
            batch_num += 1
        end_time = time.time()
        print("总耗时: {:.2f}秒".format(end_time - start_time))


if __name__ == '__main__':
    
    wqbs= wqb.WQBSession((utils.load_credentials("~/.brain_credentials.txt")), logger=wqb.wqb_logger(name='logs/wqb_' + datetime.now().strftime('%Y%m%d')))
    Checker(
        wqbs, 
        dateCreatedFilterRange=wqb.FilterRange.from_str('[2025-06-01T00:00:00-05:00, 2025-06-10T23:59:59-05:00]'), 
        out_put_path='./results', 
        is_consultant=False
    ).check()