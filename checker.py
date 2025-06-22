# -*- coding: utf-8 -*-

import asyncio
from datetime import datetime
import time
import wqb

from self_correlation import SelfCorrelation
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
            self.do_check(
                self.wqbs,
                batch_num=batch_num,
                alpha_ids=batch_list,
                out_put_path=self.out_put_path,
                local_check=True,
                # max_tries=100,
                log=f'{self.__class__}#check'
            )
            batch_num += 1
        end_time = time.time()
        print("总耗时: {:.2f}秒".format(end_time - start_time))

    def do_check(self, batch_num:int, alpha_ids: list, out_put_path: str, local_check: bool = True, log: str = ''):
        """检查alpha"""
        # total = len(alpha_ids)
        failed_alpha_ids = []
        patch_data = []
        start_time = time.time()
        self_corr = None
        if local_check:
            self_corr = SelfCorrelation(wqbs, data_path='./results')
        for alpha_id in alpha_ids:
            if local_check:
                color_data=self._local_check(alpha_id, failed_alpha_ids, self_corr, threshold=0.55)
            else:
                color_data=self.server_check(wqbs, alpha_id, failed_alpha_ids, log=log)
                patch_data.append(color_data)
            
        
        if len(failed_alpha_ids) > 0:
            alpha_ids = list(set(alpha_ids) - set(failed_alpha_ids))
            for id in failed_alpha_ids:
                patch_data.append({'id': id, 'color': 'RED'})

        patch_resp = wqbs.patch(f'{wqb.WQB_API_URL}/alphas', json=patch_data)
        
        if patch_resp.status_code == 200:
            # 写回文件
            fail_lines = [f'{id}\n' for id in failed_alpha_ids]
            pass_lines = [f'{id}\n' for id in alpha_ids]
            utils.save_lines_to_file(f'{out_put_path}/check_fail_ids.csv', fail_lines)
            utils.save_lines_to_file(f'{out_put_path}/check_pass_ids.csv', pass_lines)
            print(f"✅ 第{batch_num}批{len(patch_data)} 个Alpha检查完成...")
        else:
            print(f"❌ 第{batch_num}批{len(patch_data)} 个Alpha检查失败...")

        end_time = time.time()
        print(f"第{batch_num}批耗时: {(end_time - start_time):.2f}秒")

    def server_check(self, alpha_id: str, failed_alpha_ids:list, max_tries: int = range(600), log: str = '') -> dict:
        """服务器检查alpha"""
        color = 'GREEN' # 绿色
        try:
            resp = asyncio.run(
                wqbs.check(
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
            results = [(j['result']).upper() for j in is_check]
            # 全为pass
            if len(results) == 8 and len(set(results)) == 1 and 'PASS' in results:
                if not utils.is_favorable(wqbs, alpha_id,20):
                    color = 'PURPLE' # 紫色
            else:
                color='RED'
                failed_alpha_ids.append(alpha_id)
            return {
                'id': alpha_id, 'color':color
            }
        except Exception as e:
            print(f'检查alpha {alpha_id} 失败: {e}')
            return None
    

    def _local_check(self, alpha_id: str, failed_alpha_ids:list, self_corr:SelfCorrelation, threshold:float=0.7)-> dict:
        color = 'GREEN' # 绿色
        try:
            self_corr_val = self_corr.calc_self_corr(alpha_id)
            print(f'alpha {alpha_id} 自相关性: {(self_corr_val):.2f}')
            if self_corr_val > threshold:
                color='RED'
                failed_alpha_ids.append(alpha_id)
            else:
                if not utils.is_favorable(wqbs, alpha_id, 20):
                    color = 'PURPLE' # 紫色
            return {
                'id': alpha_id, 'color':color
            }
        except Exception as e:
            print(f'计算alpha {alpha_id} 自相关性失败: {e}')
            return None
        
if __name__ == '__main__':
    
    wqbs= wqb.WQBSession((utils.load_credentials("~/.brain_credentials.txt")), logger=wqb.wqb_logger(name='logs/wqb_' + datetime.now().strftime('%Y%m%d')))
    Checker(
        wqbs, 
        dateCreatedFilterRange=wqb.FilterRange.from_str('[2025-06-21T00:00:00-05:00, 2025-06-22T23:59:59-05:00]'), 
        out_put_path='./results', 
        is_consultant=False
    ).check()