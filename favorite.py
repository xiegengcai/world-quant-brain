# -*- coding: utf-8 -*-
import time
import wqb

from self_correlation import SelfCorrelation
import utils

class FavoriteAlpha:
    def __init__(self, wqbs:wqb.WQBSession,begin_time:str,end_time:str):
        self.wqbs = wqbs
        self.correlation = SelfCorrelation(self.wqbs)
        self.begin_time = begin_time
        self.end_time = end_time
        self.searchScope = {'region': 'USA', 'delay': 1, 'universe': 'TOP3000'}
    
    def is_favorable(self,alpha_id:str) -> bool:
        """判断 Alpha 是可收藏的"""
        resp = self.wqbs.get(f'{wqb.WQB_API_URL}/competitions/IQC2025S1/alphas/{alpha_id}/before-and-after-performance')
        retry_after = float(resp.headers.get("Retry-After", 0))
        if retry_after > 0:
            time.sleep(retry_after)
            return self.is_favorable(alpha_id)
        score = resp.json()['score']
        return score['after']>score['before']

    
    def add_favorite(self,limit:int):
        """添加收藏夹"""
        alphas = utils.submitable_alphas(
            self.wqbs
            ,start_time=self.begin_time
            ,end_time=self.end_time
            ,limit=limit, 
            others=['favorite=false']
        )
        if len(alphas) == 0:
            print('没有可收藏的 Alpha...')
            return
        
        print(f'共 {len(alphas)} 个 Alpha 可收藏...')
        # 过滤
        alphas = utils.filter_failed_alphas(self.wqbs, alphas)
        print(f'过滤失败项后共 {len(alphas)} 个 Alpha 可收藏...')
        # 自相关性过滤
        print(f'开始过滤自相关性(<0.7)...')
        alphas = self.correlation.filter_correlation(alphas,threshold=0.7)
        print(f'过滤后共 {len(alphas)} 个 Alpha 可收藏...')
        batch_num = 0
        for i in range(0,len(alphas),20):
            batch_num += 1
            list = alphas[i:i+20]
            print(f"正在检查第{batch_num}批{len(list)} 个Alpha...")
            favorable_data = []
            for alpha in list:
                if utils.is_favorable(self.wqbs, alpha_id=alpha['id']):
                    favorable_data.append({
                        'id':alpha['id']
                        ,'favorite':True
                    })
                
            if len(favorable_data) == 0:
                print(f"第{batch_num}批{len(list)} 个Alpha没有可以收藏的...")
                continue
            print(f"正在添加第第{batch_num}批{len(favorable_data)} 个alpha到收藏夹...")
            fav_resp = self.wqbs.patch(f'{wqb.WQB_API_URL}/alphas', json=favorable_data)
            if fav_resp.status_code == 200:
                print(f"✅ 第{batch_num}批{len(favorable_data)} 个Alpha添加收藏夹成功...")
            else:
                print(f"❌ 第{batch_num}批{len(favorable_data)} 个Alpha添加收藏夹失败...")
            
