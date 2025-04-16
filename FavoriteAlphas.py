# -*- coding: utf-8 -*-
import time
import wqb
from wqb import FilterRange

class FavoriteAlphas:
    def __init__(self, wqbs:wqb.WQBSession):
        self.wqbs = wqbs
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
        resp = self.wqbs.filter_alphas_limited(
            status='UNSUBMITTED',
            region=self.searchScope['region'],
            delay=1,
            favorite=False,
            universe='TOP3000',
            sharpe=FilterRange.from_str('[1.58, inf)'),
            fitness=FilterRange.from_str('[1, inf)'),
            turnover=FilterRange.from_str('(-inf, 0.7]'),
            order='-dateCreated',
            limit=limit,
            log='filter_alphas_limited'
        )
        alphas = resp.json()['results']
        print(f"共找到{len(alphas)}个Alpha")
        if len(alphas) == 0:
            return

        batch_num = 0
        for i in range(0,len(alphas),20):
            batch_num += 1
            list = alphas[i:i+20]
            print(f"正在检查第{batch_num}批{len(list)} 个Alpha...")
            favorable_data = []
            for alpha in list:
                if self.is_favorable(alpha['id']):
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
            
