# -*- coding: utf-8 -*-
import machine_lib as ml

class FavoriteAlphas:
    def __init__(self, brain: ml.WorldQuantBrain):
        self.brain = brain
        
    
    def add_favorite(self):
        while True:
            alphas = self.brain.get_alphas(alpha_num=20,sharpe_th=1.25, fitness_th=1.0,is_favorite=False)
            if len(alphas) == 0:
                break

            favorable_data = []
            for alpha in alphas:
                if self.brain.is_favorable(alpha['id']):
                    favorable_data.append({
                        'id':alpha['id']
                        ,'favorite':True
                    })
                
            if len(favorable_data) == 0:
                print("没有alpha可以添加到收藏夹...")
                return
            print(f"正在添加 {len(favorable_data)} 个alpha到收藏夹...")
            try:
                self.brain.batch_update(favorable_data)
            except Exception as e:
                print(f"添加收藏夹失败: {str(e)}")
    
    
    