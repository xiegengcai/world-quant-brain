# -*- coding: utf-8 -*-
import machine_lib as ml

class FavoriteAlphas:
    def __init__(self, brain: ml.WorldQuantBrain):
        self.brain = brain
        
    
    def add_favorite(self,alpha_num:int):
        alphas = self.brain.get_alphas(alpha_num=alpha_num,sharpe_th=1.25, fitness_th=1.0,is_favorite=False)
        if len(alphas) == 0:
            return

        batch_num = 1
        for i in range(0,len(alphas),20):
            list = alphas[i:i+20]
            print(f"正在检查第{batch_num}批{len(list)} 个Alpha...")
            batch_num += 1
            favorable_data = []
            for alpha in list:
                if self.brain.is_favorable(alpha['id']):
                    favorable_data.append({
                        'id':alpha['id']
                        ,'favorite':True
                    })
                
            if len(favorable_data) == 0:
                print(f"第{batch_num}批{len(list)} 个Alpha没有可以收藏的...")
                continue
            print(f"正在添加第第{batch_num}批{len(favorable_data)} 个alpha到收藏夹...")
            try:
                self.brain.batch_update(favorable_data)
            except Exception as e:
                print(f"添加收藏夹失败: {str(e)}")
                continue
