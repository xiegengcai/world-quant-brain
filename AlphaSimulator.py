# -*- coding: utf-8 -*-

import hashlib
import json
import os

import glob

import asyncio
import wqb


class AlphaSimulator:
    def __init__(self,  wqbs: wqb.WQBSession, simulated_alphas_file:str, available_path:str, concurrency:int=3):
        self.wqbs = wqbs
        self.available_path = available_path
        self.simulated_alphas_file = simulated_alphas_file
        self.concurrency = concurrency
        # 确保目录存在
        parent_path = os.path.dirname(os.path.abspath(self.simulated_alphas_file))
        os.makedirs(parent_path, exist_ok=True)

    def hash(self, alpha):
        """生成稳定的哈希值"""
        alpha_string = f"{alpha['regular']}{json.dumps(alpha['settings'], sort_keys=True)}"
        return hashlib.md5(alpha_string.encode('utf-8')).hexdigest()
    
    def _save_alpha(self, alpha_id):
        """保存 Alpha ID 到文件"""
        print(f"正在保存 Alpha ID: {alpha_id}")
        # 单独保存 ID 便于后续操作
        # hash_str = self.hash(alpha_id)
        with open(self.simulated_alphas_file, 'a') as f:
            f.write(f"{alpha_id}\n")
            
        print(f"✅ Alpha ID {alpha_id} 已保存")
    
    def simulate_alphas(self):
        
        # 获取 available_alphas 目录下所有的 .json 文件
        alpha_files = glob.glob(os.path.join(self.available_path, '*.json'))
        alpha_list = []


        # 读取每个 .json 文件并加载到 alpha_list 中
        for file in alpha_files:
            with open(file, 'r') as f:
                alpha_list.extend(json.load(f))  # 假设每个文件包含一个 Alpha 列表

        # 检查是否有已处理的 Alpha ID 文件
        processed_alpha_ids = set()
    
        if os.path.exists(self.simulated_alphas_file,):
            with open(self.simulated_alphas_file, 'r') as f:
                processed_alpha_ids = set(line.strip() for line in f)



        # 过滤掉已处理的 Alpha
        alpha_list = [alpha for alpha in alpha_list if self.hash(alpha) not in processed_alpha_ids]
        # 根据 hash 值过滤掉重复的 Alpha 
        alpha_list = list({self.hash(alpha): alpha for alpha in alpha_list}.values())

        # 如果没有需要处理的 Alpha，直接退出
        if not alpha_list:
            print("所有 Alpha 都已处理完毕，无需继续运行。")
            return

        print(f"共有 {len(alpha_list)} 个未处理的 Alpha 表达式需要模拟。")
        resps = asyncio.run(
            self.wqbs.concurrent_simulate(
                alpha_list,  # `alphas` or `multi_alphas`
                self.concurrency,
                return_exceptions=True,
                on_nolocation=lambda vars: print(vars['target'], vars['resp'], sep='\n'),
                on_start=lambda vars: print(vars['url']),
                on_finish=lambda vars: print(vars['resp']),
                on_success=lambda vars: print(vars['resp']),
                on_failure=lambda vars: print(vars['resp']),
            )
        )
        
        for idx, resp in enumerate(resps, start=0):
            if resp.ok:
                self._save_alpha(self.hash(alpha_list[idx]))