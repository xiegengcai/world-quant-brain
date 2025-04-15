# -*- coding: utf-8 -*-

import json
import os

import glob

import machine_lib as ml


class AlphaSimulator:
    def __init__(self,  brain: ml.WorldQuantBrain,simulated_alphas_file:str, available_path:str):
        self.brain = brain
        self.available_path = available_path
        self.simulated_alphas_file = simulated_alphas_file
    
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
        alpha_list = [alpha for alpha in alpha_list if self.brain.hash(alpha) not in processed_alpha_ids]
        # 根据 hash 值过滤掉重复的 Alpha 
        alpha_list = list({self.brain.hash(alpha): alpha for alpha in alpha_list}.values())

        print(f"共有 {len(alpha_list)} 个未处理的 Alpha 表达式需要模拟。")

        # 如果没有需要处理的 Alpha，直接退出
        if not alpha_list:
            print("所有 Alpha 都已处理完毕，无需继续运行。")
        else:
            self.brain.simulate_alphas(alpha_list)