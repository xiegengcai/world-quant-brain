# -*- coding: utf-8 -*-

import json
import os

import glob

import asyncio
import wqb

import utils


class Simulator:
    def __init__(self,  wqbs: wqb.WQBSession, simulated_alphas_file:str, is_consultant:bool=True, batch_size:int=15):
        self.wqbs = wqbs
        self.simulated_alphas_file = simulated_alphas_file
        self.is_consultant = is_consultant
        self.concurrency = 10
        self.batch_size = batch_size
        if not self.is_consultant:
            self.concurrency = 3
        # 确保目录存在
        parent_path = os.path.dirname(os.path.abspath(self.simulated_alphas_file))
        os.makedirs(parent_path, exist_ok=True)
    
    def simulate_with_available(self, available_path:str):
        """从 available_alphas 目录中读取 Alpha 并回测"""
        
        # 获取 available_alphas 目录下所有的 .json 文件
        alpha_files = glob.glob(os.path.join(available_path, '*.json'))
        alpha_list = []

        # 读取每个 .json 文件并加载到 alpha_list 中
        for file in alpha_files:
            with open(file, 'r') as f:
                alpha_list.extend(json.load(f))  # 假设每个文件包含一个 Alpha 列表
        
        self.simulate_alphas(alpha_list)

    def pre_consultant_simulate(self, alpha_list: list):
        """非顾问身份回测"""
        # 以10个大小分割成多个数组，每个子数组以{concurrency}个并发回测
        partitions = [alpha_list[i:i + self.batch_size] for i in range(0,len(alpha_list), self.batch_size)]
        batch_num = 1
        total_batch = len(partitions)
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
            'Referer':'https://platform.worldquantbrain.com/'
        }
        print(f'分{total_batch}批次，每批次{self.batch_size}个{self.concurrency}线程并发回测...')
        for list in partitions:
            resps = asyncio.run(
                self.wqbs.concurrent_simulate(
                    list,
                    self.concurrency,
                    return_exceptions=True,
                    on_nolocation=lambda vars: print(vars['target'], vars['resp'], sep='\n'),
                    on_start=lambda vars: print(vars['url']),
                    on_finish=lambda vars: print(vars['resp']),
                    # on_success=lambda vars: print(vars['resp']),
                    # on_failure=lambda vars: print(vars['resp']),
                    headers=headers,
                    log=f'{self.__class__}#pre_consultant_simulate'
                )
            )
            lines = []
            for idx, resp in enumerate(resps, start=0):
                try:
                    if not resp.ok: # 如果回测失败
                        continue
                    lines.append(f'{utils.hash(alpha_list[idx])}\n')
                except Exception as e:
                    print(f'回测 {alpha_list[idx]} 失败: {e}')
            
            # 将已处理的 Alpha ID 写入文件中
            utils.save_lines_to_file(self.simulated_alphas_file, lines)
            print(f'批次 {batch_num}/{total_batch} ✅成功：{len(lines)} 个，❌失败：{len(list)-len(lines)} 个...')
            batch_num += 1

    def consultant_simulate(self, alpha_list: list):
        """顾问身份回测"""
        multi_alphas = wqb.to_multi_alphas(alpha_list, self.batch_size)
        resps = asyncio.run(
            self.wqbs.concurrent_simulate(
                multi_alphas,
                self.concurrency,
                return_exceptions=True,
                on_nolocation=lambda vars: print(vars['target'], vars['resp'], sep='\n'),
                on_start=lambda vars: print(vars['url']),
                on_finish=lambda vars: print(vars['resp']),
                # on_success=lambda vars: print(vars['resp']),
                # on_failure=lambda vars: print(vars['resp']),
                log=f'{self.__class__}#consultant_simulate'
            )
        )
        success_num = 0
        for idx, resp in enumerate(resps, start=0):
            if not resp.ok: # 如果回测失败
                continue
            lines = []
            for alpha in multi_alphas[idx]:
                success_num += 1
                lines.append(f'{utils.hash(alpha_list[idx])}\n')
            utils.save_lines_to_file(self.simulated_alphas_file, lines)
        print(f'共{len(alpha_list)}个，✅成功{success_num} 个，❌失败：{len(alpha_list)-success_num} 个...')

            
    def simulate_alphas(self, alpha_list: list):
        """回测 Alpha 列表中的所有 Alpha"""
        # 检查是否有已处理的 Alpha ID 文件
        processed_alpha_ids = set()
    
        if os.path.exists(self.simulated_alphas_file,):
            with open(self.simulated_alphas_file, 'r') as f:
                processed_alpha_ids = set(line.strip() for line in f)

        print(f"已处理的 Alpha 表达式数量：{len(processed_alpha_ids)}")
        print(f"开始和已处理的 Alpha 表达式进行比对...")
        # 过滤掉已处理的 Alpha
        alpha_list = [alpha for alpha in alpha_list if utils.hash(alpha) not in processed_alpha_ids]
        # 根据 hash 值过滤掉重复的 Alpha 
        alpha_list = list({utils.hash(alpha): alpha for alpha in alpha_list}.values())

        # 如果没有需要处理的 Alpha，直接退出
        if not alpha_list:
            print("所有 Alpha 都已处理完毕，无需继续运行。")
            return

        print(f"共有 {len(alpha_list)} 个未处理的 Alpha 表达式需要回测。")
        print("开始回测...")
        if self.is_consultant:
            self.consultant_simulate(alpha_list)
        else:
            self.pre_consultant_simulate(alpha_list)
        print("回测完毕...")
        