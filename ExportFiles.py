# 登录
import argparse

import glob
import json
import os
from os.path import expanduser
from pathlib import Path
import time

import machine_lib as ml

class ExportFiles:

    def __init__(self, brain: ml.WorldQuantBrain, out_put_path: str):
        self.out_put_path = out_put_path
        self.brain = brain

    def generate_datasets_file(self):

        print(f"正在生成数据集文件...")
        datasets_info = self.brain.data_sets()
    

        for item_id, item_data in datasets_info.items():
            id = item_id
            name = item_data.get('name', 'N/A')
            description = item_data.get('description', 'N/A')

            # 检查是否存在 category，存在才打印
            Category = ''
            if 'category' in item_data:
                category = item_data['category']
                # 进一步检查 subcategory 是否有 name 字段
                if isinstance(category, dict) and 'name' in category:
                    print(f"Category: {category['name']}")
                elif isinstance(category, str):
                    Category = category

            Subcategory=''
            # 检查是否存在 subcategory，存在才打印
            if 'subcategory' in item_data:
                subcategory = item_data['subcategory']
                # 进一步检查 subcategory 是否有 name 字段
                if isinstance(subcategory, dict) and 'name' in subcategory:
                    print(f"Subcategory: {subcategory['name']}")
                elif isinstance(subcategory, str):
                    Subcategory = subcategory
            cs = f"{Category}/{Subcategory}"


            searchScope = {'region': 'USA', 'delay': '1', 'universe': 'TOP3000', 'instrumentType': 'EQUITY'}
            data_fields = self.brain.data_fields(searchScope=searchScope, dataset_id=id)
            stats_table = "| Region | Delay | Universe |\n"
            stats_table += "|----|-------------|------|\n"
            stats_table += f"| {searchScope['region']} | {searchScope['delay']} | {searchScope['universe']} |\n"

            datafields_id = []
            datafields_description = []
            datafields_type = []
            datafields_list_flat = [item for sublist in data_fields for item in sublist]
            for i in range(len(datafields_list_flat)):  # 0 到 100
                datafields_id.append(datafields_list_flat[i]['id'])
                datafields_description.append(datafields_list_flat[i]['description'])
                datafields_type.append(datafields_list_flat[i]['type'])
            # Create Markdown table
            markdown_table = "| id | description | type |\n"
            markdown_table += "|----|-------------|------|\n"
            for i in range(len(datafields_id)):
                id = datafields_id[i]
                description = datafields_description[i]
                type = datafields_type[i]
                markdown_table += f"| {id} | {description} | {type} |\n"

            filename = f"{self.out_put_path}/{name}.md"
            with open(filename, "w", encoding="utf-8") as f:
                print(f"**Category:{cs}**\n", file=f)  # 写入文件而不是控制台
                print(f"**Dataset ID:{id}**\n\n", file=f)  # 写入文件而不是控制台
                print(f"\n\n**{description}**\n\n", file=f)  # 写入文件而不是控制台
                print(stats_table + "\n\n", file=f)
                print(markdown_table, file=f)
        
        print(f"共生成 {len(datasets_info.items())} 个数据集文件...")

    def generate_operators_file(self):
        print(f"正在生成 Operators.md 文件...")
        operators = self.brain.get_operators()
        
        with open(f'{self.out_put_path}/Operators.md', 'w', encoding="utf-8") as f:
            f.write('|Name|Category|Scope|Definition|Description|Level|\n')
            f.write('|:---|:-------|:----|:---------|:----------|:----|\n')
            for operator in operators:
                f.write(f'|{operator["name"]}|{operator["category"]}|{operator["scope"]}|{operator["definition"]}|{operator["description"]}|{operator["level"]}|\n')

            print(f"Operators.md 文件生成 完成...")

    def generate_alphas_file(self):
        print(f"正在生成 SubmittedAlphas.md 文件...")
        alphas = self.brain.get_alphas(status="ACTIVE")
        submitted_path = f'{self.out_put_path}/SubmittedAlphas.md'
        with open(submitted_path, 'w', encoding="utf-8") as f:
            for alpha in alphas:
                f.write(f'```python\n{alpha["regular"]['code']}\n```\n')
        print(f"SubmittedAlphas.md 文件生成 完成...")

    def export_submitted_alphas(self):
        print(f"正在导出已提交的Alpha...")
        json_files = glob.glob(f'{self.out_put_path}/SubmittedAlphas-*.json')
        for file in json_files:
            os.remove(file)

        list = self.brain.get_alphas(status="ACTIVE")
        if len(list) == 0:
            print("没有Alpha可以导出...")
            return
        
        simulation_data_list = []
        
        for alpha in list:
            simulation_data_list.append({
                'type':'REGULAR'
                ,'regular':alpha['regular']['code']
                ,'settings':alpha['settings']
            })
        
        file_name = self.out_put_path + '/SubmittedAlphas-' + time.strftime("%Y%m%d%H%M%S") + '.json'
        with open(file_name, 'w', encoding="utf-8") as f:
            f.write(json.dumps(simulation_data_list))
        print(f"Alpha导出完成...")

    def generate(self):
        json_files = glob.glob(f'{self.out_put_path}/*.md')
        for file in json_files:
            os.remove(file)

        self.generate_datasets_file()
        self.generate_operators_file()
        self.generate_alphas_file()


        



        

    

