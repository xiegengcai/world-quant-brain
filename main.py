# -*- coding: utf-8 -*-
import json
from os.path import expanduser

import wqb

import AlphaSimulator as simulator
import ExportFiles as export
import FavoriteAlphas as favorite

def _load_credentials(credentials_file: str):
    """从文件加载凭据"""
    try:
        with open(expanduser(credentials_file)) as f:
            credentials = json.load(f)
        return credentials[0], credentials[1]
    except Exception as e:
        print(f"Failed to load credentials: {str(e)}")
        raise
def main():
    
    try:
        print("🚀 启动 WorldQuant Brain 程序")

        credentials = str(input("\n请输入认证文件路径(默认: ~/.brain_credentials.txt)"))
        
        if credentials == "":
            credentials = "~/.brain_credentials.txt"

        wqbs= wqb.WQBSession((_load_credentials(credentials)), logger=wqb.wqb_logger())

        print("\n📋 请选择运行模式:")
        print("1: 模拟回测(模拟给定的Alpha文件)")
        print("2: 生成数据集文件")
        print("3: 收藏Alpha")
        print("4: 导出已提交的Alpha")

        mode = int(input("\n请选择模式 (1-4): "))
        if mode not in [1, 2, 3, 4]:
            print("❌ 无效的模式选择")
            return

        if mode == 1:

            _inupts = str(input("\n请输入可模拟 Alpha 文件路径(默认: ./available_alphas)、已模拟 Alpha 文件路径（默认: ./results/alpha_ids.txt）、并发数(默认: 3)"))
            if _inupts == "":
                available_path = "./available_alphas"
                simulated_alphas_file = "./results/alpha_ids.txt"
                concurrency  = 3
                print(f"使用默认参数: {available_path} {simulated_alphas_file} {concurrency}")
            else:
                _inupt_arg = _inupts.split(" ")
                if len(_inupt_arg) == 3:
                    available_path, simulated_alphas_file, concurrency = _inupt_arg
                    print(f"没输入任何参数，使用默认值: {available_path} {simulated_alphas_file} {concurrency }")
                else:
                    if len(_inupt_arg) == 2:
                        available_path, simulated_alphas_file = _inupt_arg
                        concurrency = 3
                        print(f"输入两个参数, 并发数默认: {available_path} {simulated_alphas_file} {concurrency}")
                    else:
                        available_path = _inupts
                        simulated_alphas_file = "./results/alpha_ids.txt"
                        concurrency = 3
                        print(f"输入一个参数,已模拟及并发数默认: {available_path} {simulated_alphas_file} {concurrency}")
            simulator.AlphaSimulator(
                wqbs=wqbs
                , simulated_alphas_file=simulated_alphas_file
                , available_path=available_path
                , concurrency=int(concurrency)
            ).simulate_alphas()
        else:
            
            if mode == 3:
                alpha_num_str = input("\n请输入最大收藏Alpha数量(默认: 200):")
                # 收藏Alpha
                alpha_num = 200
                if alpha_num_str != '':
                    alpha_num = int(alpha_num_str)
                    
                favorite.FavoriteAlphas(wqbs=wqbs).add_favorite(alpha_num)
            else:
                
                # 生成数据集文件
                out_put_path = str(input("\n请输入保存文件路径(默认: ./datasetFile): "))
                if out_put_path == "":
                    out_put_path = "./datasetFile"
                _export = export.ExportFiles(
                    wqbs=wqbs
                    , out_put_path=out_put_path
                )
                if mode == 2:
                    _export.generate()
                else:
                    _export.export_submitted_alphas()
                

    except Exception as e:
        print(f"❌ 程序运行出错: {str(e)}")

if __name__ == '__main__':
    main()