# -*- coding: utf-8 -*-
import json
from os.path import expanduser

import wqb

import dataset_config
import simulator
import AlphaMachine as machine
import ExportFiles as export
import FavoriteAlphas as favorite
import utils as utils
from AutoSubmit import AutoSubmit

def run_simulator(wqbs:wqb.WQBSession):
    simulated_alphas_file = str(input("\n请输入已回测文件路径(默认: ./results/alpha_ids.txt)"))
    if simulated_alphas_file == "":
        # available_path = "./available_alphas"
        simulated_alphas_file = "./results/alpha_ids.txt"
        
    print(f"已回测文件路径: {simulated_alphas_file}")
    print("\n📋 请选择身份:")
    print("1: 顾问身份(并发8)")
    print("2: 非顾问身份")
    consultant_mode = int(input("\n请选择身份 (1-2): "))
    if consultant_mode not in [1, 2]:
        print("❌ 无效的身份选择")
        return
    if consultant_mode == 1:
        is_consultant = True
    else:
        is_consultant = False

    batch_size = int(input("\n请输入每批次数据大小 (1-100): "))
    if batch_size <= 0 or batch_size > 100:
        print("❌ 无效的每批次数据大小")
        return
    print(f"已回测文件路径: {simulated_alphas_file}, 顾问:{is_consultant}, 每批次数据大小: {batch_size}")
    
    _simulator = simulator.Simulator(wqbs, simulated_alphas_file, is_consultant, batch_size)

    print("\n📋 请选择回测模式:")
    print("1: 自动回测(按模板生成Alpha并回测)")
    print("2: 手动回测(从目录中读取JSON并回测)")
    simulated_mode = int(input("\n请选择模式 (1-2): "))
    
    if simulated_mode not in [1, 2]:
        print("❌ 无效的模式选择")
    if simulated_mode == 1:
        print("\n📊 可用数据集列表:")
        for dataset in dataset_config.get_dataset_list():
            print(dataset)

        dataset_index = input("\n请选择数据集编号: ")
        dataset_id = dataset_config.get_dataset_by_index(dataset_index)
        if not dataset_id:
            print("❌ 无效的数据集编号")
            return
        
        machine.AlphaMachine(simulator=_simulator,wqbs=wqbs, dataset_id=dataset_id).run()

    else:
        available_path = str(input("\n请输入可回测 Alpha 文件路径(默认: ./available_alphas): "))
        if available_path == "":
            available_path = "./available_alphas"
        _simulator.simulate_with_available(available_path)

def main():
    
    try:
        print("🚀 启动 WorldQuant Brain 程序")

        credentials = str(input("\n请输入认证文件路径(默认: ~/.brain_credentials.txt)"))
        
        if credentials == "":
            credentials = "~/.brain_credentials.txt"

        wqbs= wqb.WQBSession((utils.load_credentials(credentials)), logger=wqb.wqb_logger())

        print("\n📋 请选择运行模式:")
        print("1: 模拟回测")
        print("2: 剪枝提升质量")
        print("3: 自动提交")
        print("4: 生成数据集文件")
        print("5: 收藏Alpha")
        print("6: 导出已提交的Alpha")

        mode = int(input("\n请选择模式 (1-6): "))
        if mode not in [1, 2, 3, 4, 5, 6]:
            print("❌ 无效的模式选择")
            return

        if mode == 1:
            run_simulator(wqbs=wqbs)
        elif mode == 2:
            print("开发中...")
            return
        elif mode == 3:
            print("\n📋 请选择提交模式:")
            print("1: 直接提交")
            print("2: 检查排名后提交")
            submit_mode = int(input("\n请选择提交模式 (1-2): "))
            if submit_mode not in [1, 2]:
                print("❌ 无效的提交模式选择")
                return
            improve = 10
            checkRank = submit_mode == 2
            if checkRank:
                improve_str = input("\n请输入提升名次(默认: 10):")
                if improve_str != '':
                    improve = int(improve_str)

            submit_num_str = input("\n请输入提交Alpha数量(默认: 2):") 
            submit_num = 2
            improve = 10
            if submit_num_str != '':
                submit_num = int(submit_num_str)

            AutoSubmit(wqbs=wqbs, submit_num=submit_num, checkRank=checkRank, improve=improve).run()
        else:
            
            if mode == 5:
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
                if mode == 4:
                    _export.generate()
                else:
                    _export.export_submitted_alphas()
                

    except Exception as e:
        print(f"❌ 程序运行出错: {str(e)}")

if __name__ == '__main__':
    main()