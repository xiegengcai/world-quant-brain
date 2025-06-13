# -*- coding: utf-8 -*-
from datetime import datetime

import wqb

import dataset_config
from simulator import Simulator
from generator import Generator
from exports import ExportFiles
from favorite import FavoriteAlpha
import utils
from submitter import Submitter
from improvement import Improvement

def run_simulator(wqbs:wqb.WQBSession, simulator:Simulator):

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
        alpha_list = Generator(wqbs, dataset_id).generate()
        simulator.simulate_alphas(alpha_list)

    else:
        available_path = str(input("\n请输入可回测 Alpha 文件路径(默认: ./available_alphas): "))
        if available_path == "":
            available_path = "./available_alphas"
        simulator.simulate_with_available(available_path)

def improve_or_simulate(wqbs:wqb.WQBSession, mode:int):
    simulated_alphas_file = str(input("\n请输入已回测文件路径(默认: ./results/alpha_ids.csv)"))
    if simulated_alphas_file == "":
        # available_path = "./available_alphas"
        simulated_alphas_file = "./results/alpha_ids.csv"
        
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
    
    batch_size = 30

    batch_size_str = input("\n请输入每批次数据大小 (1-100, 默认: 30): ")
    if batch_size_str != '':
        batch_size = int(batch_size_str)
        if batch_size < 1 or batch_size > 100:
            print("❌ 无效的每批次数据大小")
            return
    print(f"已回测文件路径: {simulated_alphas_file}, 顾问:{is_consultant}, 每批次数据大小: {batch_size}")
    
    simulator = Simulator(wqbs, simulated_alphas_file, is_consultant, batch_size)
    if mode == 1:
        run_simulator(wqbs, simulator)
    else:
        print("\n📊 可用数据集列表:")
        for dataset in dataset_config.get_dataset_list():
            print(dataset)

        dataset_index = input("\n请选择数据集编号: ")
        dataset_id = dataset_config.get_dataset_by_index(dataset_index)
        if not dataset_id:
            print("❌ 无效的数据集编号")
            return
        today = datetime.strftime(datetime.now(), "%Y-%m-%d")
        begen_date = input("\n请输入开始日期(YYYY-MM-DD): ")
        if begen_date == "":
            begen_date = today
        end_date = input("\n请输入结束日期(YYYY-MM-DD): ")
        if end_date == "":
            end_date = today
        limit_str = input("\n请输入数据量(默认: 100): ")
        limit = 100
        if limit_str != '':
            limit = int(limit_str)

        improvement = Improvement(
            wqbs
            , dataset_id=dataset_id
            , begin_time=datetime.fromisoformat(f'{begen_date}T00:00:00-05:00')
            , end_time=datetime.fromisoformat(f'{end_date}T23:59:59-05:00')
            ,limit=limit
        )
        list = improvement.first_improve()
        if len(list) == 0:
            print("❌ 无可提升Alpha")
            return
        begin_time = datetime.now()
        simulator.simulate_alphas(list)
        end_time = datetime.now()
        seconds = (end_time - begin_time).seconds
        print(f"第一阶段提升耗时: {seconds}")
        # 12小时时差
        list = improvement.second_improve()
        simulator.simulate_alphas(list)
        seconds = (datetime.now() - end_time).seconds
        print(f"第二阶段提升耗时: {seconds}")



def main():

    try:
        print("🚀 启动 WorldQuant Brain 程序")

        credentials = str(input("\n请输入认证文件路径(默认: ~/.brain_credentials.txt)"))
        
        if credentials == "":
            credentials = "~/.brain_credentials.txt"

        wqbs= wqb.WQBSession((utils.load_credentials(credentials)), logger=wqb.wqb_logger(name='logs/wqb_' + datetime.now().strftime('%Y%m%d')))

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

        if mode == 1 or mode == 2:
            improve_or_simulate(wqbs, mode)
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
            if submit_num_str != '':
                submit_num = int(submit_num_str)

            today = datetime.strftime(datetime.now(), "%Y-%m-%d")

            begen_date = input("\n请输入开始日期(YYYY-MM-DD): ")
            if begen_date == "":
                begen_date = today
            end_date = input("\n请输入结束日期(YYYY-MM-DD): ")
            if end_date == "":
                end_date = today
            Submitter(
                wqbs, 
                begin_time=f"{begen_date}T00:00:00-05:00",
                end_time=f"{end_date}T23:59:59-05:00",
                submit_num=submit_num, 
                checkRank=checkRank, 
                improve=improve
            ).submit()
        else:
            
            if mode == 5:
                alpha_num_str = input("\n请输入最大收藏Alpha数量(默认: 200):")
                # 收藏Alpha
                alpha_num = 200
                if alpha_num_str != '':
                    alpha_num = int(alpha_num_str)
                begen_date = input("\n请输入开始日期(YYYY-MM-DD): ")
                today = datetime.strftime(datetime.now(), "%Y-%m-%d")

                if begen_date == "":
                    begen_date = today
                end_date = input("\n请输入结束日期(YYYY-MM-DD): ")
                if end_date == "":
                    end_date = today
                FavoriteAlpha(
                    wqbs
                    , begin_time=f"{begen_date}T00:00:00-05:00"
                    , end_time=f"{end_date}T23:59:59-05:00"
                ).add_favorite(alpha_num)
            else:
                
                # 生成数据集文件
                out_put_path = str(input("\n请输入保存文件路径(默认: ./datasetFile): "))
                if out_put_path == "":
                    out_put_path = "./datasetFile"
                export = ExportFiles(
                    wqbs
                    , out_put_path=out_put_path
                )
                if mode == 4:
                    export.generate()
                else:
                    export.export_submitted_alphas()
                

    except Exception as e:
        print(f"❌ 程序运行出错: {str(e)}")

if __name__ == '__main__':
    main()