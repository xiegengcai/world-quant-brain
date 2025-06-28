# -*- coding: utf-8 -*-
from datetime import datetime

from checker import Checker
import factory
from synchronizer import Synchronizer
import wqb

import dataset_config
from simulator import Simulator
from generator import Generator
from exports import ExportFiles
import utils
from submitter import Submitter

def main():

    try:
        print("🚀 启动 WorldQuant Brain 程序")

        credentials = str(input("\n请输入认证文件路径(默认: ~/.brain_credentials.txt)"))
        
        if credentials == "":
            credentials = "~/.brain_credentials.txt"

        wqbs= wqb.WQBSession((utils.load_credentials(credentials)), logger=wqb.wqb_logger(name='wqb_' + datetime.now().strftime('%Y%m%d')))

        print("\n📋 请选择运行模式:")
        print("1: 生成Alpha")
        print("2: 模拟回测")
        print("3: 同步指标")
        print("4: 自相关性检查")
        print("5: 自动提交")
        print("6: 生成数据集文件")
        print("7: 导出已提交的Alpha")

        mode = int(input("\n请选择模式 (1-5): "))
        if mode not in [1, 2, 3, 4,5,6,7]:
            print("❌ 无效的模式选择")
            return

        if mode == 1:
            generator = Generator(wqbs)
            print(f"\n📋 请选择生成类别：")
            print("1: 一阶")
            print("2: 二阶")
            print("3: 三阶")
            print("4: 来自文件")
            gen_mode = int(input("\n请选生成类别 (1-4): "))
            if gen_mode not in [1, 2, 3, 4]:
                print("❌ 无效的生成类别")
                return
            if gen_mode == 1:
                print("\n📊 可用数据集列表:")
                for dataset in dataset_config.get_dataset_list():
                    print(dataset)
                dataset_index = int(input("\n请选择数据集编号: "))
                dataset_id = dataset_config.get_dataset_by_index(dataset_index)
                if not dataset_id:
                    print("❌ 无效的数据集编号")
                    return
                generator.generate_first(dataset_id)
            if gen_mode == 2:
                generator.generate_second(factory.group_ops)
            if gen_mode == 3:
                generator.generate_third(factory.third_op)
            if gen_mode == 4:
                file_path = str(input("\n请输入文件路径: "))
                fields = []
                with open(file_path, "r") as f:
                    for line in f.readlines():
                        fields.append(line.strip())
                generator.generate_first_with_fields(fields)

        elif mode == 2:
            concurrency = int(input("\n📋 请输入回测并发数: "))
            simulator = Simulator(wqbs, concurrency)
            simulator.simulate()
        elif mode == 3:
            Synchronizer(wqbs).run()
        elif mode == 4:
            print(f"\n📋 请选择检查模式：")
            print("1: 本地检查")
            print("2: 服务器检查")
            check_mode = int(input("\n请选择检查模式 (1-2): "))
            if check_mode not in [1, 2]:
                print("❌ 无效的检查模式")
                return
            checker = Checker(wqbs)
            checker.check(check_mode)
        elif mode == 5:

            today = datetime.strftime(datetime.now(), "%Y-%m-%d")
            sharpe = float(input("\n请输入Sharpe阈值: "), 1.25)
            fitness = float(input("\n请输入Fitness阈值: "), 1.0)
            self_corr = float(input("\n请输入SelfCorr阈值: "), 0.6)
            begen_date = input("\n请输入开始日期(YYYY-MM-DD): ")
            if begen_date == "":
                begen_date = today
            end_date = input("\n请输入结束日期(YYYY-MM-DD): ")
            if end_date == "":
                end_date = today
            Submitter(
                wqbs, 
                begin_time=f"{begen_date}T00:00:00-05:00",
                end_time=f"{end_date}T23:59:59-05:00"
            ).submit({"sharpe": sharpe, "fitness": fitness, "self_corr": self_corr})
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
                export.export_submitted_alphas()
            else:
                export.generate()

    except Exception as e:
        print(f"❌ 程序运行出错: {str(e)}")

if __name__ == '__main__':
    main()