# -*- coding: utf-8 -*-

import machine_lib as ml
import AlphaSimulator as simulator
import ExportFiles as export
import FavoriteAlphas as favorite

def main():
    
    try:
        print("🚀 启动 WorldQuant Brain 程序")

        credentials = str(input("\n请输入认证文件路径(默认: ~/.brain_credentials.txt)"))
        if credentials == "":
            credentials = "~/.brain_credentials.txt"

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

            _inupts = str(input("\n请输入可模拟 Alpha 文件路径(默认: ./available_alphas)、已模拟 Alpha 文件路径（默认: ./results/alpha_ids.txt）、最大并发数(默认: 3)"))
            if _inupts == "":
                available_path = "./available_alphas"
                simulated_alphas_file = "./results/alpha_ids.txt"
                max_workers = 3
                print(f"使用默认参数: {available_path} {simulated_alphas_file} {max_workers}")
            else:
                _inupt_arg = _inupts.split(" ")
                if len(_inupt_arg) == 3:
                    available_path, simulated_alphas_file, max_workers = _inupt_arg
                    print(f"没输入任何参数，使用默认值: {available_path} {simulated_alphas_file} {max_workers}")
                else:
                    if len(_inupt_arg) == 2:
                        available_path, simulated_alphas_file = _inupt_arg
                        max_workers = 3
                        print(f"输入两个参数, 并发数默认: {available_path} {simulated_alphas_file} {max_workers}")
                    else:
                        available_path = _inupts
                        simulated_alphas_file = "./results/alpha_ids.txt"
                        print(f"输入一个参数,已模拟及并发数默认: {available_path} {simulated_alphas_file} {max_workers}")
            
            _simulator = simulator.AlphaSimulator(
                ml.WorldQuantBrain(
                    credentials_file=credentials
                    , simulated_alphas_file=simulated_alphas_file
                    , max_workers=max_workers
                )
                , simulated_alphas_file=simulated_alphas_file
                , available_path=available_path
            )
            # 模拟 Alpha
            _simulator.simulate_alphas()
        elif mode == 3:
            # 收藏Alpha
            favorite.FavoriteAlphas(brain=brain).add_favorite()
        else:
            brain = ml.WorldQuantBrain(
                credentials_file=credentials
            )
            # 生成数据集文件
            out_put_path = str(input("\n请输入保存文件路径(默认: ./datasetFile): "))
            if out_put_path == "":
                out_put_path = "./datasetFile"
            _export = export.ExportFiles(
                brain=brain
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