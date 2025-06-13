import json
import pandas as pd
from itertools import combinations
from collections import defaultdict
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
from check_local import *

# -------------------- 常量配置 --------------------
CONFIG = {
    "CORRELATION_THRESHOLD": 0.7,
    "MIN_DATA_POINTS": 10,
    "TIME_WINDOW_YEARS": 4,
    "GROUP_SIZE": 1,
    "MAX_WORKERS": max(1, os.cpu_count() - 1),  # 动态设置核心数
    "CHUNK_SIZE": 500,
    "DEBUG_MODE": True  # 新增调试模式
}

# -------------------- 全局数据记录 --------------------
failed_cross = defaultdict(list)
failed_internal = defaultdict(list)
valid_groups = []


# -------------------- 核心功能函数 --------------------
def calculate_diff(pnl_data, years_filter=CONFIG["TIME_WINDOW_YEARS"]):
    """增强版时间序列处理，包含数据校验"""
    schema = pnl_data["schema"]

    try:
        df = pd.DataFrame(
            pnl_data["records"],
            columns=[prop["name"] for prop in schema["properties"]]
        )
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').set_index('date')

        pnl_series = df['pnl'].ffill()
        diff_series = pnl_series - pnl_series.shift(1)

        if diff_series.empty:
            if CONFIG["DEBUG_MODE"]:
                print(f"空序列警告: {pnl_data.get('id', '未知')}")
            return pd.Series()

        cutoff_date = diff_series.index.max() - pd.DateOffset(years=years_filter)
        filtered = diff_series[diff_series.index > cutoff_date].dropna()

        if CONFIG["DEBUG_MODE"] and len(filtered) < CONFIG["MIN_DATA_POINTS"]:
            print(f"数据不足警告: {len(filtered)} points after filtering")

        return filtered

    except Exception as e:
        print(f"数据处理错误: {str(e)}")
        return pd.Series()


def validate_candidate(candidate_id, candidate_data):
    """增强版候选验证，包含调试输出"""

    if candidate_data.empty:
        if CONFIG["DEBUG_MODE"]:
            print(f"候选 {candidate_id} 数据为空")
        return False
    pass_ids = []

    for submitted_id, submitted_data in submitted.items():
        common_dates = submitted_data.index.intersection(candidate_data.index)
        overlap = len(common_dates)

        if overlap < CONFIG["MIN_DATA_POINTS"]:
            if CONFIG["DEBUG_MODE"]:
                print(f"跳过 {submitted_id}-{candidate_id} (重叠天数: {overlap})")
            continue

        try:
            corr = submitted_data[common_dates].corr(candidate_data[common_dates])
            abs_corr = abs(corr)

            if abs_corr > CONFIG["CORRELATION_THRESHOLD"]:
                record = {
                    "compared_with": submitted_id,
                    "correlation": round(corr, 4),
                    "overlap_days": overlap,
                    "dates": [str(d) for d in common_dates[:5]]  # 记录部分日期
                }
                failed_cross[candidate_id].append(record)

                if CONFIG["DEBUG_MODE"]:
                    print(f"超标剔除 {candidate_id} vs {submitted_id}: {abs_corr:.4f}")
                return False
            pass_ids.append(round(corr, 4))

        except Exception as e:
            print(f"相关系数计算错误: {str(e)}")
            continue

    return {candidate_id: max(pass_ids)}


# -------------------- 组合分析函数 --------------------
def analyze_group(group, group_data):
    """增强版组合分析，包含详细日志"""
    analysis = {
        "members": sorted(group),
        "valid": True,
        "max_correlation": 0.0,
        "invalid_pairs": [],
        "checked_pairs": 0
    }

    for idx, ((id1, data1), (id2, data2)) in enumerate(
            combinations(zip(group, group_data), 2), 1
    ):
        if not analysis["valid"]:
            if CONFIG["DEBUG_MODE"]:
                print(f"组合 {group} 已失效，跳过剩余检查")
            break

        try:
            common_dates = data1.index.intersection(data2.index)
            overlap = len(common_dates)
            analysis["checked_pairs"] += 1

            if overlap < CONFIG["MIN_DATA_POINTS"]:
                if CONFIG["DEBUG_MODE"]:
                    print(f"跳过 {id1}-{id2} (重叠天数: {overlap})")
                continue

            corr = round(data1[common_dates].corr(data2[common_dates]), 4)
            abs_corr = abs(corr)
            analysis["max_correlation"] = max(analysis["max_correlation"], abs_corr)

            if CONFIG["DEBUG_MODE"]:
                print(f"检查 {id1} vs {id2}: {abs_corr:.4f} (重叠{overlap}天)")

            if abs_corr > CONFIG["CORRELATION_THRESHOLD"]:
                analysis["valid"] = False
                record = {
                    "pair": (id1, id2),
                    "correlation": corr,
                    "overlap_days": overlap,
                    "date_range": [
                        str(common_dates.min()),
                        str(common_dates.max())
                    ]
                }
                analysis["invalid_pairs"].append(record)

                if CONFIG["DEBUG_MODE"]:
                    print(f"超标配对 {id1}-{id2}: {abs_corr:.4f}")
                break

        except Exception as e:
            print(f"配对分析错误: {str(e)}")
            continue

    return analysis


# -------------------- 并行处理函数 --------------------
def parallel_analyze_groups(candidates):
    """增强版并行处理，包含进度监控"""
    total = sum(1 for _ in combinations(candidates, CONFIG["GROUP_SIZE"]))
    print(f"\n需要分析的总组合数: {total:,}")

    candidate_data = {k: v.copy() for k, v in candidates.items()}
    tasks = [
        (group, [candidate_data[name] for name in group])
        for group in combinations(candidates.keys(), CONFIG["GROUP_SIZE"])
    ]

    valid = []
    start_time = datetime.now()

    with ProcessPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as executor:
        futures = []

        # 分块提交任务
        for i in range(0, len(tasks), CONFIG["CHUNK_SIZE"]):
            chunk = tasks[i:i + CONFIG["CHUNK_SIZE"]]
            futures.append(executor.submit(process_chunk, chunk))

        # 处理结果
        for idx, future in enumerate(as_completed(futures), 1):
            chunk_valid, chunk_invalid = future.result()
            valid.extend(chunk_valid)

            # 合并违规记录
            for pair_key, violations in chunk_invalid.items():
                failed_internal[pair_key].extend(violations)

            # 进度报告
            elapsed = (datetime.now() - start_time).total_seconds()
            speed = len(valid) / elapsed if elapsed > 0 else 0
            print(
                f"进度: {idx}/{len(futures)} chunks | "
                f"有效: {len(valid):,} | "
                f"违规: {len(failed_internal):,} | "
                f"速度: {speed:.1f}组/秒"
            )

    return valid


def process_chunk(chunk):
    """安全的数据块处理"""
    chunk_valid = []
    chunk_invalid = defaultdict(list)

    for args in chunk:
        try:
            result = analyze_group(*args)
            if result["valid"]:
                chunk_valid.append(result)
            else:
                for pair_info in result["invalid_pairs"]:
                    pair = tuple(sorted(pair_info["pair"]))
                    chunk_invalid[pair].append({
                        "correlation": pair_info["correlation"],
                        "overlap_days": pair_info["overlap_days"],
                        "date_range": pair_info["date_range"]
                    })
        except Exception as e:
            print(f"处理组合时出错: {str(e)}")
            continue

    return chunk_valid, dict(chunk_invalid)


# -------------------- 结果处理函数 --------------------
def save_results(valid_groups):
    """增强版结果保存，包含统计信息"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 有效组合统计
    if valid_groups:
        max_corr = max(g["max_correlation"] for g in valid_groups)
        min_corr = min(g["max_correlation"] for g in valid_groups)
        avg_corr = sum(g["max_correlation"] for g in valid_groups) / len(valid_groups)
    else:
        max_corr = min_corr = avg_corr = 0.0

    # 保存有效组合
    valid_data = {
        "metadata": {
            "analysis_date": timestamp,
            "group_size": CONFIG["GROUP_SIZE"],
            "total_groups": len(valid_groups),
            "max_correlation": round(max_corr, 4),
            "min_correlation": round(min_corr, 4),
            "avg_correlation": round(avg_corr, 4),
            "threshold": CONFIG["CORRELATION_THRESHOLD"]
        },
        "groups": [
            {
                "members": g["members"],
                "max_correlation": g["max_correlation"],
                "checked_pairs": g["checked_pairs"]
            } for g in valid_groups
        ]
    }

    with open(f'valid_groups.json', 'w') as f:
        json.dump(valid_data, f, indent=2)
    # 字典 根据max_correlation的值  排序
    sorted_groups = sorted(valid_groups, key=lambda g: g["max_correlation"], reverse=False)
    print(f"打印出最低相关性的前20个组合")
    # for i in range(20):
    #     print(sorted_groups[i])
    # print(sorted_groups[:10])

    # 保存违规记录
    violation_data = {
        "cross": {
            k: v for k, v in failed_cross.items() if v
        },
        "internal": {
            f"{k[0]}-{k[1]}": v for k, v in failed_internal.items() if v
        }
    }

    with open(f'violations.json', 'w') as f:
        json.dump(violation_data, f, indent=2)


# -------------------- 主流程 --------------------
if __name__ == "__main__":
    CONFIG["CORRELATION_THRESHOLD"] = 0.7
    CONFIG["GROUP_SIZE"] = 1
    # fileneme = r'pnl_list_unsub_d1.json'
    # color = None

    # filename  = r'pnl_list_unsub_green.json'
    # color = 'GREEN'
    #
    filename = r'pnl_list_unsub_blue.json'
    # color = 'RED'

    # filename = r'pnl_list_unsub_purple.json'
    # color = 'PURPLE'

    # filename = r'pnl_list_unsub_yelow.json'
    # color = 'YELLOW'

    # filename ='pnl_data_20250520.json'
    # filename = r'pnl_list_unsub_red.json'
    # color = 'RED'
    # 'color': 'GREEN',  # 'RED', 'YELLOW', 'GREEN', 'BLUE', 'PURPLE'
    print("获取最新已提交数据")
    get_pnl_data_list('submitted', 'pnl_list.json')
    print("获取待提交数据")
    get_pnl_data_list('unsubmitted', filename, None)

    # 初始化环境
    pd.set_option('display.max_columns', None)
    start_time = datetime.now()

    try:
        # 数据加载
        print("{:=^50}".format(" 数据加载阶段 "))
        with open('pnl_list.json') as f:
            submitted = {k: calculate_diff(v) for k, v in json.load(f).items()}
            submitted = {k: v for k, v in submitted.items() if not v.empty}
            print(f"已加载提交曲线: {len(submitted)}条")

        with open(filename) as f:
            green = {k: calculate_diff(v) for k, v in json.load(f).items()}
            green = {k: v for k, v in green.items() if not v.empty}
            print(f"已加载待筛选曲线: {len(green)}条")

        # 第一阶段筛选
        print("\n{:=^50}".format(" 第一阶段筛选 "))
        stage1_results = {}
        pass_res = {}
        for cid, data in green.items():
            res = validate_candidate(cid, data)
            if res:
                stage1_results[cid] = data
                pass_res.update(res)
                print(f"✓ {cid}")
            else:
                print(f"✗ {cid} (超标次数: {len(failed_cross[cid])})")
        with open('pass_res.json', 'w') as f:
            json.dump(pass_res, f)
            # 根据相关性排序（修复部分）
            sorted_stage1_results = sorted(pass_res.items(), key=lambda x: x[1],
                                           reverse=False)

            # 打印前十个
            print("\nTop 10 最小相关系数:")
            for i, (cid, corr) in enumerate(sorted_stage1_results[:10], 1):
                print(f"{i}. {cid}: {corr:.4f}")
        # print(f"{sorted_stage1_results}")
        print(f"\n通过候选: {len(stage1_results)}/{len(green)}")
        print(f"耗时: {(datetime.now() - start_time).total_seconds():.1f}秒")
        # 第二阶段筛选
        if len(stage1_results) >= CONFIG["GROUP_SIZE"]:
            print("\n{:=^50}".format(" 组合分析阶段 "))
            valid_groups = parallel_analyze_groups(stage1_results)

            print("\n{:=^50}".format(" 最终统计 "))
            print(f"有效组合数: {len(valid_groups):,}")
            print(f"违规配对总数: {sum(len(v) for v in failed_internal.values()):,}")
            print(f"总耗时: {(datetime.now() - start_time).total_seconds():.1f}秒")

            save_results(valid_groups)

            print("\n结果已保存至JSON文件")

        else:
            print("\n候选数量不足，无法生成组合")

    except Exception as e:
        print(f"\n运行时错误: {str(e)}")
        raise

    finally:
        print("\n{:=^50}".format(" 分析完成 "))
