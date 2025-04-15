# -*- coding: utf-8 -*-
import datetime
import hashlib
import os
import threading
import time
import concurrent
from turtle import pd
import requests
import json

import tqdm

from session_manager import SessionManager

class WorldQuantBrain:
    def __init__(self, credentials_file:str, simulated_alphas_file:str="simulated_alphas.txt", max_workers:int=3):
        
        self.base_url = 'https://api.worldquantbrain.com'
        self.simulated_alphas_file = simulated_alphas_file
        self.max_workers = max_workers
    
        self.session = SessionManager(credentials_file).create_session()
        self.print_lock = threading.Lock()  # 用于保护打印的线程锁

    def hash(self, alpha):
        """生成稳定的哈希值"""
        alpha_string = f"{alpha['regular']}{json.dumps(alpha['settings'], sort_keys=True)}"
        return hashlib.md5(alpha_string.encode('utf-8')).hexdigest()
    
    def generate_sim_data(self, alpha_list, region='USA', delay=1, decay=0, universe='TOP3000', neut='INDUSTRY'):
        """生成 Alpha 模拟数据"""
        sim_data_list = []
        for alpha, decay in alpha_list:
            simulation_data = {
                'type': 'REGULAR',
                'settings': {
                    'instrumentType': 'EQUITY',
                    'region': region,
                    'universe': universe,
                    'delay': delay,
                    'decay': decay,
                    'neutralization': neut,
                    'truncation': 0.08,
                    'pasteurization': 'ON',
                    'unitHandling': 'VERIFY',
                    'nanHandling': 'OFF',
                    'language': 'FASTEXPR',
                    'visualization': False,
                },
                'regular': alpha}

            sim_data_list.append(simulation_data)
        return sim_data_list
    
    def get_operators(self):
        operators = self.session.get(f"{self.base_url}/operators")
        return operators.json()

    def locate_alpha(self, alpha_id):
        alpha = self.session.get(f"{self.base_url}/alphas/{alpha_id}")
        string = alpha.content.decode('utf-8')
        metrics = json.loads(string)
        #print(metrics["regular"]["code"])
        
        dateCreated = metrics["dateCreated"]
        sharpe = metrics["is"]["sharpe"]
        fitness = metrics["is"]["fitness"]
        turnover = metrics["is"]["turnover"]
        margin = metrics["is"]["margin"]
        
        triple = [sharpe, fitness, turnover, margin, dateCreated]
    
        return triple

    def set_alpha_properties(self,
        alpha_id,
        name: str = None,
        color: str = None,
        selection_desc: str = "None",
        combo_desc: str = "None",
        tags: str = ["ace_tag"],
        favorite: bool = False
    ):
        """
        Function changes alpha's description parameters
        """
    
        params = {
            "color": color,
            "name": name,
            "tags": tags,
            "category": None,
            "regular": {"description": None},
            "combo": {"description": combo_desc},
            "selection": {"description": selection_desc},
            "favorite": favorite
        }
        self.session.patch(f"{self.base_url}/alphas/{alpha_id}", json=params)

    def data_sets(self, 
        instrument_type: str = 'EQUITY',
        region: str = 'USA',
        delay: int = 1,
        universe: str = 'TOP3000'):
        """
        Get a list of data sets.
        """
        url = f"{self.base_url}/data-sets?instrumentType={instrument_type}&region={region}&delay={str(delay)}&universe={universe}"
        resp = self.session.get(url)
        # 提取id,name,description,subcategory
        datasets_info = {
            item["id"]: {
                "name": item["name"],
                "description": item["description"],
                "category": item["category"]["name"],
                "subcategory": item["subcategory"]["name"]  # 提取子类名称
            }
            for item in resp.json()["results"]
        }
        return datasets_info
    
    def data_fields(self, 
        searchScope,
        dataset_id: str = '',
        search: str = ''
    ):
        instrument_type = searchScope['instrumentType']
        region = searchScope['region']
        delay = searchScope['delay']
        universe = searchScope['universe']

        if len(search) == 0:
            url_template = f"{self.base_url}/data-fields?&instrumentType={instrument_type}" + \
                        f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" + \
                        "&offset={x}"
            count = self.session.get(url_template.format(x=0)).json()['count']
        else:
            url_template = f"{self.base_url}/data-fields?&instrumentType={instrument_type}" + \
                        f"&region={region}&delay={str(delay)}&universe={universe}&limit=50" + \
                        f"&search={search}" + \
                        "&offset={x}"
            count = 100

        datafields_list = []
        for x in range(0, count, 50):
            datafields = self.session.get(url_template.format(x=x))
            datafields_list.append(datafields.json()['results'])
        return datafields_list
    
    def get_alphas(
        self
        , alpha_num: int = 1000
        , page_size = 100
        , region="USA"
        , type="REGULAR"
        , status="UNSUBMITTED"
        , is_favorite=None
        , fitness_th:float = None
        , turnover_th:float=None
        , sharpe_th:float=None
        , margin_th:float=None
        , order='-dateCreated'
    ):    
        if alpha_num < page_size:
            page_size = alpha_num
        """获取filter条件下的alpha"""
        url = f"/users/self/alphas?limit={page_size}&settings.region={region}&&type={type}&status={status}&order={order}&hidden=false"
        if is_favorite is not None:
            if is_favorite == True:
                url += "&favorite=true"
            else:
                url += "&favorite=false"

        if fitness_th is not None:
            url += "&is.fitness%3E="+str(fitness_th)
        if turnover_th is not None:
            url += "&is.turnover%3E="+str(turnover_th)
        if sharpe_th is not None:
            url += "&is.sharpe%3E="+str(sharpe_th)
        if margin_th is not None:
            url += "&is.margin%3E="+str(margin_th)
        

        print(f"正在获取 Alpha 列表...")
        
        offset = 0
        alpha_list = []
        try:
        
            
            while True:
                resp = self.session.get(f'{self.base_url}{url}&offset={offset}')
                data = resp.json()
                # 打印一次
                if offset == 0:
                    print(f"共有 Alpha: {data['count']} 条")
                if data['count'] == 0:
                    break
                # 成功获取数据
                results = data['results']

                alpha_list.extend(results)
                # 最后一页了
                if len(results) < page_size:
                    break
                offset += page_size
                if len(alpha_list) >= alpha_num:
                    break
            
            print(f"获取到 {len(alpha_list)} 个 Alpha")
            return alpha_list[:alpha_num]
        except Exception as e:
            print(f"获取 Alpha 列表失败: {str(e)}")

    def _simulate_single_alpha(self, alpha, alpha_index, total_pbar_position):
        """模拟单个 Alpha"""
        try:
            with self.print_lock:
                print(f"开始模拟表达式: {alpha.get('regular', 'Unknown')}")

            # 发送模拟请求
            sim_resp = self.session.post(
                f"{self.base_url}/simulations",
                json=alpha
            )

            if sim_resp.status_code != 201:
                with self.print_lock:
                    print(f"❌ 模拟请求失败 (状态码: {sim_resp.status_code})")
                return None

            try:
                sim_progress_url = sim_resp.headers['Location']
                start_time = datetime.now()

                # 创建本地进度条，使用 position 参数确保每个进度条固定位置
                pbar = tqdm(
                    total=100,
                    desc=f"Alpha {alpha_index}: 模拟进度",
                    position=total_pbar_position + alpha_index,
                    bar_format='{desc}: {percentage:3.0f}%|{bar}| {elapsed} 秒',
                    leave=False
                )
                last_progress = 0

                while True:
                    sim_progress_resp = self.session.get(sim_progress_url)
                    retry_after_sec = float(sim_progress_resp.headers.get("Retry-After", 0))

                    if retry_after_sec == 0:  # simulation done!
                        # 完成进度条
                        if last_progress < 100:
                            pbar.update(100 - last_progress)
                        pbar.close()

                        alpha_id = sim_progress_resp.json()['alpha']
                        with self.print_lock:
                            print(f"✅ 获得 Alpha ID: {alpha_id}")

                        # 等待一下让指标计算完成
                        time.sleep(3)

                        # 获取 Alpha 详情
                        alpha_url = f"{self.base_url}/alphas/{alpha_id}"
                        alpha_detail = self.session.get(alpha_url)
                        alpha_data = alpha_detail.json()

                        # 检查是否有 is 字段
                        if 'is' not in alpha_data:
                            with self.print_lock:
                                print("❌ 无法获取指标数据")
                            return None

                        is_qualified = self.check_alpha_qualification(alpha_data)
                        # 保存hash
                        alpha_hash = self.hash(alpha)
                        self._save_alpha_id(alpha_hash)
                        return {
                            'expression': alpha.get('regular'),
                            'alpha_id': alpha_id,
                            'passed_all_checks': is_qualified,
                            'metrics': alpha_data.get('is', {}),
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }

                    # 更新等待时间和进度
                    elapsed = (datetime.now() - start_time).total_seconds()
                    current_progress = min(95, int((elapsed / 30) * 100))

                    # 只在进度变化时更新进度条
                    if current_progress > last_progress:
                        pbar.update(current_progress - last_progress)
                        last_progress = current_progress

                    time.sleep(retry_after_sec)

            except KeyError:
                with self.print_lock:
                    print("❌ 无法获取模拟进度 URL")
                return None

        except Exception as e:
            with self.print_lock:
                print(f"⚠️ Alpha 模拟失败: {str(e)}")
            return None


    def simulate_alphas(self, alpha_list):
        """并行模拟多个 Alpha 表达式"""

        try:


            print(f"\n🚀 开始并行模拟 {len(alpha_list)} 个 Alpha 表达式 (最大 {self.max_workers} 个线程)...")

            results = []
            results_lock = threading.Lock()  # 用于保护结果列表的线程锁

            # 使用线程池执行模拟任务
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 创建任务到线程池
                futures = {
                    executor.submit(self._simulate_single_alpha, alpha, i, 1): i
                    for i, alpha in enumerate(alpha_list, 1)
                }

                # 创建总进度条，固定在最底部
                with tqdm(total=len(alpha_list), desc="🔄 总体进度", unit="alpha", position=0) as total_pbar:
                    # 处理完成的任务
                    for future in concurrent.futures.as_completed(futures):
                        alpha_index = futures[future]
                        try:
                            result = future.result()

                            with self.print_lock:
                                print(f"\n[{alpha_index}/{len(alpha_list)}] Alpha 模拟完成")
                            # if result and result.get('passed_all_checks'):
                            #     with results_lock:
                            #         results.append(result)
                            #         self._save_alpha_id(result['alpha_id'], result)
                        except Exception as exc:
                            with self.print_lock:
                                print(f"\n[{alpha_index}/{len(alpha_list)}] Alpha 模拟失败: {exc}")

                        # 更新总进度条
                        total_pbar.update(1)

            return results

        except Exception as e:
            print(f"❌ 模拟过程出错: {str(e)}")
            return []
        
    def check_alpha_qualification(self, alpha_data):
        """检查 Alpha 是否满足所有提交条件"""

        try:
            # 从 'is' 字段获取指标
            is_data = alpha_data.get('is', {})
            if not is_data:
                with self.print_lock:
                    print("❌ 无法获取指标数据")
                return False

            # 获取指标值
            sharpe = float(is_data.get('sharpe', 0))
            fitness = float(is_data.get('fitness', 0))
            turnover = float(is_data.get('turnover', 0))
            ic_mean = float(is_data.get('margin', 0))  # margin 对应 IC Mean

            # 获取子宇宙 Sharpe
            sub_universe_check = next(
                (
                    check for check in is_data.get('checks', [])
                    if check['name'] == 'LOW_SUB_UNIVERSE_SHARPE'
                ),
                {}
            )
            subuniverse_sharpe = float(sub_universe_check.get('value', 0))
            required_subuniverse_sharpe = float(sub_universe_check.get('limit', 0))

            # 保护输出，防止多线程输出混乱
            with self.print_lock:
                # 打印指标
                print("\n📊 Alpha 指标详情:")
                print(f"  Sharpe: {sharpe:.3f} (>1.5)")
                print(f"  Fitness: {fitness:.3f} (>1.0)")
                print(f"  Turnover: {turnover:.3f} (0.1-0.9)")
                print(f"  IC Mean: {ic_mean:.3f} (>0.02)")
                print(f"  子宇宙 Sharpe: {subuniverse_sharpe:.3f} (>{required_subuniverse_sharpe:.3f})")

                print("\n📝 指标评估结果:")

            # 检查每个指标并输出结果
            is_qualified = True

            with self.print_lock:
                if sharpe < 1.5:
                    print("❌ Sharpe ratio 不达标")
                    is_qualified = False
                else:
                    print("✅ Sharpe ratio 达标")

                if fitness < 1.0:
                    print("❌ Fitness 不达标")
                    is_qualified = False
                else:
                    print("✅ Fitness 达标")

                if turnover < 0.1 or turnover > 0.9:
                    print("❌ Turnover 不在合理范围")
                    is_qualified = False
                else:
                    print("✅ Turnover 达标")

                if ic_mean < 0.02:
                    print("❌ IC Mean 不达标")
                    is_qualified = False
                else:
                    print("✅ IC Mean 达标")

                if subuniverse_sharpe < required_subuniverse_sharpe:
                    print(f"❌ 子宇宙 Sharpe 不达标 ({subuniverse_sharpe:.3f} < {required_subuniverse_sharpe:.3f})")
                    is_qualified = False
                else:
                    print(f"✅ 子宇宙 Sharpe 达标 ({subuniverse_sharpe:.3f} > {required_subuniverse_sharpe:.3f})")

                print("\n🔍 检查项结果:")
                checks = is_data.get('checks', [])
                for check in checks:
                    name = check.get('name')
                    result = check.get('result')
                    value = check.get('value', 'N/A')
                    limit = check.get('limit', 'N/A')

                    if result == 'PASS':
                        print(f"✅ {name}: {value} (限制: {limit})")
                    elif result == 'FAIL':
                        print(f"❌ {name}: {value} (限制: {limit})")
                        is_qualified = False
                    elif result == 'PENDING':
                        print(f"⚠️ {name}: 检查尚未完成")
                        is_qualified = False

                print("\n📋 最终评判:")
                if is_qualified:
                    print("✅ Alpha 满足所有条件，可以提交!")
                else:
                    print("❌ Alpha 未达到提交标准")

            return is_qualified

        except Exception as e:
            with self.print_lock:
                print(f"❌ 检查 Alpha 资格时出错: {str(e)}")
            return False
            
    # 还需要确保其他被线程调用的方法也是线程安全的
    def _save_alpha_id(self, alpha_id):
        """保存 Alpha ID 到文件"""
        try:
            # 确保目录存在
            parent_path = os.path.dirname(os.path.abspath(self.simulated_alphas_file))
            os.makedirs(parent_path, exist_ok=True)
            
            # 使用线程锁保护文件写入
            with self.print_lock:
                print(f"正在保存 Alpha ID: {alpha_id}")
                # 单独保存 ID 便于后续操作
                with open(self.simulated_alphas_file, 'a') as f:
                    f.write(f"{alpha_id}\n")
                    
                print(f"✅ Alpha ID {alpha_id} 已保存")
        except Exception as e:
            with self.print_lock:
                print(f"❌ 保存 Alpha ID 失败: {str(e)}")

    def batch_update(self, alpha_data):
        """批量更新"""
        try:
            self.session.patch(f"{self.base_url}/alphas", json=alpha_data)
        except Exception as e:
            print(f"❌ 批量更新失败: {str(e)}")

    def is_favorable(self, alpha_id):
        """判断 Alpha 是可收藏的"""
        print(f"正在判断 Alpha {alpha_id} 是否可收藏...")
        resp = self.session.get(f"{self.base_url}/competitions/IQC2025S1/alphas/{alpha_id}/before-and-after-performance")
        retry_after = float(resp.headers.get("Retry-After", 0))
        if retry_after > 0:
            time.sleep(retry_after)
            return self.is_favorable(alpha_id)
        score = resp.json()['score']
        is_favorable = score['after']>score['before']
        if is_favorable:
            print(f"✅ Alpha {alpha_id} 可收藏.....")
        else:
            print(f"❌ Alpha {alpha_id} 不可收藏.....")
        return is_favorable