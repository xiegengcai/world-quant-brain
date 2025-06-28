import asyncio
import pandas as pd
import logging
import time
from typing import Optional, Tuple
from typing import Tuple, Dict, List
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor
import pickle
from collections import defaultdict
import numpy as np


import utils
import wqb

class SelfCorrelation:
    def __init__(self, wqbs: wqb.WQBSession, data_path: str='./results'):
        self.wqbs = wqbs
        self.data_path = data_path
        np.seterr(divide='ignore',invalid='ignore')
        # 增量下载数据
        self.download_data(flag_increment=True)


    def save_obj(self, obj: object, name: str) -> None:
        """
        保存对象到文件中，以 pickle 格式序列化。
        Args:
            obj (object): 需要保存的对象。
            name (str): 文件名（不包含扩展名），保存的文件将以 '.pickle' 为扩展名。
        Returns:
            None: 此函数无返回值。
        Raises:
            pickle.PickleError: 如果序列化过程中发生错误。
            IOError: 如果文件写入过程中发生 I/O 错误。
        """
        with open(name + '.pickle', 'wb') as f:
            pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
    def load_obj(self, name: str) -> object:
        """
        加载指定名称的 pickle 文件并返回其内容。
        此函数会打开一个以 `.pickle` 为扩展名的文件，并使用 `pickle` 模块加载其内容。
        Args:
            name (str): 不带扩展名的文件名称。
        Returns:
            object: 从 pickle 文件中加载的 Python 对象。
        Raises:
            FileNotFoundError: 如果指定的文件不存在。
            pickle.UnpicklingError: 如果文件内容无法被正确反序列化。
        """
        with open(name + '.pickle', 'rb') as f:
            return pickle.load(f)

    def _get_alpha_pnl(self, alpha_id: str) -> pd.DataFrame:
        """
        获取指定 alpha 的 PnL数据，并返回一个包含日期和 PnL 的 DataFrame。
        此函数通过调用 WorldQuant Brain API 获取指定 alpha 的 PnL 数据，
        并将其转换为 pandas DataFrame 格式，方便后续数据处理。
        Args:
            alpha_id (str): Alpha 的唯一标识符。
        Returns:
            pd.DataFrame: 包含日期和对应 PnL 数据的 DataFrame，列名为 'Date' 和 alpha_id。
        """
        # 可能会被限流
        resp = self.wqbs.get(f"{wqb.WQB_API_URL}/alphas/{alpha_id}/recordsets/pnl")
        retry_after = float(resp.headers.get(wqb.RETRY_AFTER, 0))
        if retry_after > 0:
            time.sleep(retry_after)
            return self._get_alpha_pnl(alpha_id)

        pnl = resp.json()
        df = pd.DataFrame(pnl['records'], columns=[item['name'] for item in pnl['schema']['properties']])
        df = df.rename(columns={'date':'Date', 'pnl':alpha_id})
        df = df[['Date', alpha_id]]
        return df
    def get_alpha_pnls(
        self,
        alphas: list[dict],
        alpha_pnls: Optional[pd.DataFrame] = None,
        alpha_ids: Optional[dict[str, list]] = None
    ) -> Tuple[dict[str, list], pd.DataFrame]:
        """
        获取 alpha 的 PnL 数据，并按区域分类 alpha 的 ID。
        Args:
            alphas (list[dict]): 包含 alpha 信息的列表，每个元素是一个字典，包含 alpha 的 ID 和设置等信息。
            alpha_pnls (Optional[pd.DataFrame], 可选): 已有的 alpha PnL 数据，默认为空的 DataFrame。
            alpha_ids (Optional[dict[str, list]], 可选): 按区域分类的 alpha ID 字典，默认为空字典。
        Returns:
            Tuple[dict[str, list], pd.DataFrame]:
                - 按区域分类的 alpha ID 字典。
                - 包含所有 alpha 的 PnL 数据的 DataFrame。
        """
        if alpha_ids is None:
            alpha_ids = defaultdict(list)
        if alpha_pnls is None:
            alpha_pnls = pd.DataFrame()

        new_alphas = [item for item in alphas if item['id'] not in alpha_pnls.columns]
        if not new_alphas:
            return alpha_ids, alpha_pnls

        for item_alpha in new_alphas:
            alpha_ids[item_alpha['settings']['region']].append(item_alpha['id'])
        fetch_pnl_func = lambda alpha_id: self._get_alpha_pnl(alpha_id).set_index('Date')
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = executor.map(fetch_pnl_func, [item['id'] for item in new_alphas])
        alpha_pnls = pd.concat([alpha_pnls] + list(results), axis=1)
        alpha_pnls.sort_index(inplace=True)
        return alpha_ids, alpha_pnls
    # def get_os_alphas(self, limit: int = 100, get_first: bool = False) -> List[Dict]:
    #     """
    #     获取OS阶段的alpha列表。
    #     此函数通过调用WorldQuant Brain API获取用户的alpha列表，支持分页获取，并可以选择只获取第一个结果。
    #     Args:
    #         limit (int, optional): 每次请求获取的alpha数量限制。默认为100。
    #         get_first (bool, optional): 是否只获取第一次请求的alpha结果。如果为True，则只请求一次。默认为False。
    #     Returns:
    #         List[Dict]: 包含alpha信息的字典列表，每个字典表示一个alpha。
    #     """
    #     return utils.filter_alphas(
    #         self.wqbs,
    #         status='ACTIVE',
    #         order='-dateSubmitted',
    #         others=['stage=OS'],
    #         log_name=f'{self.__class__}#get_os_alphas'
    #     )
    def calc_self_corr(
        self,
        alpha_id: str,
        # os_alpha_rets: pd.DataFrame | None = None,
        # os_alpha_ids: dict[str, str] | None = None,
        alpha_result: dict | None = None,
        return_alpha_pnls: bool = False,
        alpha_pnls: pd.DataFrame | None = None
    ) -> float | tuple[float, pd.DataFrame]:
        """
        计算指定 alpha 与其他 alpha 的最大自相关性。
        Args:
            alpha_id (str): 目标 alpha 的唯一标识符。
            os_alpha_rets (pd.DataFrame | None, optional): 其他 alpha 的收益率数据，默认为 None。
            os_alpha_ids (dict[str, str] | None, optional): 其他 alpha 的标识符映射，默认为 None。
            alpha_result (dict | None, optional): 目标 alpha 的详细信息，默认为 None。
            return_alpha_pnls (bool, optional): 是否返回 alpha 的 PnL 数据，默认为 False。
            alpha_pnls (pd.DataFrame | None, optional): 目标 alpha 的 PnL 数据，默认为 None。
        Returns:
            float | tuple[float, pd.DataFrame]: 如果 `return_alpha_pnls` 为 False，返回最大自相关性值；
                如果 `return_alpha_pnls` 为 True，返回包含最大自相关性值和 alpha PnL 数据的元组。
        """
        os_alpha_ids, os_alpha_rets =self.load_data()
        if alpha_result is None:
            alpha_result = asyncio.run(self.wqbs.locate_alpha(alpha_id=alpha_id, log=f'{self.__class__}#calc_self_corr')).json()
        if alpha_pnls is not None:
            if len(alpha_pnls) == 0:
                alpha_pnls = None
        if alpha_pnls is None:
            _, alpha_pnls = self.get_alpha_pnls([alpha_result])
            # _, alpha_pnls = self.wqbs.get_alpha_pnls_bulk([alpha_result])
            alpha_pnls = alpha_pnls[alpha_id]
        alpha_rets = alpha_pnls - alpha_pnls.ffill().shift(1)
        alpha_rets = alpha_rets[pd.to_datetime(alpha_rets.index)>pd.to_datetime(alpha_rets.index).max() - pd.DateOffset(years=4)]
        # os_alpha_rets = os_alpha_rets.replace(0, np.nan)
        # alpha_rets = alpha_rets.replace(0, np.nan)
        # print(os_alpha_rets[os_alpha_ids[alpha_result['settings']['region']]].corrwith(alpha_rets).sort_values(ascending=False).round(4))
        os_alpha_rets[os_alpha_ids[alpha_result['settings']['region']]].corrwith(alpha_rets).sort_values(ascending=False).round(4).to_csv(f'{self.data_path}/os_alpha_corr.csv')
        self_corr = os_alpha_rets[os_alpha_ids[alpha_result['settings']['region']]].corrwith(alpha_rets).max()
        if np.isnan(self_corr):
            self_corr = 0
        if return_alpha_pnls:
            return self_corr, alpha_pnls
        else:
            return self_corr
    def download_data(self,flag_increment=True):
        """
        Downloads and saves alpha data from the API.
        
        This function checks if data already exists, and if not, downloads it from the API and saves to the specified path.
        It supports incremental downloads to avoid re-downloading existing data.
        
        Args:
            flag_increment (bool): Whether to use incremental download mode. Defaults to True.
                If True, loads existing data and only downloads new alphas.
                If False, downloads all alphas from scratch.
        
        The function downloads alpha IDs and PnL data, and saves them to three files:
        - os_alpha_ids: Dictionary mapping of alpha IDs
        - os_alpha_pnls: Matrix of alpha PnL values  
        - ppac_alpha_ids: List of Power Pool Alpha IDs
        
        Prints summary of newly downloaded alphas and total alpha count.
        """
        """
        下载数据并保存到指定路径。
        此函数会检查数据是否已经存在，如果不存在，则从 API 下载数据并保存到指定路径。
        Args:
            flag_increment (bool): 是否使用增量下载，默认为 True。
        """
        if flag_increment:
            try:
                os_alpha_ids = self.load_obj(f'{self.data_path}/os_alpha_ids')
                os_alpha_pnls = self.load_obj(f'{self.data_path}/os_alpha_pnls')
                ppac_alpha_ids = self.load_obj(f'{self.data_path}/ppac_alpha_ids')
                exist_alpha = [alpha for ids in os_alpha_ids.values() for alpha in ids]
            except Exception as e:
                logging.error(f"Failed to load existing data: {e}")
                os_alpha_ids = None
                os_alpha_pnls = None
                exist_alpha = []
                ppac_alpha_ids = []
        else:
            os_alpha_ids = None
            os_alpha_pnls = None
            exist_alpha = []
            ppac_alpha_ids = []

        if os_alpha_ids is None:
            # alphas = self.get_os_alphas(limit=100, get_first=False)
            alphas = self.wqbs.get_os_alphas(limit=100, get_first=False)
        else:
            # alphas = self.get_os_alphas(limit=30, get_first=True)
             alphas = self.wqbs.get_os_alphas(limit=100, get_first=True)

        alphas = [item for item in alphas if item['id'] not in exist_alpha]
        ppac_alpha_ids += [item['id'] for item in alphas for item_match in item['classifications'] if item_match['name'] == 'Power Pool Alpha']

        os_alpha_ids, os_alpha_pnls = self.get_alpha_pnls(alphas, alpha_pnls=os_alpha_pnls, alpha_ids=os_alpha_ids)
        self.save_obj(os_alpha_ids, f'{self.data_path}/os_alpha_ids')
        self.save_obj(os_alpha_pnls, f'{self.data_path}/os_alpha_pnls')
        self.save_obj(ppac_alpha_ids, f'{self.data_path}/ppac_alpha_ids')
        print(f'新下载的alpha数量: {len(alphas)}, 目前总共alpha数量: {os_alpha_pnls.shape[1]}')
    def load_data(self,tag=None):
        """
        加载数据。
        此函数会检查数据是否已经存在，如果不存在，则从 API 下载数据并保存到指定路径。
        Args:
            tag (str): 数据标记，默认为 None。
        """
        os_alpha_ids = self.load_obj(f'{self.data_path}/os_alpha_ids')
        os_alpha_pnls = self.load_obj(f'{self.data_path}/os_alpha_pnls')
        ppac_alpha_ids = self.load_obj(f'{self.data_path}/ppac_alpha_ids')
        if tag=='PPAC':
            for item in os_alpha_ids:
                os_alpha_ids[item] = [alpha for alpha in os_alpha_ids[item] if alpha in ppac_alpha_ids]
        elif tag=='SelfCorr':
            for item in os_alpha_ids:
                os_alpha_ids[item] = [alpha for alpha in os_alpha_ids[item] if alpha not in ppac_alpha_ids]
        else:
            os_alpha_ids = os_alpha_ids
        exist_alpha = [alpha for ids in os_alpha_ids.values() for alpha in ids]
        os_alpha_pnls = os_alpha_pnls[exist_alpha]
        os_alpha_rets = os_alpha_pnls - os_alpha_pnls.ffill().shift(1)
        os_alpha_rets = os_alpha_rets[pd.to_datetime(os_alpha_rets.index)>pd.to_datetime(os_alpha_rets.index).max() - pd.DateOffset(years=4)]
        return os_alpha_ids, os_alpha_rets

    def filter_correlation(self, alpha_list: list, threshold:float=0.7) -> list:
        list=[]
        lines = []
        print(f'过滤相关性大于{threshold}的Alpha...')
        os_alpha_ids, os_alpha_rets = self.load_data(tag='SelfCorr')
        # os_alpha_ids, os_alpha_rets =self.correlation.load_data()
        for alpha in alpha_list:
            try:
                ret = self.calc_self_corr(
                    alpha_id=alpha['id'],
                    os_alpha_rets=os_alpha_rets
                    ,os_alpha_ids=os_alpha_ids
                )
                if ret <= threshold:
                    lines.append(f"{alpha['id']}\n")
                    list.append(alpha)
            except Exception as e:
                print(f'计算alpha {alpha["id"]} 自相关性失败: {e}')
        utils.save_lines_to_file('./results/correlation.txt', lines)
        return list