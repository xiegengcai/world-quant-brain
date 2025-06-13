# import datetime
import json
from os.path import expanduser
import time
import wqb
from wqb import WQBSession
from wqb import print as p
from datetime import datetime, timedelta

import utils

# 加载凭据文件
with open(expanduser('~/.brain_credentials.txt')) as f:
    credentials = json.load(f)

# 从列表中提取用户名和密码
username, password = credentials

# Create `logger`
logger = wqb.wqb_logger(name='logs/wqb_check' + datetime.now().strftime('%Y%m%d'))
wqb.print(f"{logger.name = }")  # print(f"{logger.name = }", flush=True)

# Manual logging
# logger.info('This is an info for testing.')
# logger.warning('This is a warning for testing.')

# Create `wqbs`
wqbs = WQBSession((username, password), logger=logger)
# If `logger` was not created, use the following line instead.
# wqbs = WQBSession(('<email>', '<password>'))

# Test connectivity (Optional)
resp = wqbs.auth_request()
p(resp.status_code)  # 201
p(resp.ok)  # True
p(resp.json()['user']['id'])  # <Your BRAIN User ID>

# from datetime import datetime
from wqb import FilterRange


# 获取明天日期 格式2025-05-10
def get_tomorrow_date():
    today = datetime.now()
    tomorrow = today + timedelta(days=1)
    return tomorrow.strftime('%Y-%m-%d')


formatted_time = get_tomorrow_date()
lo = datetime.fromisoformat('2025-05-10T00:00:00-04:00')
hi = datetime.fromisoformat(f'{formatted_time}T00:00:00-04:00')
# resp = wqbs.filter_alphas_limited(
#     status='UNSUBMITTED',
#     # region='USA',
#     # delay=1,
#     # universe='TOP3000',
#     # sharpe=FilterRange.from_str('[1.58, inf)'),
#     # fitness=FilterRange.from_str('[1, inf)'),
#     # turnover=FilterRange.from_str('(-inf, 0.7]'),
#     # date_created=FilterRange.from_str(f"[{lo.isoformat()}, {hi.isoformat()})"),
#     order='dateCreated',
# )
# alpha_ids = [item['id'] for item in resp.json()['results']]
# print(alpha_ids)


import random
import time
import requests
import json

sess = wqbs
# 获取未提交的 Alpha 列表url
unsubmitted_alpha_list_url = r'https://api.worldquantbrain.com/users/self/alphas'
submitted_alpha_list_url = "https://api.worldquantbrain.com/users/self/alphas"

unsubmitted_alpha_list = []
submitted_alpha_list = []

unsubmitted_alpha_params = {
    'limit': 100,
    'offset': 0,
    'status': 'UNSUBMITTED%1FIS_FAIL',
    'is.sharpe%3E': 1.25,
    'is.fitness%3E': 1,
    'is.turnover%3E': 0.01,
    'is.turnover%3C': 0.7,
    # 'dateCreated%3E': '2025-04-26T00:00:00-04:00',
    # 'dateCreated%3C': '2025-04-30T00:00:00-04:00',
    # 'color': 'GREEN',  # 'RED', 'YELLOW', 'GREEN', 'BLUE', 'PURPLE'
    'order': '-dateCreated',
    'hidden': 'false',
    # 'settings.universe': 'TOP1000',
    # "settings.delay": 1,
}
unsubmitted_alpha_params_two = {
    'limit': 100,
    'offset': 0,
    'status': 'UNSUBMITTED%1FIS_FAIL',
    'is.sharpe%3C': -1.25,
    'is.fitness%3C': -1,
    'order': '-dateCreated',
    'hidden': 'false',
    # "settings.delay": 1,
}

submitted_alpha_params = {
    'limit': 100,
    'offset': 0,
    'order': '-dateSubmitted',
    'hidden': 'false'
}
status_str = '&status!=UNSUBMITTED%1FIS-FAIL'


# 递归获取所有 Alpha
def fetch_alphas(sess, url, params, alpha_list, is_sub: bool,color=None):
    # 手动拼接查询字符串
    # if color:
    #     params.update({'color': color})
    query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
    if color:
       query_string += f"{query_string}&color!={color}"
    wqb.print(f"Fetching {url} with query_string {query_string}...")
    if is_sub:
        full_url = f'{url}?{query_string}{status_str}'
    else:
        full_url = f'{url}?{query_string}'
    response = sess.get(full_url)

    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}")

    try:
        res = response.json()
    except ValueError:
        raise Exception("Failed to parse JSON response")

    alpha_list.extend(res['results'])

    if res.get('next'):
        # 解析 next URL 并更新 offset 参数
        next_params = dict(params)  # 复制当前参数
        next_params['offset'] += next_params['limit']  # 更新 offset
        fetch_alphas(sess, url, next_params, alpha_list, is_sub, color)


def get_pnl_data(sess, alpha_id):
    url = f"https://api.worldquantbrain.com/alphas/{alpha_id}/recordsets/pnl"
    response = sess.get(url)
    while True:
        try:
            if response.status_code == 200:
                if not response.text:
                    time.sleep(random.uniform(1, 10))
                    response = sess.get(url)
                else:
                    return response.json()
        except Exception as e:
            print(f"Failed to fetch PNL data for Alpha {alpha_id}. Status code: {response.status_code}")
            return None



green_url = r"https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=0&status=UNSUBMITTED%1FIS_FAIL&color=GREEN&order=-dateCreated&hidden=false"


# alpha_list = []
def get_pnl_data_list(status, file_path,color=None):
    # 加载本地已有数据
    try:
        with open(file_path, 'r') as f:
            pnl_dict = json.load(f)
    except:
        pnl_dict = {}

    # 获取最新数据列表
    alpha_list = []
    if status == 'submitted':
        # fetch_alphas(sess, submitted_alpha_list_url, submitted_alpha_params, alpha_list, True,color)
        alpha_list = utils.filter_alphas(
            wqbs,
            status='ACTIVE',
            # region=self.searchScope['region'],
            # delay=self.searchScope['delay'],
            log_name=f'pnl_anl_5#get_active_alphas'
        )
        data = alpha_list
    else:
        # fetch_alphas(sess, unsubmitted_alpha_list_url, unsubmitted_alpha_params, alpha_list, False,color)
        alpha_list=utils.submitable_alphas(
            wqbs,
            start_time='2025-06-04T00:00:00-05:00',
            end_time=f'{datetime.strftime(datetime.now(), "%Y-%m-%d")}T23:59:59-05:00',
            limit=10000, order='is.sharpe',others=['color!=RED']
        )
        data = [
            i for i in alpha_list
            if "FAIL" not in [j['result'].upper() for j in i['is']['checks']]
        ]

    print(f'total_pnl:::{len(data)}')

    # 统一处理新增逻辑
    new_ids = {item['id'] for item in data}
    existing_ids = set(pnl_dict.keys())

    # 新增缺失条目
    for index, sub_alpha in enumerate(data):
        if sub_alpha['id'] in existing_ids:
            print(f'{index + 1}. {sub_alpha["id"]}已存在')
            continue

        print(f'{index + 1}. {sub_alpha["id"]}保存中')
        if pnl_data := get_pnl_data(sess, sub_alpha['id']):
            pnl_dict[sub_alpha['id']] = pnl_data
        print(f'{index + 1}. {sub_alpha["id"]}保存')
        time.sleep(random.uniform(1, 5))

    # 仅当非submitted时处理移除逻辑
    if status != 'submitted':
        # 移除本地多余条目
        removed_ids = existing_ids - new_ids
        for rid in removed_ids:
            del pnl_dict[rid]
            print(f'{rid} 已从本地移除')
    if new_ids == existing_ids:
        print('无新增条目')
    else:
        # 统一保存结果
        with open(file_path, 'w') as f:
            json.dump(pnl_dict, f)

# def get_pnl_data_list(status, file_path):
#     alpha_list =  []
#     try:
#         with open(file_path, 'r') as f:
#             pnl_dict = json.load(f)
#     except:
#         pnl_dict = {}
#
#     if status == 'submitted':
#         fetch_alphas(sess, submitted_alpha_list_url, submitted_alpha_params, alpha_list, True)
#         data = alpha_list
#     else:
#         fetch_alphas(sess, unsubmitted_alpha_list_url, unsubmitted_alpha_params, alpha_list, False)
#         alpha_ids = []
#         for i in alpha_list:
#             is_check = i['is']['checks']
#             results = [(j['result']).upper() for j in is_check]
#             if "FAIL" in results:
#                 continue
#             alpha_ids.append(i)
#         data = alpha_ids
#     print(f'total_pnl:::{len(data)}')
#     new_pnl_dict = {}
#     for index, sub_alpha in enumerate(data):
#         if pnl_dict.get(sub_alpha['id'], None):
#             print(f'{index + 1}. {sub_alpha["id"]}已存在')
#             continue
#         print(f'{index + 1}. {sub_alpha["id"]}保存中')
#         pnl_data = get_pnl_data(sess, sub_alpha['id'])
#         if pnl_data:
#             pnl_dict.update({sub_alpha['id']: pnl_data})
#         print(f'{index + 1}. {sub_alpha["id"]}保存')
#         time.sleep(random.uniform(0, 0.5))
#
#     with open(file_path, 'w') as f:
#         json.dump(pnl_dict, f)
#
#     # print(len(pnl_list))


# get_pnl_data_list('submitted', 'pnl_list.json')
# get_pnl_data_list('unsubmitted', 'pnl_list_unsub_d1_blue.json')
