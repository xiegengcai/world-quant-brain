# -*- coding: utf-8 -*-

"""数据集配置管理模块"""

default_settings ={
    'instrumentType': 'EQUITY',
    'region': 'USA',
    'universe': 'TOP3000',
    'delay': 1,
    'decay': 0,
    'neutralization': 'SUBINDUSTRY',
    'truncation': 0.08,
    'pasteurization': 'ON',
    'unitHandling': 'VERIFY',
    'nanHandling': 'ON',
    'language': 'FASTEXPR',
    'visualization': False,
}

DATASET_CONFIGS = {
    'fundamental2': {
        'id': 'fundamental2',
        'field_prefix': 'fn',
        'universe': 'TOP3000',
        'description': 'Report Footnotes',
        'api_settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'delay': 1,
            'decay': 0,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False
        }
    },
    'fundamental6': {
        'id': 'fundamental6',
        'field_prefix': None,
        'universe': 'TOP3000',
        'description': '基础财务数据',
        'api_settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'delay': 1,
            'decay': 0,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False
        }
    },
    'analyst4': {
        'id': 'analyst4',
        'field_prefix': 'anl4',
        'universe': 'TOPSP500',
        'description': '分析师预测数据',
        'api_settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'delay': 1,
            'decay': 0,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False
        }
    },
    'pv1': {
        'id': 'pv1',
        'field_prefix': None,
        'universe': 'TOP3000',
        'description': '股市成交量数据',
        'api_settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'delay': 1,
            'decay': 0,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False
        }
    },'option8': {
        'id': 'option8',
        'field_prefix': None,
        'universe': 'TOPSP500',
        'description': '波动性数据',
        'api_settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'delay': 1,
            'decay': 0,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False
        }
    },
    'option9': {
        'id': 'option9',
        'field_prefix': None,
        'universe': 'TOPSP500',
        'description': 'US News Data',
        'api_settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'delay': 1,
            'decay': 0,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False
        }
    },
    'news12': {
        'id': 'news12',
        'field_prefix': 'news',
        'universe': 'TOP200',
        'description': 'US News Data',
        'api_settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'delay': 1,
            'decay': 0,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False
        }
    },
    'news18': {
        'id': 'news18',
        'field_prefix': None,
        'universe': 'TOP200',
        'description': 'Ravenpack News Data',
        'api_settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'delay': 1,
            'decay': 0,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False
        }
    },
    'socialmedia12': {
        'id': 'socialmedia12',
        'field_prefix': 'scl12',
        'universe': 'TOP500',
        'description': '股票情绪数据',
        'api_settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'delay': 1,
            'decay': 0,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False
        }
    },
    'sentiment1': {
        'id': 'sentiment1',
        'field_prefix': 'snt1',
        'universe': 'TOP500',
        'description': '股市成交量数据',
        'api_settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'delay': 1,
            'decay': 0,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False
        }
    },
    'model51': {
        'id': 'model51',
        'field_prefix': None,
        'universe': 'TOP3000',
        'description': '系统性风险指标',
        'api_settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'delay': 1,
            'decay': 0,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False
        }
    },
    'model16': {
        'id': 'model16',
        'field_prefix': None,
        'universe': 'TOP3000',
        'description': 'Fundamental Scores',
        'api_settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'delay': 1,
            'decay': 0,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False
        }
    },
    'model77':{
        'id': 'model77',
        'field_prefix': 'mdl77',
        'universe': 'TOP3000',
        'description': 'Model/Technical Models',
        'api_settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'delay': 1,
            'decay': 6,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False
        }
    },
    'model53':{
        'id': 'model53',
        'field_prefix': 'mdl53',
        'universe': 'TOP3000',
        'description': 'Creditworthiness Risk Measure Model',
        'api_settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'delay': 1,
            'decay': 6,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False
        }
    },
    'fundamental7':{
        'id': 'fundamental7',
        'field_prefix': 'fnd7',
        'universe': 'TOP3000',
        'description': 'Comprehensive Fundamentals Data',
        'api_settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'delay': 1,
            'decay': 6,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False
        }
    }
}


def get_dataset_list():
    """获取所有可用数据集列表"""

    return [
        f"{idx+1}: {name} ({config['universe']}) - {config['description']}"
        for idx, (name, config) in enumerate(DATASET_CONFIGS.items())
    ]


def get_dataset_config(dataset_name):
    """获取指定数据集的配置"""

    return DATASET_CONFIGS.get(dataset_name)

def get_dataset_by_index(index):
    """通过索引获取数据集名称"""

    try:
        return list(DATASET_CONFIGS.values())[int(index)-1]['id']
    except (IndexError, ValueError):
        return None

def get_api_settings(dataset_name):
    """获取指定数据集的API设置"""

    config = DATASET_CONFIGS.get(dataset_name)
    if config and 'api_settings' in config:
        settings = config['api_settings'].copy()
        settings['universe'] = config['universe']
        return settings
    return default_settings.copy()
