# -*- coding: utf-8 -*-

# 初始状态
ALPHA_STATUS_INIT = 'INIT'
# 已回测
ALPHA_STATUS_SIMUATED = 'SIMULATED'
# 已同步指标数据
ALPHA_STATUS_SYNC = 'SYNC'
# 已检查自相关性
ALPHA_STATUS_CHECKED = 'CHECKED'
# 已提交
ALPHA_STATUS_SUBMITTED = 'SUBMITTED'
# 已废弃（失败或主动自相关过高）
ALPHA_STATUS_DISCARDED = 'DISCARDED'

IS_SHARPE = 'sharpe'
IS_FITNESS = 'fitness'
IS_TURNOVER = 'turnover'
IS_RETURNS = 'returns'
IS_DRAWDOWN = 'drawdown'
IS_LONGCOUNT = 'longCount'
IS_SHORTCOUNT = 'shortCount'
IS_MARGIN = 'margin'

STEP_FIRST = 1
STEP_SECOND = 2
STEP_THIRD = 3
