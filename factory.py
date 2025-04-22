# -*- coding: utf-8 -*-

import dataset_config
from itertools import product

basic_ops = ["reverse", "inverse", "rank", "zscore", "quantile", "normalize"]
 
ts_ops = ["ts_rank", "ts_zscore", "ts_delta",  "ts_sum", "ts_delay", 
        "ts_std_dev", "ts_mean",  "ts_arg_min", "ts_arg_max","ts_scale", "ts_quantile"]

ops_set = basic_ops + ts_ops

dedefault_settings = {
    'instrumentType': 'EQUITY',
    'region': 'USA',
    'universe': 'TOP3000',
    'delay': 1,
    'decay': 6,
    'neutralization': 'SUBINDUSTRY',
    'truncation': 0.08,
    'pasteurization': 'ON',
    'testPeriod': 'P2Y',
    'unitHandling': 'VERIFY',
    'nanHandling': 'ON',
    'language': 'FASTEXPR',
    'visualization': False,
}

def generate_sim_data(dataset_id:str, alpha_list):
    """生成回测数据"""
    sim_data_list = []
    settings = dataset_config.get_api_settings(dataset_id)
    if settings is None:
        settings = dedefault_settings.copy()
        
    for regular in alpha_list:
        simulation_data = {
            'type': 'REGULAR',
            'settings': settings,
            'regular': regular
        }

        sim_data_list.append(simulation_data)
    return sim_data_list
    
def first_order_factory(fields, ops_set):
    alpha_set = []
    #for field in fields:
    for field in fields:
        #reverse op does the work
        alpha_set.append(field)
        #alpha_set.append("-%s"%field)
        for op in ops_set:

            if op == "ts_percentage":

                #lpha_set += ts_comp_factory(op, field, "percentage", [0.2, 0.5, 0.8])
                alpha_set += ts_comp_factory(op, field, "percentage", [0.5])


            elif op == "ts_decay_exp_window":

                #alpha_set += ts_comp_factory(op, field, "factor", [0.2, 0.5, 0.8])
                alpha_set += ts_comp_factory(op, field, "factor", [0.5])


            elif op == "ts_moment":

                alpha_set += ts_comp_factory(op, field, "k", [2, 3, 4])

            elif op == "ts_entropy":

                #alpha_set += ts_comp_factory(op, field, "buckets", [5, 10, 15, 20])
                alpha_set += ts_comp_factory(op, field, "buckets", [10])

            elif op.startswith("ts_") or op == "inst_tvr":

                alpha_set += ts_factory(op, field)

            elif op.startswith("group_"):

                alpha_set += group_factory(op, field, "usa")

            elif op.startswith("vector"):

                alpha_set += vector_factory(op, field)

            elif op == "signed_power":

                alpha = "%s(%s, 2)"%(op, field)
                alpha_set.append(alpha)

            else:
                alpha = "%s(%s)"%(op, field)
                alpha_set.append(alpha)

    return alpha_set

def get_group_second_order_factory(first_order, group_ops, region):
    second_order = []
    for fo in first_order:
        for group_op in group_ops:
            second_order += group_factory(group_op, fo, region)
    return second_order

def get_ts_second_order_factory(first_order, ts_ops):
    second_order = []
    for fo in first_order:
        for ts_op in ts_ops:
            second_order += ts_factory(ts_op, fo)
    return second_order

def ts_arith_factory(ts_op, arith_op, field):
    first_order = "%s(%s)"%(arith_op, field)
    second_order = ts_factory(ts_op, first_order)
    return second_order
 
def arith_ts_factory(arith_op, ts_op, field):
    second_order = []
    first_order = ts_factory(ts_op, field)
    for fo in first_order:
        second_order.append("%s(%s)"%(arith_op, fo))
    return second_order
 
def ts_group_factory(ts_op, group_op, field, region):
    second_order = []
    first_order = group_factory(group_op, field, region)
    for fo in first_order:
        second_order += ts_factory(ts_op, fo)
    return second_order
 
def group_ts_factory(group_op, ts_op, field, region):
    second_order = []
    first_order = ts_factory(ts_op, field)
    for fo in first_order:
        second_order += group_factory(group_op, fo, region)
    return second_order
 
def vector_factory(op, field):
    output = []
    vectors = ["cap"]
    
    for vector in vectors:
    
        alpha = "%s(%s, %s)"%(op, field, vector)
        output.append(alpha)
    
    return output
 
def trade_when_factory(op,field):
    output = []
    open_events = ["ts_arg_max(volume, 5) == 0", "ts_corr(close, volume, 20) < 0",
                   "ts_corr(close, volume, 5) < 0", "ts_mean(volume,10)>ts_mean(volume,60)",
                   "group_rank(ts_std_dev(returns,60), sector) > 0.7", "ts_zscore(returns,60) > 2",
                   "ts_arg_min(volume, 5) > 3",
                   "ts_std_dev(returns, 5) > ts_std_dev(returns, 20)",
                   "ts_arg_max(close, 5) == 0", "ts_arg_max(close, 20) == 0",
                   "ts_corr(close, volume, 5) > 0", "ts_corr(close, volume, 5) > 0.3", "ts_corr(close, volume, 5) > 0.5",
                   "ts_corr(close, volume, 20) > 0", "ts_corr(close, volume, 20) > 0.3", "ts_corr(close, volume, 20) > 0.5",
                   "ts_regression(returns, %s, 5, lag = 0, rettype = 2) > 0"%field,
                   "ts_regression(returns, %s, 20, lag = 0, rettype = 2) > 0"%field,
                   "ts_regression(returns, ts_step(20), 20, lag = 0, rettype = 2) > 0",
                   "ts_regression(returns, ts_step(5), 5, lag = 0, rettype = 2) > 0"]

    exit_events = ["abs(returns) > 0.1", "-1"]

    # usa_events = ["rank(rp_css_business) > 0.8", "ts_rank(rp_css_business, 22) > 0.8", "rank(vec_avg(mws82_sentiment)) > 0.8",
    #               "ts_rank(vec_avg(mws82_sentiment),22) > 0.8", "rank(vec_avg(nws48_ssc)) > 0.8",
    #               "ts_rank(vec_avg(nws48_ssc),22) > 0.8", "rank(vec_avg(mws50_ssc)) > 0.8", "ts_rank(vec_avg(mws50_ssc),22) > 0.8",
    #               "ts_rank(vec_sum(scl12_alltype_buzzvec),22) > 0.9", "pcr_oi_270 < 1", "pcr_oi_270 > 1",]

    # asi_events = ["rank(vec_avg(mws38_score)) > 0.8", "ts_rank(vec_avg(mws38_score),22) > 0.8"]

    # eur_events = ["rank(rp_css_business) > 0.8", "ts_rank(rp_css_business, 22) > 0.8",
    #               "rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos)) > 0.8",
    #               "ts_rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos),22) > 0.8",
    #               "rank(vec_avg(mws84_sentiment)) > 0.8", "ts_rank(vec_avg(mws84_sentiment),22) > 0.8",
    #               "rank(vec_avg(mws85_sentiment)) > 0.8", "ts_rank(vec_avg(mws85_sentiment),22) > 0.8",
    #               "rank(mdl110_analyst_sentiment) > 0.8", "ts_rank(mdl110_analyst_sentiment, 22) > 0.8",
    #               "rank(vec_avg(nws3_scores_posnormscr)) > 0.8",
    #               "ts_rank(vec_avg(nws3_scores_posnormscr),22) > 0.8",
    #               "rank(vec_avg(mws36_sentiment_words_positive)) > 0.8",
    #               "ts_rank(vec_avg(mws36_sentiment_words_positive),22) > 0.8"]

    # glb_events = ["rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
    #               "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
    #               "rank(vec_avg(nws20_ssc)) > 0.8",
    #               "ts_rank(vec_avg(nws20_ssc),22) > 0.8",
    #               "vec_avg(nws20_ssc) > 0",
    #               "rank(vec_avg(nws20_bee)) > 0.8",
    #               "ts_rank(vec_avg(nws20_bee),22) > 0.8",
    #               "rank(vec_avg(nws20_qmb)) > 0.8",
    #               "ts_rank(vec_avg(nws20_qmb),22) > 0.8"]

    # chn_events = ["rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform)) > 0.8",
    #               "ts_rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform),22) > 0.8",
    #               "rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform)) > 0.8",
    #               "ts_rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform),22) > 0.8",
    #               "rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform)) > 0.8",
    #               "ts_rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform),22) > 0.8"]

    # kor_events = ["rank(vec_avg(mdl110_analyst_sentiment)) > 0.8",
    #               "ts_rank(vec_avg(mdl110_analyst_sentiment),22) > 0.8",
    #               "rank(vec_avg(mws38_score)) > 0.8",
    #               "ts_rank(vec_avg(mws38_score),22) > 0.8"]

    # twn_events = ["rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
    #               "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
    #               "rank(rp_ess_business) > 0.8",
    #               "ts_rank(rp_ess_business,22) > 0.8"]

    for oe in open_events:
        for ee in exit_events:
            alpha = "%s(%s, %s, %s)"%(op, oe, field, ee)
            output.append(alpha)
    return output
 
def ts_factory(op, field):
    output = []
    #days = [3, 5, 10, 20, 60, 120, 240]
    days = [5, 22, 66, 120, 240]
    
    for day in days:
    
        alpha = "%s(%s, %d)"%(op, field, day)
        output.append(alpha)
    
    return output
 
def ts_comp_factory(op, field, factor, paras):
    output = []
    #l1, l2 = [3, 5, 10, 20, 60, 120, 240], paras
    l1, l2 = [5, 22, 66, 240], paras
    comb = list(product(l1, l2))
    
    for day,para in comb:
        
        if type(para) == float:
            alpha = "%s(%s, %d, %s=%.1f)"%(op, field, day, factor, para)
        elif type(para) == int:
            alpha = "%s(%s, %d, %s=%d)"%(op, field, day, factor, para)
            
        output.append(alpha)
    
    return output
 
def twin_field_factory(op, field, fields):
    
    output = []
    #days = [3, 5, 10, 20, 60, 120, 240]
    days = [5, 22, 66, 240]
    outset = list(set(fields) - set([field]))
    
    for day in days:
        for counterpart in outset:
            alpha = "%s(%s, %s, %d)"%(op, field, counterpart, day)
            output.append(alpha)
    
    return output
 
 
def group_factory(op, field, region):
    output = []
    vectors = ["cap"] 
    
    chn_group_13 = ['pv13_h_min2_sector', 'pv13_di_6l', 'pv13_rcsed_6l', 'pv13_di_5l', 'pv13_di_4l', 
                        'pv13_di_3l', 'pv13_di_2l', 'pv13_di_1l', 'pv13_parent', 'pv13_level']
    
    
    chn_group_1 = ['sta1_top3000c30','sta1_top3000c20','sta1_top3000c10','sta1_top3000c2','sta1_top3000c5']
    
    chn_group_2 = ['sta2_top3000_fact4_c10','sta2_top2000_fact4_c50','sta2_top3000_fact3_c20']
    
    hkg_group_13 = ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 
                    'pv13_2_minvol_1m_sector', 'pv13_5_minvol_1m_sector', 'pv13_1l_scibr', 'pv13_3l_scibr',
                    'pv13_2l_scibr', 'pv13_4l_scibr', 'pv13_5l_scibr']
    
    hkg_group_1 = ['sta1_allc50','sta1_allc5','sta1_allxjp_513_c20','sta1_top2000xjp_513_c5']
    
    hkg_group_2 = ['sta2_all_xjp_513_all_fact4_c10','sta2_top2000_xjp_513_top2000_fact3_c10',
                   'sta2_allfactor_xjp_513_13','sta2_top2000_xjp_513_top2000_fact3_c20']
    
    twn_group_13 = ['pv13_2_minvol_1m_sector','pv13_20_minvol_1m_sector','pv13_10_minvol_1m_sector',
                    'pv13_5_minvol_1m_sector','pv13_10_f3_g2_minvol_1m_sector','pv13_5_f3_g2_minvol_1m_sector',
                    'pv13_2_f4_g3_minvol_1m_sector']
    
    twn_group_1 = ['sta1_allc50','sta1_allxjp_513_c50','sta1_allxjp_513_c20','sta1_allxjp_513_c2',
                   'sta1_allc20','sta1_allxjp_513_c5','sta1_allxjp_513_c10','sta1_allc2','sta1_allc5']
    
    twn_group_2 = ['sta2_allfactor_xjp_513_0','sta2_all_xjp_513_all_fact3_c20',
                   'sta2_all_xjp_513_all_fact4_c20','sta2_all_xjp_513_all_fact4_c50']
    
    usa_group_13 = ['pv13_h_min2_3000_sector','pv13_r2_min20_3000_sector','pv13_r2_min2_3000_sector',
                    'pv13_r2_min2_3000_sector', 'pv13_h_min2_focused_pureplay_3000_sector']
    
    usa_group_1 = ['sta1_top3000c50','sta1_allc20','sta1_allc10','sta1_top3000c20','sta1_allc5']
    
    usa_group_2 = ['sta2_top3000_fact3_c50','sta2_top3000_fact4_c20','sta2_top3000_fact4_c10']
    
    usa_group_6 = ['mdl10_group_name']
    
    asi_group_13 = ['pv13_20_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector', 'pv13_10_f3_g2_minvol_1m_sector',
                    'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_5_minvol_1m_sector']
    
    asi_group_1 = ['sta1_allc50', 'sta1_allc10', 'sta1_minvol1mc50','sta1_minvol1mc20',
                   'sta1_minvol1m_normc20', 'sta1_minvol1m_normc50']
    
    jpn_group_1 = ['sta1_alljpn_513_c5', 'sta1_alljpn_513_c50', 'sta1_alljpn_513_c2', 'sta1_alljpn_513_c20']
    
    jpn_group_2 = ['sta2_top2000_jpn_513_top2000_fact3_c20', 'sta2_all_jpn_513_all_fact1_c5',
                   'sta2_allfactor_jpn_513_9', 'sta2_all_jpn_513_all_fact1_c10']
    
    jpn_group_13 = ['pv13_2_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector',
                    'pv13_10_f3_g2_minvol_1m_sector', 'pv13_all_delay_1_parent', 'pv13_all_delay_1_level']
    
    kor_group_13 = ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_5_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector',
                    'pv13_2_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector']
    
    kor_group_1 = ['sta1_allc20','sta1_allc50','sta1_allc2','sta1_allc10','sta1_minvol1mc50',
                   'sta1_allxjp_513_c10', 'sta1_top2000xjp_513_c50']
    
    kor_group_2 =['sta2_all_xjp_513_all_fact1_c50','sta2_top2000_xjp_513_top2000_fact2_c50',
                  'sta2_all_xjp_513_all_fact4_c50','sta2_all_xjp_513_all_fact4_c5']
    
    eur_group_13 = ['pv13_5_sector', 'pv13_2_sector', 'pv13_v3_3l_scibr', 'pv13_v3_2l_scibr', 'pv13_2l_scibr',
                    'pv13_52_sector', 'pv13_v3_6l_scibr', 'pv13_v3_4l_scibr', 'pv13_v3_1l_scibr']
    
    eur_group_1 = ['sta1_allc10', 'sta1_allc2', 'sta1_top1200c2', 'sta1_allc20', 'sta1_top1200c10']
    
    eur_group_2 = ['sta2_top1200_fact3_c50','sta2_top1200_fact3_c20','sta2_top1200_fact4_c50']
    
    glb_group_13 = ["pv13_10_f2_g3_sector", "pv13_2_f3_g2_sector", "pv13_2_sector", "pv13_52_all_delay_1_sector"]
        
    glb_group_1 = ['sta1_allc20', 'sta1_allc10', 'sta1_allc50', 'sta1_allc5']
    
    glb_group_2 = ['sta2_all_fact4_c50', 'sta2_all_fact4_c20', 'sta2_all_fact3_c20', 'sta2_all_fact4_c10']
    
    glb_group_13 = ['pv13_2_sector', 'pv13_10_sector', 'pv13_3l_scibr', 'pv13_2l_scibr', 'pv13_1l_scibr',
                    'pv13_52_minvol_1m_all_delay_1_sector','pv13_52_minvol_1m_sector','pv13_52_minvol_1m_sector'] 
    
    amr_group_13 = ['pv13_4l_scibr', 'pv13_1l_scibr', 'pv13_hierarchy_min51_f1_sector',
                    'pv13_hierarchy_min2_600_sector', 'pv13_r2_min2_sector', 'pv13_h_min20_600_sector']
    
    bps_group = "bucket(rank(fnd28_value_05480), range='0.1, 1, 0.1')"
    pb_group = "bucket(rank(close/fnd28_value_05480), range='0.1, 1, 0.1')"
    cap_group = "bucket(rank(cap), range='0.1, 1, 0.1')"
    asset_group = "bucket(rank(assets),range='0.1, 1, 0.1')"
    sector_cap_group = "bucket(group_rank(cap, sector),range='0.1, 1, 0.1')"
    sector_asset_group = "bucket(group_rank(assets, sector),range='0.1, 1, 0.1')"

    vol_group = "bucket(rank(ts_std_dev(returns,20)),range = '0.1, 1, 0.1')"

    liquidity_group = "bucket(rank(close*volume),range = '0.1, 1, 0.1')"

    groups = ["market","sector", "industry", "subindustry",
              pb_group, bps_group, cap_group, asset_group, sector_cap_group, sector_asset_group, vol_group, liquidity_group]

    if region == "CHN":
        groups += chn_group_13 + chn_group_1 + chn_group_2  
    if region == "TWN":
        groups += twn_group_13 + twn_group_1 + twn_group_2 
    if region == "ASI":
        groups += asi_group_13 + asi_group_1 
    if region == "USA":
        groups += usa_group_13 + usa_group_1 + usa_group_2  
    if region == "HKG":
        groups += hkg_group_13 + hkg_group_1 + hkg_group_2 
    if region == "KOR":
        groups += kor_group_13 + kor_group_1 + kor_group_2 
    if region == "EUR": 
        groups += eur_group_13 + eur_group_1 + eur_group_2 
    if region == "GLB":
        groups += glb_group_13 + glb_group_1 + glb_group_2
    if region == "AMR":
        groups += amr_group_13 
    if region == "JPN":
        groups += jpn_group_1 + jpn_group_2 + jpn_group_13 
        
    for group in groups:
        if op.startswith("group_vector"):
            for vector in vectors:
                alpha = "%s(%s,%s,densify(%s))"%(op, field, vector, group)
                output.append(alpha)
        elif op.startswith("group_percentage"):
            alpha = "%s(%s,densify(%s),percentage=0.5)"%(op, field, group)
            output.append(alpha)
        else:
            alpha = "%s(%s,densify(%s))"%(op, field, group)
            output.append(alpha)
        
    return output