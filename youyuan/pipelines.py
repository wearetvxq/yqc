# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import pymongo
from pymongo import MongoClient

class YouyuanPipeline(object):
    def process_item(self, item, spider):
        coll = MongoClient('119.23.65.95', 27017)
        # coll1 = MongoClient('192.168.12.16', 27017)
        coll1 = MongoClient('192.168.12.16', 27017)
        coll.Insight.authenticate('flyminer', 'fm110707*')
        db = coll.Insight
        db1 = coll1.Insight
        # 分支机构
        # 分支机构

        total = db1.w_branch.count({'pid': item['_id']})
        if total == 0:
            for branch in item['branch']:
                db.w_branch.insert(branch)
                print('--------------------------')
                print(item['basics']['company'] + ' 分支机构线上插入成功！')
                print('--------------------------')
                db1.w_branch.insert(branch)
                print('--------------------------')
                print(item['basics']['company'] + ' 分支机构线上插入成功！')
                print('--------------------------')
        else:
            print('--------------------------')
            print(item['basics']['company'] + ' 分支机构已存在！')
            print('--------------------------')

        # 对外投资
        total = db1.w_was_cast.count({'pid': item['_id']})
        if total == 0:
            for invest in item['invest']:
                db.w_was_cast.insert(invest)
                print('--------------------------')
                print(item['basics']['company'] + ' 对外投资线上插入成功！')
                print('--------------------------')
                db1.w_was_cast.insert(invest)
                print('--------------------------')
                print(item['basics']['company'] + ' 对外投资本地插入成功！')
                print('--------------------------')
        else:
            print('--------------------------')
            print(item['basics']['company'] + ' 对外投资已存在！')
            print('--------------------------')

        # 融资历史
        total = db1.w_financing.count({'pid': item['_id']})
        if total == 0:
            for financehistory in item['financehistory']:
                db.w_financing.insert(financehistory)
                print('--------------------------')
                print(item['basics']['company'] + ' 融资历史线上插入成功！')
                print('--------------------------')
                db1.w_financing.insert(financehistory)
                print('--------------------------')
                print(item['basics']['company'] + ' 融资历史本地插入成功！')
                print('--------------------------')
        else:
            print('--------------------------')
            print(item['basics']['company'] + ' 融资历史已存在！')
            print('--------------------------')

        # 变更记录
        total = db1.w_change.count({'pid': item['_id']})
        if total == 0:
            for changeLogList in item['changeLogList']:
                db.w_change.insert(changeLogList)
                print('--------------------------')
                print(item['basics']['company'] + ' 变更记录线上插入成功！')
                print('--------------------------')
                db1.w_change.insert(changeLogList)
                print('--------------------------')
                print(item['basics']['company'] + ' 变更记录本地插入成功！')
                print('--------------------------')
        else:
            print('--------------------------')
            print(item['basics']['company'] + ' 变更记录已存在！')
            print('--------------------------')

        # 经营异常
        total = db1.w_operating_exception.count({'pid': item['_id']})
        if total == 0:
            for abnormalList in item['abnormalList']:
                db.w_operating_exception.insert(abnormalList)
                print('--------------------------')
                print(item['basics']['company'] + ' 经营异常线上插入成功！')
                print('--------------------------')
                db1.w_operating_exception.insert(abnormalList)
                print('--------------------------')
                print(item['basics']['company'] + ' 经营异常本地插入成功！')
                print('--------------------------')
        else:
            print('--------------------------')
            print(item['basics']['company'] + ' 经营异常已存在！')
            print('--------------------------')

        # 行政处罚
        total = db1.w_punish.count({'pid': item['_id']})
        if total == 0:
            for punishList in item['punishList']:
                db.w_punish.insert(punishList)
                print('--------------------------')
                print(item['basics']['company'] + ' 行政处罚线上插入成功！')
                print('--------------------------')
                db1.w_punish.insert(punishList)
                print('--------------------------')
                print(item['basics']['company'] + ' 行政处罚本地插入成功！')
                print('--------------------------')
        else:
            print('--------------------------')
            print(item['basics']['company'] + ' 行政处罚已存在！')
            print('--------------------------')

        # 动产抵押
        total = db1.w_mortgage.count({'pid': item['_id']})
        if total == 0:
            for mortgageList in item['mortgageList']:
                db.w_mortgage.insert(mortgageList)
                print('--------------------------')
                print(item['basics']['company'] + ' 动产抵押线上插入成功！')
                print('--------------------------')
                db1.w_mortgage.insert(mortgageList)
                print('--------------------------')
                print(item['basics']['company'] + ' 动产抵押线上插入成功！')
                print('--------------------------')
        else:
            print('--------------------------')
            print('已存在！')
            print('--------------------------')

        # 股权出质
        total = db1.w_equity.count({'pid': item['_id']})
        if total == 0:
            for equityList in item['equityList']:
                db.w_equity.insert(equityList)
                print('--------------------------')
                print(item['basics']['company'] + ' 股权出质线上插入成功！')
                print('--------------------------')
                db1.w_equity.insert(equityList)
                print('--------------------------')
                print(item['basics']['company'] + ' 股权出质本地插入成功！')
                print('--------------------------')
        else:
            print('--------------------------')
            print(item['basics']['company'] + ' 股权出质已存在！')
            print('--------------------------')

        return item