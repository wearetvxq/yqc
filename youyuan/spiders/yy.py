# -*- coding: utf-8 -*-
# import scrapy
# from scrapy.linkextractors import LinkExtractor
# #from scrapy.spiders import CrawlSpider, Rule
# from scrapy.spiders import Rule
# from scrapy_redis.spiders import RedisCrawlSpider
# from youyuan.items import YouyuanItem
# import re

from scrapy_redis.spiders import RedisSpider
import scrapy
import sys
import scrapy,requests,re,urllib,json,random,time
from pymongo import MongoClient
import logging as log

import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')




class YySpider(RedisSpider):
    name = 'yy'
    redis_key = "yyspider:start_urls"

    connect = MongoClient('192.168.12.16', 27017)
    db = connect.Insight.w_basics
    post = connect.Company_name.hunan

    connect1 = MongoClient('119.23.65.95', 27017)
    connect1.Insight.authenticate('flyminer', 'fm110707*')
    db1 = connect1.Insight.w_basics

    website = '--'

    headers = {"User-Agent": 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0'}

    industry_dict = {
        '电力、热力、燃气、水生产、供应业': '电力、热力、燃气、水生产和供应业',
        '燃气生产、供应业': '燃气生产和供应业',
        '电力、热力生产、供应业': '电力、热力生产和供应业',
        '水的生产、供应业': '水的生产和供应业',
        '建筑装饰、其他建筑业': '建筑装饰和其他建筑业',
        '批发、零售业': '批发和零售业',
        '交通运输、仓储、邮政业': '交通运输、仓储和邮政业',
        '装卸搬运、运输代理业': '装卸搬运和运输代理业',
        '煤炭开采、洗选业': '煤炭开采和洗选业',
        '石油、天然气开采业': '石油和天然气开采业',
        '计算机、通信、其他电子设备制造业': '计算机、通信和其他电子设备制造业',
        '铁路、船舶、航空航天、其他运输设备制造业': '铁路、船舶、航空航天和其他运输设备制造业',
        '电气机械、器材制造业': '电气机械和器材制造业',
        '金属制品、机械、设备修理业': '金属制品、机械和设备修理业',
        '造纸、纸制品业': '造纸和纸制品业',
        '印刷、记录媒介复制业': '印刷和记录媒介复制业',
        '文教、工美、体育、娱乐用品制造业': '文教、工美、体育和娱乐用品制造业',
        '石油加工、炼焦、核燃料加工业': '石油加工、炼焦、核燃料加工业',
        '化学原料、化学制品制造业': '化学原料和化学制品制造业',
        '橡胶、塑料制品业': '橡胶和塑料制品业',
        '有色金属冶炼、压延加工业': '有色金属冶炼和压延加工业',
        '黑色金属冶炼、压延加工业': '黑色金属冶炼和压延加工业',
        '皮革、毛皮、羽毛及其制品、制鞋业': '皮革、毛皮、羽毛及其制品和制鞋业',
        '酒、饮料、精制茶制造业': '酒、饮料和精制茶制造业',
        '木材加工和、木、竹、藤、棕、草制品业': '木材加工、木、竹、藤、棕、草制品业',
        '租赁、商务服务业': '租赁和商务服务业',
        '科学研究、技术服务': '科学研究和技术服务',
        '水利、环境、公共设施管理业': '水利、环境和公共设施管理业',
        '居民服务、修理、其他服务业': '居民服务、修理和其他服务业',
        '住宿、餐饮业': '住宿和餐饮业',
        '信息传输、软件、信息技术服务业': '信息传输、软件和信息技术服务业',
        '卫生、社会工作': '卫生和社会工作',
        '公共管理、社会保障、社会组织': '公共管理、社会保障和社会组织',
        '文化、体育和娱乐业': '文化、体育、娱乐业',
        '研究、试验发展': '研究和试验发展',
        '科技推广、应用服务业': '科技推广和应用服务业',
        '生态保护、环境治理业': '生态保护和环境治理业',
        '机动车、电子产品、日用产品修理业': '机动车、电子产品和日用产品修理业',
        '互联网、相关服务': '互联网和相关服务',
        '软件、信息技术服务业': '软件和信息技术服务业',
        '电信、广播电视、卫星传输服务': '电信、广播电视和卫星传输服务',
        '群众团体、社会团体、其他成员组织': '群众团体、社会团体和其他成员组织',
        '科学研究、技术服务业': '科学研究和技术服务业',
        '文化、体育、娱乐业': '文化、体育和娱乐业',
        '广播、电视、电影、影视录音制作业': '广播、电视、电影和影视录音制作业',
        '新闻、出版业': '新闻和出版业',
        '皮革、毛皮、羽毛、其制品、制鞋业': '皮革、毛皮、羽毛、其制品和制鞋业',
        '石油加工、炼焦、核燃料加工业': '石油加工、炼焦、核燃料加工业',
        '木材加工、木、竹、藤、棕、草制品业': '木材加工和木、竹、藤、棕、草制品业',
        '农、林、牧、渔业': '农、林、牧、渔业',
        '农、林、牧、渔服务业': '农、林、牧、渔服务业'

    }

    import json


    # JSONEncoder().encode(analytics)
    # 百度地图api调用
    def baidu_map(self, address):
        if address != '0':
            url1 = 'http://api.map.baidu.com/geocoder/v2/?address=' + address + '&output=json&ak=gRManfxm4xGfswhaIT4xGh78UpHV8kCV'
            r = requests.get(url1)
            r.status_code
            result = json.loads(r.text)
            x = result['result']['location']['lng']
            y = result['result']['location']['lat']
            url2 = 'http://api.map.baidu.com/geocoder/v2/?location=' + str(y) + ',' + str(
                x) + '&output=json&pois=0&ak=gRManfxm4xGfswhaIT4xGh78UpHV8kCV'
            r1 = requests.get(url2)
            result1 = json.loads(r1.text)
            prov = result1['result']['addressComponent']['province']
            city = result1['result']['addressComponent']['city']
            area = result1['result']['addressComponent']['district']
            return (prov, city)

    # # 动态域范围获取
    # def __init__(self, *args, **kwargs):
    #     # Dynamically define the allowed domains list.
    #     domain = kwargs.pop('domain', '')
    #     self.allowed_domains = filter(None, domain.split(','))
    #     super(YySpider, self).__init__(*args, **kwargs)

#start_url 给的是一个查询url
    def parse(self, response):

        # data = response.text

        data = json.loads(response.text)
        try:
            messages = data['data']['corps']
            # print(messages)

            if messages != None:
                for message in messages:
                    code = message['corpKey']
                    # print(code)
                    url = 'http://m.eqicha.com/yqcapp/api/corp/companyWebsite?request=%7B%22corpKey%22:%22{}%22%7D'.format(
                        code)
                    code1 = {
                        'code': code,

                    }

                    code = self.connect.WebCode.hunan
                    code.insert(code1)
                    # yield self.content_parse(url)
                    # a=self.content_parse(url)
                    try:
                        yield scrapy.Request(url=url, callback=self.content_parse)

                    except :
                        pass

        except:
            print(response.text)
    def content_parse(self, response):
        print('****************************************')

        # num = 0
        # for i in self.post.find({}):
        #     num += 1
        #     if num >= 1:
        #         print(num)
        #         print('----------------------')
        #
        #         company_name1 = i['company']
        #         # url编码
        #         company_name1 = urllib.request.quote(company_name1)
        #         yield scrapy.Request(url='http://m.eqicha.com/yqcapp/api/corp/h5/searchList?request=%7B%22searchKey%22:%22{}%22%7D'.format(company_name1), callback=self.parse)
        #
        #     if num >= 701187:
        #         break
        # print('****************************************')













        data = json.loads(response.text)

        data = data['data']



        info={}


        # 工商
        businessInfo={}
        businessInfo['company']=data['businessInfo']['corpName']

        company_name1 = businessInfo['company']
        company_name1 = urllib.request.quote(company_name1)
        # 会有公司名

        businessInfo['englishName']='0'
        businessInfo['logo']='0'
        businessInfo['browse']=str(data['browseCount'])
        try:
            businessInfo['creditCode']=data['businessInfo']['corpCreditCode']
        except:
            businessInfo['creditCode']='未公示'
        try:
            businessInfo['regNumber']=data['businessInfo']['corpRegNo']
        except:
            businessInfo['regNumber']='未公示'
        try:
            businessInfo['type']=data['businessInfo']['corpEconKind']
        except:
            businessInfo['type']='未公示'
        try:
            for i in self.db.find({'company':businessInfo['company']}):
                self.website=i['website']
        except:
            self.website = '--'
        # print(self.website)

        try:
            industry=data['businessInfo']['corpIndustry'].split(';')
            businessInfo['industry']=industry[1]
        except:
            businessInfo['industry']='0'
        try:
            businessInfo['classify'] = industry[0]
        except:
            businessInfo['classify']='0'

        if businessInfo['industry'].find('、')!=-1:

            businessInfo['industry']=self.industry_dict[businessInfo['industry']]
        if businessInfo['classify'].find('、')!=-1:
            businessInfo['classify']=self.industry_dict[businessInfo['classify']]
        try:
            businessInfo['registeredTime']=data['businessInfo']['corpStartDate'].replace('年','-').replace('月','-').replace('日','')
        except:
            businessInfo['registeredTime']=='未公示'
        try:
            End=data['businessInfo']['corpTeamEnd']
        except:
            End='未公示'
        try:
            Start=data['businessInfo']['corpTermStart']
        except:
            Start='未公示'


        businessInfo['operatingPeriod']=str(Start)+'到'+str(End)
        if businessInfo=='未公示到未公示':
            businessInfo['operatingPeriod']='未公示'
        try:
            businessInfo['regAuthority']=data['businessInfo']['corpBelongOrg']
        except:
            businessInfo['regAuthority']='未公示'
        try:
            businessInfo['regAddress']=data['businessInfo']['corpAddress']
        except:
            businessInfo['regAddress']='未公示'
        try:
            businessInfo['range']=data['businessInfo']['corpScope']
        except:
            businessInfo['range']='未公示'
        try:
            businessInfo['phone']=data['companyDetailCount']['companyDetail']['phoneNumber']
        except:
            businessInfo['phone']='未公示'
        try:
            businessInfo['mail']=data['companyDetailCount']['companyDetail']['email']
        except:
            businessInfo['mail'] ='未公示'
        businessInfo['website']=self.website
        businessInfo['regStatus']=data['companyDetailCount']['companyDetail']['regStatus']
        try:
            businessInfo['capital']=data['companyDetailCount']['companyDetail']['regCapital']
        except:
            businessInfo['capital']='未公示'
        businessInfo['score']='0'
        try:
            businessInfo['taxpayer']=businessInfo['regNumber']
        except:
            businessInfo['taxpayer'] =='未公示'
        try:
            businessInfo['legalPerson']=data['companyDetailCount']['companyDetail']['legalPersonName']
        except:
            businessInfo['legalPerson'] ='未公示'
        try:
            businessInfo['approvedDate']=data['businessInfo']['corpCheckDate'].replace('年','-').replace('月','-').replace('日','')
        except:
            businessInfo['approvedDate'] ='未公示'
        businessInfo['introduction']='0'

        try:
            (businessInfo['province'], businessInfo['city']) = self.baidu_map(businessInfo['regAddress'])
        except:
            businessInfo['province'] = '0'
            businessInfo['city'] = '0'

        if businessInfo['province'] == '0':
            try:
                (businessInfo['province'], businessInfo['city']) = self.baidu_map(businessInfo['company'])
            except:
                businessInfo['province'] = '0'
                businessInfo['city'] = '0'

        print(businessInfo['province'])
        if ('湖南' not in businessInfo['province'])and('湖北' not in businessInfo['province'])and('北京' not in businessInfo['province']):
            return


        businessInfo['MainStaff'] = []
        MainStaffs=data['mainStaff']['list']
        if MainStaffs==[]:
            businessInfo['MainStaff']=[]
        else:
            for MainStaff in MainStaffs:
                mainstaff={}
                mainstaff['username']=MainStaff['name']
                mainstaff['position']=MainStaff['staffTypeName']
                businessInfo['MainStaff'].append(mainstaff)
                # print(mainstaff)



        businessInfo['shareholder']=[]
        try:
            shareholders=data['partnerInfo']['list']
        except:
            shareholders=[]
        if shareholders==[]:
            businessInfo['MainStaff']=[]
        else:
            for shareholder in shareholders:
                holder={}
                holder['name']=shareholder['chName']
                try:
                    share=eval(shareholder['chShouldCapi'].replace('[', '').replace(']', ''))
                    # print(type(share))
                except:
                    chShouldCapi='未公示'
                try:
                    holder['proportion']=shareholder['proportion']
                except:
                    holder['proportion']='未公示'
                try:
                    holder['amount']=share['amomon']+shareholder['monetaryUnit']
                except:
                    try:
                        holder['amount'] = chShouldCapi
                    except:
                        holder['amount'] ='未公示'
                try:
                    holder['posttime']=share['time']
                except:
                    holder['posttime']='未公示'
                businessInfo['shareholder'].append(holder)
                # print(holder)

        businessInfo['companyId']='0'
        businessInfo['posttime']=int(time.time())
        print('----------------')
        # print(len(businessInfo))
        info['basics'] = businessInfo
        print(info['basics'])
        #
        # for key in businessInfo:
        #     print(key)

        total = self.db.count({'company': businessInfo['company']})
        if total==0:
            self.db.insert(info['basics'])
            for i in self.db.find({'company':businessInfo['company']}):
                self.db1.insert(i)
            print(businessInfo['company'] + ' 插入成功！')
        if total!=0:
            self.db.update({"company": businessInfo['company']}, {'$set': info['basics']})
            self.db1.update({"company": businessInfo['company']}, {'$set': info['basics']})
            print(businessInfo['company']+' 更新成功！')

        for i in self.db.find({'company':businessInfo['company']}):
            mongodb_id=i['_id']

        print(mongodb_id)
        info['_id']=mongodb_id



        # 分支机构
        branch=[]
        branchOffices=data['branchOffice']['list']
        if branchOffices==[]:
            branch=[]
        else:
            for branchOffice in branchOffices:
                # print(branchOffice)
                branch_info={}
                try:
                    branch_info['name']=branchOffice['corpName']
                except:
                    branch_info['name']='0'
                try:
                    branch_info['legalPerson']=branchOffice['legalPersonName']
                except:
                    branch_info['legalPerson'] ='0'

                branch_info['pid']=mongodb_id
                try:
                    branch_info['status']=branchOffice['regStatus']
                except:
                    branch_info['status'] ='0'
                try:
                    branch_info['posttime']=branchOffice['estiblishTime']
                except:
                    branch_info['posttime'] ='0'
                branch.append(branch_info)

            # print(branch)
        info['branch']=branch

        # 对外投资
        invest=[]
        try:
            foreignInvests=data['foreignInvest']['list']
            if foreignInvests==[]:
                invest=[]
            else:
                for foreignInvest in foreignInvests:
                    # print(foreignInvest)
                    invest_info={}
                    invest_info['pid']=mongodb_id
                    try:
                        invest_info['name']=foreignInvest['corpName']
                    except:
                        invest_info['name'] ='0'
                    try:
                        invest_info['legalPerson']=foreignInvest['legalPerson']
                    except:
                        invest_info['legalPerson']='0'
                    try:
                        invest_info['capital']=str(int(foreignInvest['investMoney']))
                        if invest_info['capital']!='0':
                            invest_info['capital']=invest_info['capital']+'万元人民币'
                        else:
                            invest_info['capital']='未公示'
                    except:
                        invest_info['capital']='未公示'

                    invest.append(invest_info)

        except:
            invest = []
        info['invest'] = invest
        # print(info['invest'])



        # 融资历史
        financehistory=[]
        try:
            financInfos=data['financInfo']['corpFincingResps']
            for financInfo in financInfos:
                financehistory_info={}
                financehistory_info['pid']=mongodb_id
                try:
                    financehistory_info['posttime']=financInfo['investTime']
                except:
                    financehistory_info['posttime'] ='未公示'
                try:
                    financehistory_info['rounds']=financInfo['investRound']
                except:
                    financehistory_info['rounds']='未公示'

                financehistory_info['valuation']='未公示'
                try:
                    financehistory_info['amount']=financInfo['investAmt']
                except:
                    financehistory_info['amount'] ='未公示'
                financehistory_info['proportion']='未公示'
                financehistory_info['investor']='未公示'
                financehistory_info['source']='未公示'
                financehistory.append(financehistory_info)

        except:
            financehistory = []

        info['financehistory']=financehistory
        # print(info['financehistory'])

        # 变更记录
        changeLogList=[]
        try:
            changeLogs=data['changeLog']['list']
            for changeLog in changeLogs:
                changeLogList_info={}
                # print(changeLog)
                changeLogList_info['pid']=mongodb_id
                try:
                    changeLogList_info['posttime']=changeLog['changeTime']
                except:
                    changeLogList_info['posttime']='未公示'
                try:
                    changeLogList_info['project']=changeLog['changeItem']
                except:
                    changeLogList_info['project'] ='未公示'
                try:
                    changeLogList_info['before']=changeLog['contentBefore']
                except:
                    changeLogList_info['before'] ='未公示'
                try:
                    changeLogList_info['after'] = changeLog['contentAfter']
                except:
                    changeLogList_info['after'] = '未公示'
                changeLogList.append(changeLogList_info)
        except:
            changeLogList = []

        info['changeLogList']=changeLogList
        # print(info['changeLogList'])

        # 竞品信息
        competition=[]
        info['competition']=competition

        # 招聘信息
        recruitList = []
        info['recruitList']=recruitList

        # 经营异常
        abnormalList = []
        try:
            operaAbnors=data['operaAbnor']['list']
            for operaAbnor in operaAbnors:
                # print(operaAbnor)
                abnormalList_info={}
                abnormalList_info['pid']=mongodb_id
                try:
                    timestamp=int(operaAbnor['putDate'])/1000
                    time_local = time.localtime(timestamp)
                    dt = time.strftime("%Y-%m-%d", time_local)
                    abnormalList_info['posttime']=dt
                except:
                    abnormalList_info['posttime']='未公示'
                try:
                    abnormalList_info['reason']=operaAbnor['putReason']
                except:
                    abnormalList_info['reason'] ='未公示'
                try:
                    abnormalList_info['organ']=operaAbnor['putDepartment']
                except:
                    abnormalList_info['organ'] ='未公示'

                abnormalList.append(abnormalList_info)

        except:
            abnormalList = []

        info['abnormalList']=abnormalList
        # print(info['abnormalList'])

        # 行政处罚
        punishList = []
        try:
            adminPenals=data['adminPenal']['list']
            # print(adminPenals)
            for adminPenal in adminPenals:
                # print(adminPenal)
                punishList_info={}
                try:
                    timestamp = int(adminPenal['decisionDate']) / 1000
                    time_local = time.localtime(timestamp)
                    dt = time.strftime("%Y-%m-%d", time_local)
                    punishList_info['posttime'] = dt

                except:
                    punishList_info['posttime'] ='未公示'

                try:
                    punishList_info['number']=adminPenal['punishNumber']
                except:
                    punishList_info['number']='未公示'

                try:
                    punishList_info['type']=adminPenal['type']
                except:
                    punishList_info['type']='未公示'

                try:
                    punishList_info['organ']=adminPenal['departmentName']
                except:
                    punishList_info['organ']='未公示'

                try:
                    punishList_info['content']=adminPenal['content']
                except:
                    punishList_info['content']='未公示'

                punishList_info['pid']=mongodb_id
                punishList.append(punishList_info)

        except:
            punishList = []
        info['punishList']=punishList
        # print(info['punishList '])

        # 动产抵押
        mortgageList = []
        try:
            chattels=data['chattel']['list']
            for chattel in chattels:
                # print(chattel)
                mortgageList_info={}
                mortgageList_info['pid']=mongodb_id
                try:
                    mortgageList_info['number']=chattel['regNum']
                except:
                    mortgageList_info['number'] ='未公示'
                try:
                    mortgageList_info['type']=chattel['type']
                except:
                    mortgageList_info['type']='未公示'
                try:
                    mortgageList_info['organ']=chattel['regDepartment']
                except:
                    mortgageList_info['organ']='未公示'
                try:
                    mortgageList_info['status']=chattel['status']
                except:
                    mortgageList_info['status']='未公示'
                try:
                    mortgageList_info['amount']=chattel['amount']
                except:
                    mortgageList_info['amount']='未公示'
                try:
                    mortgageList_info['range']=chattel['scope']
                except:
                    mortgageList_info['range']='未公示'
                try:
                    timestamp = int(chattel['regDate']) / 1000
                    time_local = time.localtime(timestamp)
                    dt = time.strftime("%Y-%m-%d", time_local)
                    mortgageList_info['date']=dt
                except:
                    mortgageList_info['date'] ='未公示'
                mortgageList_info['peopleName']='未公示'
                mortgageList_info['pawnInfoList']=[]
                mortgageList_info['changeInfoList']=[]
                mortgageList.append(mortgageList_info)
        except:
            mortgageList = []
        info['mortgageList']=mortgageList
        # print(len(mortgageList_info))
        # print(info['mortgageList'])

        # 股权出质
        equityList=[]
        try:
            equitys=data['equity']['list']
            for equity in equitys:
                # print(equity)
                equityList_info={}
                equityList_info['pid']=mongodb_id
                try:
                    timestamp = int(equity['regDate']) / 1000
                    time_local = time.localtime(timestamp)
                    dt = time.strftime("%Y-%m-%d", time_local)
                    equityList_info['posttime']=dt
                except:
                    equityList_info['posttime']='未公示'
                try:
                    equityList_info['number']=equity['regNumber']
                except:
                    equityList_info['number']='未公示'
                try:
                    equityList_info['person']=equity['pledgor']
                except:
                    equityList_info['person'] ='未公示'
                try:
                    equityList_info['pledgee']=equity['pledgee']
                except:
                    equityList_info['pledgee']='未公示'
                try:
                    equityList_info['status']=equity['status']
                except:
                    equityList_info['status'] ='未公示'
                try:
                    equityList_info['amount']=equity['equityAmount']
                except:
                    equityList_info['amount'] ='未公示'
                try:
                    equityList_info['certifNumber']=equity['certifNumberL']
                except:
                    equityList_info['certifNumber'] ='未公示'
                try:
                    equityList_info['pledgeeNumber']=equity['certifNumberR']
                except:
                    equityList_info['pledgeeNumber'] ='未公示'
                equityList.append(equityList_info)

        except:
            equityList = []
        info['equityList']=equityList
        # print(len(equityList_info))
        # print(info['equityList'])

        # # 法院公告
        # courtList = []
        # try:
        #     courtAnnouns=data['courtAnnoun']['list']
        #     for courtAnnoun in courtAnnouns:
        #         # print(courtAnnoun)
        #         courtList_info={}
        #         courtList_info['pid']=mongodb_id
        #         courtList_info['appeal']=courtAnnoun['party1']
        #         courtList_info['sued']=courtAnnoun['party2']
        #         courtList_info['type']=courtAnnoun['bltntypename']
        #         courtList_info['court']=courtAnnoun['courtcode']
        #         courtList_info['']
        #
        # except:
        #
        #     courtList = []


        print(info)
        yield info

        yield scrapy.Request(url='http://m.eqicha.com/yqcapp/api/corp/h5/searchList?request=%7B%22searchKey%22:%22{}%22%7D'.format(company_name1), callback=self.parse)