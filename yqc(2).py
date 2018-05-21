# -*- coding: utf-8 -*-
import requests,re,urllib,json,random,time
from pymongo import MongoClient
import  pymysql


class YqcSpider():
    name = 'yqc'
    start_urls = ['https://m.eqicha.com/']


    connect1=MongoClient('119.23.65.95',27017)
    connect1.Insight.authenticate('flyminer', 'fm110707*')
    db1=connect1.Insight.w_basics
    post1=connect1.Insight



    website='--'

    headers = {"User-Agent": 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0'}

    industry_dict={
        '电力、热力、燃气、水生产、供应业':'电力、热力、燃气、水生产和供应业',
        '燃气生产、供应业':'燃气生产和供应业',
        '电力、热力生产、供应业':'电力、热力生产和供应业',
        '水的生产、供应业':'水的生产和供应业',
        '建筑装饰、其他建筑业':'建筑装饰和其他建筑业',
        '批发、零售业':'批发和零售业',
        '交通运输、仓储、邮政业':'交通运输、仓储和邮政业',
        '装卸搬运、运输代理业':'装卸搬运和运输代理业',
        '煤炭开采、洗选业':'煤炭开采和洗选业',
        '石油、天然气开采业':'石油和天然气开采业',
        '计算机、通信、其他电子设备制造业':'计算机、通信和其他电子设备制造业',
        '铁路、船舶、航空航天、其他运输设备制造业':'铁路、船舶、航空航天和其他运输设备制造业',
        '电气机械、器材制造业':'电气机械和器材制造业',
        '金属制品、机械、设备修理业':'金属制品、机械和设备修理业',
        '造纸、纸制品业':'造纸和纸制品业',
        '印刷、记录媒介复制业':'印刷和记录媒介复制业',
        '文教、工美、体育、娱乐用品制造业':'文教、工美、体育和娱乐用品制造业',
        '石油加工、炼焦、核燃料加工业':'石油加工、炼焦、核燃料加工业',
        '化学原料、化学制品制造业':'化学原料和化学制品制造业',
        '橡胶、塑料制品业':'橡胶和塑料制品业',
        '有色金属冶炼、压延加工业':'有色金属冶炼和压延加工业',
        '黑色金属冶炼、压延加工业':'黑色金属冶炼和压延加工业',
        '皮革、毛皮、羽毛及其制品、制鞋业':'皮革、毛皮、羽毛及其制品和制鞋业',
        '酒、饮料、精制茶制造业':'酒、饮料和精制茶制造业',
        '木材加工和、木、竹、藤、棕、草制品业':'木材加工、木、竹、藤、棕、草制品业',
        '租赁、商务服务业':'租赁和商务服务业',
        '科学研究、技术服务':'科学研究和技术服务',
        '水利、环境、公共设施管理业':'水利、环境和公共设施管理业',
        '居民服务、修理、其他服务业':'居民服务、修理和其他服务业',
        '住宿、餐饮业':'住宿和餐饮业',
        '信息传输、软件、信息技术服务业':'信息传输、软件和信息技术服务业',
        '卫生、社会工作':'卫生和社会工作',
        '公共管理、社会保障、社会组织':'公共管理、社会保障和社会组织',
        '文化、体育和娱乐业':'文化、体育、娱乐业',
        '研究、试验发展':'研究和试验发展',
        '科技推广、应用服务业':'科技推广和应用服务业',
        '生态保护、环境治理业':'生态保护和环境治理业',
        '机动车、电子产品、日用产品修理业':'机动车、电子产品和日用产品修理业',
        '互联网、相关服务':'互联网和相关服务',
        '软件、信息技术服务业':'软件和信息技术服务业',
        '电信、广播电视、卫星传输服务':'电信、广播电视和卫星传输服务',
        '群众团体、社会团体、其他成员组织':'群众团体、社会团体和其他成员组织',
        '科学研究、技术服务业':'科学研究和技术服务业',
        '文化、体育、娱乐业':'文化、体育和娱乐业',
        '广播、电视、电影、影视录音制作业':'广播、电视、电影和影视录音制作业',
        '新闻、出版业':'新闻和出版业',
        '皮革、毛皮、羽毛、其制品、制鞋业':'皮革、毛皮、羽毛、其制品和制鞋业',
        '石油加工、炼焦、核燃料加工业':'石油加工、炼焦、核燃料加工业',
        '木材加工、木、竹、藤、棕、草制品业':'木材加工和木、竹、藤、棕、草制品业',
        '农、林、牧、渔业':'农、林、牧、渔业',
        '农、林、牧、渔服务业':'农、林、牧、渔服务业'

    }


    # 百度地图api调用
    def baidu_map(self,address):
        if address != '0':
            url1 = 'https://api.map.baidu.com/geocoder/v2/?address=' + address + '&output=json&ak=gRManfxm4xGfswhaIT4xGh78UpHV8kCV'
            r = requests.get(url1)
            r.status_code
            result = json.loads(r.text)
            x = result['result']['location']['lng']
            y = result['result']['location']['lat']
            url2 = 'https://api.map.baidu.com/geocoder/v2/?location=' + str(y) + ',' + str(x) + '&output=json&pois=0&ak=gRManfxm4xGfswhaIT4xGh78UpHV8kCV'
            r1 = requests.get(url2)
            result1 = json.loads(r1.text)
            prov = result1['result']['addressComponent']['province']
            city = result1['result']['addressComponent']['city']
            area = result1['result']['addressComponent']['district']
            return (prov,city)

    def parse(self,company_name):
        # url编码
        company_name=urllib.request.quote(company_name)
        url='https://m.eqicha.com/yqcapp/api/corp/h5/searchList?request=%7B%22searchKey%22:%22{}%22%7D'.format(company_name)
        r=requests.get(url,headers=self.headers)
        data=json.loads(r.text)
        messages=data['data']['corps']
        # print(messages)
        if messages!=None:
            for message in messages:
                code=message['corpKey']
                # print(code)
                url='https://m.eqicha.com/yqcapp/api/corp/companyWebsite?request=%7B%22corpKey%22:%22{}%22%7D'.format(code)
                information= self.content_parse(url)
                self.post_data(information)
                break



    def content_parse(self,url):
        r=requests.get(url,headers=self.headers)
        data = json.loads(r.text)
        data=data['data']
        # print(data)
        # informations=[]
        info={}


        # 工商信息
        businessInfo={}
        businessInfo['company']=data['businessInfo']['corpName']
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
            for i in self.db1.find({'company':businessInfo['company']}):
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

        info['basics'] = businessInfo


        total = self.db1.count({'company': businessInfo['company']})
        if total==0:
            self.db1.insert(info['basics'])
            # print(businessInfo['company'] + ' 插入成功！')
        if total!=0:
            self.db1.update({"company": businessInfo['company']}, {'$set': info['basics']})
            # print(businessInfo['company']+' 更新成功！')

        for i in self.db1.find({'company':businessInfo['company']}):
            # print(i)
            mongodb_id=i['_id']

        # print(mongodb_id)
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
                    mortgageList_info['date'] ='0'
                mortgageList_info['peopleName']='未公示'
                mortgageList_info['pawnInfoList']=[]
                mortgageList_info['changeInfoList']=[]
                mortgageList.append(mortgageList_info)
        except:
            mortgageList = []
        info['mortgageList']=mortgageList


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

        return info


    def post_data(self,item):

        coll1 = MongoClient('119.23.65.95', 27017)
        coll1.Insight.authenticate('flyminer', 'fm110707*')
        db1 = coll1.Insight
        # 分支机构
        if item['branch']!=[]:
            total = db1.w_branch.count({'pid': item['_id']})
            if total == 0:
                for branch in item['branch']:

                    db1.w_branch.insert(branch)

            else:
                pass


        # 对外投资
        if item['invest']!=[]:
            total = db1.w_was_cast.count({'pid': item['_id']})
            if total == 0:
                for invest in item['invest']:

                    db1.w_was_cast.insert(invest)

            else:
                pass


        # 融资历史
        if item['financehistory'] != []:
            total = db1.w_financing.count({'pid': item['_id']})
            if total == 0:
                for financehistory in item['financehistory']:

                    db1.w_financing.insert(financehistory)

            else:
                pass

        # 变更记录
        if item['changeLogList'] != []:
            total = db1.w_change.count({'pid': item['_id']})
            # print(total)
            if total == 0:
                for changeLogList in item['changeLogList']:
                    db1.w_change.insert(changeLogList)
                    # print('成功插入！')

            else:
                pass


        # 经营异常
        if item['abnormalList'] != []:
            total = db1.w_operating_exception.count({'pid': item['_id']})
            if total == 0:
                for abnormalList in item['abnormalList']:


                    db1.w_operating_exception.insert(abnormalList)

            else:
                pass


        # 行政处罚
        if item['punishList'] != []:
            total = db1.w_punish.count({'pid': item['_id']})
            if total == 0:
                for punishList in item['punishList']:

                    db1.w_punish.insert(punishList)

            else:
                pass


        # 动产抵押
        if item['mortgageList'] != []:
            total = db1.w_mortgage.count({'pid': item['_id']})
            if total == 0:
                for mortgageList in item['mortgageList']:

                    db1.w_mortgage.insert(mortgageList)

            else:
                pass


        # 股权出质
        if item['equityList'] != []:
            total = db1.w_equity.count({'pid': item['_id']})
            if total == 0:
                for equityList in item['equityList']:

                    db1.w_equity.insert(equityList)

            else:
                pass

        # print(item)
        mongodb_id=item['_id']
        # print(mongodb_id)
        self.elasticsearch(item,mongodb_id)

    def elasticsearch(self,data,mongodb_id):
        coll = MongoClient('119.23.65.95', 27017)
        coll.Insight.authenticate('flyminer', 'fm110707*')
        db = coll.Insight

        conn = pymysql.Connect(host='119.23.65.95', port=3306, user='root', passwd='fm110707*', db='Insight',charset='utf8')
        cursor = conn.cursor()
        sql = "select * from i_region"  # i_reginon这个表  ,还有i_industry行业表
        sql1 = "select * from i_industry"
        cursor.execute(sql)
        results1 = cursor.fetchall()
        prov = []
        area = []
        diqu = []
        one_level = []
        two_level = []
        three_level = []
        for i in results1:
            city = {}
            if i[3] == 1:
                city[i[0]] = i[2]
                prov.append(city)
            for x in prov:
                for key in x.keys():
                    acity = {}
                    if key == int(i[3]):
                        acity[i[2]] = int(i[0])
                        area.append(acity)
            for y in area:
                for value in y.values():
                    adiqu = {}
                    if value == int(i[3]):
                        adiqu[i[2]] = int(i[0])
                        diqu.append(adiqu)
        cursor.execute(sql1)
        results = cursor.fetchall()
        for i in results:
            one = {}
            if i[2] == 1:
                one[i[0]] = i[1]
                one_level.append(one)
            for m in one_level:
                for n in m.keys():
                    two = {}
                    three = {}
                    if i[3] == n:
                        two[i[3]] = i[1]
                        two_level.append(two)
                        three[i[0]] = i[1]
                        three_level.append(three)
        conn.close()

        posttime = data['basics']['posttime']
        mongo_id = mongodb_id
        company = data['basics']['company']
        legalPerson = str(data['basics']['legalPerson'])
        amount = data['basics']['capital'].replace(',', '')
        try:
            amount_number = str(float(re.findall(r'.*万', amount)[0][:-1]) * 10000)
        except:
            amount_number = '未公开'

        url = 'http://api.map.baidu.com/geocoder/v2/?address=' + data['basics'][
            'company'] + '&output=json&ak=gRManfxm4xGfswhaIT4xGh78UpHV8kCV'
        r = requests.get(url)
        result = json.loads(r.text)
        x = result['result']['location']['lng']
        y = result['result']['location']['lat']
        url1 = 'http://api.map.baidu.com/geocoder/v2/?location=' + str(y) + ',' + str(
            x) + '&output=json&pois=0&ak=gRManfxm4xGfswhaIT4xGh78UpHV8kCV'
        r1 = requests.get(url1)
        result1 = json.loads(r1.text)

        i_province = result1['result']['addressComponent']['province']
        i_city = result1['result']['addressComponent']['city']
        i_area = result1['result']['addressComponent']['district']

        status = data['basics']['regStatus']
        regTime = data['basics']['registeredTime']
        phone = data['basics']['phone']
        mail = data['basics']['mail']
        if mail == '':
            mail = '0'
        website = data['basics']['website']
        address = data['basics']['regAddress']
        score = data['basics']['score']
        # 工商信息
        regNumber = data['basics']['regNumber']
        code = data['basics']['creditCode']
        number = data['basics']['creditCode']
        type = data['basics']['type']
        industry = data['basics']['industry']
        estiblish = data['basics']['operatingPeriod']
        approved = data['basics']['approvedDate']
        if approved == '未公开':
            approved = '0'
        organ = data['basics']['regAuthority']
        english = data['basics']['englishName']
        scope = data['basics']['range']
        if scope == "":
            scope = '0'
        # 省会对应的id
        prov_id = "0",
        try:
            for m in prov:
                for n in m.keys():
                    if m[n].find(i_province) != -1:
                        prov_id = str(n)
                        # print(prov_id)
        except:
            pass
        # 这里是判断市级，如北京，重庆这种
        # print(prov_id)


        try:
            for m in area:
                for n in m.keys():

                    if n.find(i_city) != -1:
                        city_id = m[n]
                        # print(city_id)
        except:
            pass

        if prov_id=='2':
            city_id='2'

        if '北京分公司' or '北京分行' or'（北京）' or '(北京)'  in company:
            prov_id == '2'
            city_id = '2'


        # 地区对应id
        # 这里匹配长沙县，开福区这些地址
        try:
            for m in diqu:
                for n in m.keys():
                    if n.find(i_area) != -1:
                        area_id = m[n]
        except:
            pass
        # area_id = str(area_id)

        if address.find('浏阳市')!=-1:
            i_province='湖南'
            i_city='长沙'
            i_area='浏阳市'
            prov_id='19'
            city_id='218'
            area_id='2181'

        # 行业对应ID
        industry_id = "0"
        try:
            for va in one_level:
                for m in va.values():
                    if m.find(industry) != -1:
                        for n in va.keys():
                            industry_id = n
        except:
            pass
        try:
            for av in two_level:
                for m in av.values():
                    if m.find(industry) != -1:
                        for n in av.keys():
                            industry_id = n
        except:
            pass
        try:
            for twv in three_level:
                for k in twv.values():
                    if industry == k:
                        for haha in twv.keys():
                            two_id = haha
        except:
            pass
        industry_id = str(industry_id)
        # print(industry_id)

        # print(two_id)

        # 主要人员
        try:
            MainStaffList = data['basics']['MainStaff']
        except:
            MainStaffList = []
        # 股东信息
        try:
            shareholderList = data['basics']['shareholder']
            if shareholderList == 0:
                shareholderList = []
        except:
            shareholderList = []
        # 对外投资
        try:
            investList = []
            if db['w_was_cast'].find({'pid': mongo_id}) != -1:
                for i in db['w_was_cast'].find({'pid': mongo_id}):
                    invest_Temp = {}
                    invest_Temp['company'] = i['name']
                    invest_Temp['legalPerson'] = i['legalPerson']
                    invest_Temp['capital'] = i['capital']
                    invest_Temp['amount'] = i['amount']
                    invest_Temp['proportion'] = i['proportion']
                    invest_Temp['regTime'] = i['regTime']
                    invest_Temp['status'] = i['status']
                    # print(invest_Temp)
                    investList.append(invest_Temp)
            # print(investList)
        except:
            investList = []

        # 变更记录
        try:
            changeLogList = []
            if db['w_change'].find({'pid': mongo_id}) != -1:
                for i in db['w_change'].find({'pid': mongo_id}):
                    changeLogTemp = {}
                    changeLogTemp['changeTime'] = i['posttime']
                    changeLogTemp['changeProject'] = i['project']
                    changeLogTemp['changeBefore'] = i['before']
                    changeLogTemp['changeAfter'] = i['after']
                    changeLogList.append(changeLogTemp)
            # print(changeLogList)
        except:
            changeLogList = []
        # 招投标

        try:
            bidList = []
            if db['w_bidding'].find({'pid': mongo_id}) != -1:
                for i in db['w_bidding'].find({'pid': mongo_id}):
                    bidTemp = {}
                    bidTemp['time'] = i['posttime']
                    bidTemp['title'] = i['name']
                    # 这里的bidPurchaser后面有一个空格，当时入库时没有仔细检查，是‘bidPurchaser ’
                    bidTemp['purchaser'] = i['purchase']
                    bidTemp['url'] = i['url']
                    bidList.append(bidTemp)
            # print(bidList)
        except:
            bidList = []
        # 债券信息
        try:
            bondList = []
            if db['w_bond'].find({'pid': mongo_id}) != -1:
                for i in db['w_bond'].find({'pid': mongo_id}):
                    bondTemp = {}
                    # 27个字段，后面几个为0的字段是为了和上次入库的格式一致加入的，没有特殊意义
                    bondTemp['bondName'] = i['bondName']
                    bondTemp['bondNum'] = i['bondNum']
                    bondTemp['publisherName'] = i['publisherName']
                    bondTemp['bondType'] = i['bondType']
                    bondTemp['publishTime'] = i['publishTime']
                    bondTemp['publishExpireTime'] = i['publishExpireTime']
                    bondTemp['bondTimeLimit'] = i['bondTimeLimit']
                    bondTemp['bondTradeTime'] = i['bondTradeTime']
                    bondTemp['calInterestType'] = i['calInterestType']
                    bondTemp['bondStopTime'] = i['bondStopTime']
                    bondTemp['creditRatingGov'] =i['creditRatingGov']
                    bondTemp['debtRating'] = i['debtRating']
                    bondTemp['faceValue'] = i['faceValue']
                    bondTemp['realIssuedQuantity'] = i['realIssuedQuantity']
                    bondTemp['planIssuedQuantity'] = i['planIssuedQuantity']
                    bondTemp['issuedPrice'] = i['issuedPrice']
                    bondTemp['payInterestHZ'] = i['payInterestHZ']
                    bondTemp['startCalInterestTime'] = i['startCalInterestTime']
                    bondTemp['exeRightType'] = i['exeRightType']
                    bondTemp['exeRightTime'] = i['exeRightTime']
                    bondTemp['escrowAgent'] = i['escrowAgent']
                    bondTemp['flowRange'] = i['flowRange']
                    bondTemp['tip'] = i['tip']
                    # 没有意义的字段
                    bondTemp['id'] = i['id']
                    bondTemp['remark'] = i['remark']
                    bondTemp['createTime'] = i['createTime']
                    bondTemp['updateTime'] = i['updateTime']
                    bondList.append(bondTemp)
            # print(bondList)
        except:
            bondList = []
        # 购地信息
        try:
            purchaselandList = []
            if db['w_purchase_land'].find({'pid': mongo_id}) != -1:
                for i in db['w_purchase_land'].find({'pid': mongo_id}):
                    purchaselandTemp = {}
                    purchaselandTemp['adminRegion'] = i['adminRegion']
                    purchaselandTemp['elecSupervisorNo'] = i['elecSupervisorNo']
                    purchaselandTemp['totalArea'] = i['totalArea']
                    purchaselandTemp['location'] = i['location']
                    purchaselandTemp['assignee'] = i['assignee']
                    purchaselandTemp['parentCompany'] = i['parentCompany']
                    purchaselandTemp['purpose'] = i['purpose']
                    purchaselandTemp['supplyWay'] = i['supplyWay']
                    purchaselandTemp['minVolume'] = i['minVolume']
                    purchaselandTemp['maxVolume'] = i['maxVolume']
                    purchaselandTemp['dealPrice'] = i['dealPrice']
                    # 这个app爬虫是没有这个字段，
                    purchaselandTemp['linkUrl'] = i['linkUrl']
                    purchaselandTemp['signedDate'] = i['signedDate']
                    purchaselandTemp['startTime'] = i['startTime']
                    purchaselandTemp['endTime'] = i['endTime']
                    # 同上无意义，为了和上次格式一致
                    purchaselandTemp['createTime'] = i['createTime']
                    purchaselandTemp['updateTime'] = i['createTime']
                    purchaselandTemp['id'] = i['id']
                    purchaselandList.append(purchaselandTemp)
            # print(purchaselandList)
        except:
            purchaselandList = []
        # 招聘信息
        try:
            recruitList = []
            if db['w_recruit'].find({'pid': mongo_id}) != -1:
                for i in db['w_recruit'].find({'pid': mongo_id}):
                    recruitTemp = {}
                    recruitTemp['time'] = i['posttime']  # 这个字段没有
                    recruitTemp['position'] = i['job']
                    recruitTemp['salary'] = i['salary']
                    recruitTemp['jobExp'] = i['exp']
                    recruitTemp['number'] = i['number']
                    recruitTemp['city'] = i['city']
                    recruitTemp['company'] = i['company']
                    recruitTemp['edu'] = i['edu']
                    recruitTemp['source'] = i['source']
                    recruitTemp['area'] = i['area']
                    recruitTemp['start'] = i['starttime']
                    recruitTemp['end'] = i['1501516800000']  # 没有这个字段
                    recruitTemp['url'] = i['url']  # 没有原始连接这个字段    app端
                    recruitTemp['desc'] = i['desc']
                    recruitList.append(recruitTemp)
            # print(recruitList)
        except:
            recruitList = []
            # 税务评级
        try:
            taxcreditList = []
            if db['w_tax_rating'].find({'pid': mongo_id}) != -1:
                for i in db['w_tax_rating'].find({'pid': mongo_id}):
                    taxcreditTemp = {}
                    taxcreditTemp['taxcreditTemp'] = i['posttime']
                    taxcreditTemp['rating'] = i['grade']
                    taxcreditTemp['CreditCode'] = i['number']
                    taxcreditTemp['type'] = i['type']  # 这个字段app是没有的
                    taxcreditTemp['unit'] = i['unit']
                    taxcreditList.append(taxcreditTemp)
            # print(taxcreditList)
        except:
            taxcreditList = []
        # 抽查检查
        try:
            checkList = []
            if db['w_check'].find({'pid': mongo_id}) != -1:
                for i in db['w_check'].find({'pid': mongo_id}):
                    checkTemp = {}
                    checkTemp['date'] = i['posttime']
                    if '未公示' in checkTemp['date']:
                        checkTemp['date']='0'
                    checkTemp['result'] = i['result']
                    checkTemp['organ'] = i['organ']
                    checkTemp['type'] = i['type']
                    checkList.append(checkTemp)
            # print(checkList)
        except:
            checkList = []
        # 产品信息
        try:
            productList = []
            if db['w_product'].find({'pid': mongo_id}) != -1:
                for i in db['w_product'].find({'pid': mongo_id}):
                    productTemp = {}
                    productTemp['icon'] = i['logo']  # app没有
                    productTemp['name'] = i['name']
                    productTemp['introduction'] = i['introduction']
                    productTemp['classify'] = i['class']
                    productTemp['field'] = i['field']
                    productTemp['desc'] = i['desc']
                    productList.append(productTemp)
            # print(productList)
        except:
            productList = []
        # 经营异常
        try:
            abnormalList = []
            if db['w_operating_exception'].find({'pid': mongo_id}) != -1:
                for i in db['w_operating_exception'].find({'pid': mongo_id}):
                    abnormalTemp = {}
                    abnormalTemp['dtae'] = i['posttime']
                    if '未公示' in abnormalTemp['dtae']:
                        abnormalTemp['dtae']='0'
                    abnormalTemp['reason'] = i['reason']
                    abnormalTemp['organ'] = i['organ']
                    # abnormalList.append(abnormalTemp)
            # print(abnormalList)
        except:
            abnormalList = []
        # 行政处罚
        try:
            punishList = []
            if db['w_punish'].find({'pid': mongo_id}) != -1:
                for i in db['w_punish'].find({'pid': mongo_id}):
                    punishTemp = {}
                    punishTemp['dtae'] = i['posttime']
                    if '未公示' in punishTemp['dtae']:
                        punishTemp['dtae']='0'
                    punishTemp['number'] = i['number']
                    punishTemp['type'] = i['type']
                    punishTemp['organ'] = i['organ']
                    punishTemp['content'] = i['content']
                    punishList.append(punishTemp)
            # print(punishList)
        except:
            punishList = []
        # 严重违法
        try:
            illegalList = []
            if db['w_yanzhong'].find({'pid': mongo_id}) != -1:
                for i in db['w_yanzhong'].find({'pid': mongo_id}):
                    illegalTemp = {}
                    illegalTemp['reason'] = i['reason']
                    illegalTemp['organ'] = i['organ']
                    illegalTemp['posttime'] = i['time']
                    illegalList.append(illegalTemp)
            # print(illegalList)
        except:
            illegalList = []
        # 股权出质
        try:
            equityList = []
            if db['w_equity'].find({'pid': mongo_id}) != -1:
                for i in db['w_equity'].find({'pid': mongo_id}):
                    equityTemp = {}
                    equityTemp['time'] = i['posttime']
                    equityTemp['numbering'] = i['number']
                    equityTemp['pledgor'] = i['person']
                    equityTemp['pledgee'] = i['pledgee']
                    equityTemp['status'] = i['status']
                    equityTemp['amount'] = i['amount']
                    equityTemp['certificate'] = i['certifNumber']
                    equityTemp['pledgeeNumber'] = i['certifNumberR']
                    equityList.append(equityTemp)
            # print(equityList)
        except:
            equityList = []
            # 动产抵押
        try:
            mortgageList = []
            if db['w_mortgage'].find({'pid': mongo_id}) != -1:
                for i in db['w_mortgage'].find({'pid': mongo_id}):
                    mortgageTemp = {}
                    mortgageTemp['number'] = i['number']
                    mortgageTemp['type'] = i['type']
                    mortgageTemp['organ'] = i['organ']
                    mortgageTemp['status'] = i['status']
                    mortgageTemp['amount'] = i['amount']
                    mortgageTemp['range'] = i['range']
                    mortgageTemp['name'] = i['peopleName']
                    mortgageTemp['pawnInfoList'] = i['pawnInfoList']
                    mortgageTemp['changeInfoList'] = i['changeInfoList']
                    mortgageList.append(mortgageTemp)
            # print(mortgageList)
        except:
            mortgageList = []
        # 欠税公告
        try:
            taxeList = []
            if db['w_taxes'].find({'pid': mongo_id}) != -1:
                for i in db['w_taxes'].find({'pid': mongo_id}):
                    taxeTemp = {}
                    if i['taxpayer'] == "":
                        i['taxpayer'] = '0'
                    if i['tax'] == "":
                        i['tax'] = '0'
                    taxeTemp['CreditCode'] = i['taxpayer']
                    taxeTemp['taxes'] = i['taxes']
                    taxeTemp['tax'] = i['tax']
                    taxeTemp['balance'] = i['balance']
                    taxeTemp['organ'] = i['organ']
                    taxeTemp['date'] = i['posttime']
                    if '未公示' in taxeTemp['date']:
                        taxeTemp['date']='0'

                    taxeList.append(taxeTemp)
            # print(taxeList)
        except:
            taxeList = []
        # 年报
        try:
            yearList = []
            if db['w_Year'].find({'pid': mongo_id}) != -1:
                for i in db['w_Year'].find({'pid': mongo_id}):
                    yearTemp = {}
                    del i['pid']
                    del i['_id']
                    yearTemp['content'] = i
                    yearTemp['year'] = i['time'] + "年度"
                    yearList.append(yearTemp)
            # print(yearList)
        except:
            yearList = []

        # 融资历史w_financing
        try:
            financingList = []
            if db['w_financing'].find({'pid': mongo_id}):
                for i in db['w_financing'].find({'pid': mongo_id}):
                    financingTemp = {}
                    if i['proportion'] == "":
                        i['proportion'] = '0'
                    if i['investor'] == "":
                        i['investor'] = '0'
                    if i['valuation'] == "":
                        i['valuation'] = '0'
                    financingTemp['rounds'] = i['rounds']
                    financingTemp['valuation'] = i['valuation']  # 没有这个字段
                    financingTemp['amount'] = i['amount']
                    financingTemp['proportion'] = i['proportion']
                    financingTemp['investor'] = i['investor']
                    financingTemp['time'] = i['posttime']
                    financingList.append(financingTemp)
            # print(financingList)
        except:
            financingList = []
        # 核心团队
        try:
            teamList = []
            if db['w_cord'].find({'pid': mongo_id}):
                for i in db['w_cord'].find({'pid': mongo_id}):
                    teamTemp = {}
                    teamTemp['topic'] = i['topic']  # app没有头像
                    teamTemp['username'] = i['username']
                    teamTemp['position'] = i['position']
                    teamTemp['experience'] = i['experience']
                    teamList.append(teamTemp)
            # print(teamList)
        except:
            teamList = []

        # 投资事件
        try:
            investmentList = []
            if db['w_investment'].find({'pid': mongo_id}):
                for i in db['w_investment'].find({'pid': mongo_id}):
                    investmentTemp = {}
                    investmentTemp['rounds'] = i['rounds']
                    investmentTemp['amount'] = i['amount']
                    investmentTemp['investor'] = i['investor']
                    investmentTemp['product'] = i['product']
                    investmentTemp['area'] = i['area']  # 这三个字段app没有
                    investmentTemp['industry'] = i['industry']
                    investmentTemp['business'] = i['business']
                    investmentTemp['posttime'] = i['time']
                    investmentList.append(investmentTemp)
            # print(investmentList)
        except:
            investmentList = []

        # 竞品信息
        try:
            competitionList = []
            if db['w_competing_products'].find({'pid': mongo_id}):
                for i in db['w_competing_products'].find({'pid': mongo_id}):
                    competitionTemp = {}
                    if i['rounds'] == ' ':
                        i['rounds'] = '0'
                    if i['valuation'] == ' ':
                        i['valuation'] = '0'
                    competitionTemp['name'] = i['product']
                    competitionTemp['area'] = i['area']
                    competitionTemp['rounds'] = i['rounds']
                    competitionTemp['industry'] = i['industry']
                    competitionTemp['business'] = i['business']
                    competitionTemp['valuation'] = i['valuation']
                    competitionTemp['posttime'] = data['time']
                    competitionList.append(competitionTemp)
            # print(competitionList)
        except:
            competitionList = []

        if regTime == 0:
            regTime = '0'
        if regTime == '未公开':
            regTime = '0'
        if shareholderList == 0:
            shareholderList = []
        if industry_id == '0':
            two_id = '0'
        information = {
            "mongodb_id": str(mongo_id),
            "posttime": int(posttime),
            "company": company,
            "legalPerson": legalPerson,
            "amount": amount,
            "amount_number": amount_number,
            "status": status,
            "province": i_province,
            "city": i_city,
            "area": i_area,
            "regTime": regTime,
            "phone": phone,
            "mail": mail,
            "website": website,
            "address": address,
            "score": score,

            "number": regNumber,
            "code": code,
            "creditCode": number,
            "type": type,
            "industry": industry,
            "trade": estiblish,
            "approved": approved,
            "organ": organ,
            "scale": "0",
            "once": "未公开",
            "english": english,
            "range": scope,

            "prov_id": prov_id,
            "city_id": city_id,
            "area_id": area_id,
            "industry_one": industry_id,
            "industry_two": str(two_id),

            "MainStaff": MainStaffList,

            "shareholder": shareholderList,

            "invest": investList,

            "changeLog": changeLogList,

            "bidList": bidList,

            "bondList": bondList,
            "purchaselandList": purchaselandList,

            "recruitList": recruitList,
            "taxcreditList": taxcreditList,

            "checkList": checkList,

            "productList": productList,

            "abnormalList": abnormalList,

            "punishList": punishList,

            "illegal": illegalList,

            "equityList": equityList,

            "mortgageList": mortgageList,

            "taxes": taxeList,

            "year": yearList,

            "financing": financingList,

            "team": teamList,

            "investment": investmentList,


            "competition": competitionList,

        }
        # print(information)
        # 这里判断去重

        url = 'http://es.51rec.com/enterprise/_search?q=mongodb_id:' + str(information["mongodb_id"])
        r = requests.get(url=url, auth=('flyminer', 'fm110707*'))
        # print(r.status_code)
        result = json.loads(r.text)
        count = result["hits"]["total"]
        # print(count)

        if int(count) == 0:
            # print("不存在")
            information = json.dumps(information)
            # print(information)
            r = requests.post(url='http://es.51rec.com/enterprise/fm_type?pretty', data=information,
                              auth=('flyminer', 'fm110707*'))
            # print(r.text)
            print(mongodb_id)
            # print('插入成功!')

        if int(count) == 1:
            web_id = result["hits"]["hits"][0]["_id"]
            information = json.dumps(information)
            r = requests.put(url='http://es.51rec.com/enterprise/fm_type/' + web_id, data=information,auth=('flyminer', 'fm110707*'))
            print(mongodb_id)
            # print('更新成功!')
            # print(information)
            # print('存在更新')








