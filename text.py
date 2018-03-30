import urllib ,requests
# company_name1 = '湖南长沙'
# # url编码
# company_name1 = urllib.request.quote(company_name1)
# url='http://m.eqicha.com/yqcapp/api/corp/h5/searchList?request=%7B%22searchKey%22:%22{}%22%7D'.format(company_name1)
# print(url)
from pymongo import MongoClient
connect = MongoClient('127.0.0.1', 27017)
db = connect.Insight.w_basics
post = connect.Now_Company_name.hunan
print('****************************************')
url_list=[]
num = 0
for i in post.find({}):
    num += 1
    if num in range(27801,27805):
        print(num)
        print('----------------------')

        company_name1 = i['company']
        # url编码
        company_name1 = urllib.request.quote(company_name1)
        url='http://m.eqicha.com/yqcapp/api/corp/h5/searchList?request=%7B%22searchKey%22:%22{}%22%7D'.format(company_name1)
        url_list.append(url)



url_list=' '.join(url_list)
print(url_list)
