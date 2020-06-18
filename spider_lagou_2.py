
import pprint
import json
import time
import requests
import random
import datetime
import os
from os import mkdir
from _datetime import timedelta

url = 'https://www.lagou.com/jobs/positionAjax.json'
cookie_url = 'https://www.lagou.com/jobs/list_python?labelWords=&fromSearch=true&suginput='
city = '上海'  #城市
kd = 'python' # 关键词
PATH= 'c:/data/lagou' #数据保存根目录
FILE_NAME_PRE = 'position'+'_'+city  #文件名前缀
isExecuted = False
def get_cookies(url):
    # 获取最新的cookie（由第一个网站生成）
    try:
        headers = {
            'origin': 'https://www.lagou.com',
            'referer': 'https://www.lagou.com/jobs/list_python?labelWords=&fromSearch=true&suginput=',
            'authority': 'www.lagou.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
        }
        response = requests.get(url,headers=headers)
        if response.status_code == 200:
            return response.cookies.get_dict()
    except RequestException:
        return None

def post_one_page(url,cookies,city,pn=1,kd='java'):
    # post方法请求网站数据
    try:
        headers = {
            'origin':'https://www.lagou.com',
            'referer':'https://www.lagou.com/jobs/list_python?labelWords=&fromSearch=true&suginput=',
            'authority':'www.lagou.com',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
            #'cookie':'JSESSIONID=ABAAAECAAEBABIIBF487D9A472FDFBA04D1697E847424C1; SEARCH_ID=42a52d7c8905491184a6761104a485e8; user_trace_token=20200516094129-08709fb3-8a1c-4b36-a12a-14410546f3c0; X_HTTP_TOKEN=42daf4b72327b2819823959851bf5e71415983ed09; WEBTJ-ID=20200516094127-1721b24abfd102-009101658dddaf-d373666-1327104-1721b24abfe688',
        }
        params = {
            'city': city,
            'needAddtionalResult': 'false',
        }
        data = {
            'first': 'false',
            'pn': pn,
            'kd': kd,
        }
        response = requests.post(url,
                                 params=params,
                                 data=data,
                                 headers=headers,
                                 cookies=cookies)
        if response.status_code == 200:
            return response
    except RequestException:
        return None

def wash_data(raw_data):
    # 获取信息,并且返回一个由字符串组成的列表
    pattern_list = raw_data['content']['positionResult']['result']
    data_list = []  
    for data in pattern_list:
        t_data ={}
        t_data['city']=data['city']
        t_data['companyFullName']=data['companyFullName']
        print('>>>正在清洗的数据：' + str(data['companyFullName']) + '...')
        t_data['companySize']=data['companySize']
        t_data['createTime']=data['createTime']
        t_data['district']=data['district']
        t_data['education']=data['education']
        t_data['financeStage']=data['financeStage']
        t_data['positionAdvantage']=data['positionAdvantage']
        t_data['skillLables']=data['skillLables']
        t_data['subwayline']=data['subwayline']
        t_data['positionName']=data['positionName']
        t_data['salary']=data['salary']
        t_data['workYear']=data['workYear']
        data_list.append(t_data)     
        time.sleep(0.5)
    return data_list

def write_to_file_txt(data_list):
    filename= generate_filename('.txt')      
    print(f">>>存储数据至文件：{filename}")
    # 存储为txt格式的数据
    with open(filename,mode='a',encoding='utf-8')as f:
        for data in data_list:
            rStr = ''.join([
                            str(data['city']), '&'+str(data['companyFullName']), '&'+str(data['companySize']), '&'+str(data['workYear']), '&'+str(data['city']), '&'+str(data['createTime']), '&'+str(data['district']),
                            '&'+str(data['education']), '&'+str(data['financeStage']), '&'+str(data['positionAdvantage'])
                            , '&'+str(data['skillLables']), '&'+str(data['subwayline']), '&'+str(data['positionName']), '&'+str(data['salary']), '&'+str(data['workYear'])
                            ])
            f.write(rStr+'\n')

def write_to_file_csv(date_list):
    filename= generate_filename('.csv')      
    print(f">>>存储数据至文件：{filename}")
    # 存储为csv格式的数据
    with open(filename,mode='a',newline='',encoding='utf-8-sig')as csvfile:
        for data in date_list:  
            rStr = ''.join([
                            str(data['city']), '&'+str(data['companyFullName']), '&'+str(data['companySize']), '&'+str(data['workYear']), '&'+str(data['city']), '&'+str(data['createTime']), '&'+str(data['district']),
                            '&'+str(data['education']), '&'+str(data['financeStage']), '&'+str(data['positionAdvantage'])
                            , '&'+str(data['skillLables']), '&'+str(data['subwayline']), '&'+str(data['positionName']), '&'+str(data['salary']), '&'+str(data['workYear'])
                            ])    
            csvfile.write(rStr+'\n')

def write_to_file_json(data_json):
    filename= generate_filename('.json')      
    print(f">>>存储数据至文件：{filename}")
    # 存储为一行一行的json格式的数据
    json_data = json.dumps(data_json, ensure_ascii=False)
    with open(filename,mode='a',encoding='utf-8')as f:
        f.write(json_data+'\n')

def load_to_hbase():
    print('>>>调用save-hbase.jar,加载数据至hbase...')
    

def load_to_hive():
    print('>>>调用save-hive.jar,加载数据至hive...')
    

def generate_filename(type):
    dateStr = datetime.datetime.now().strftime('%Y%m%d')
    save_path = PATH +'/'+ dateStr
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    absolute_path = save_path + '/'+FILE_NAME_PRE + '_' + dateStr + type
    return absolute_path

#获得从当前时间到明日的具体秒数
def getSleepSeconds():
    dateNow = datetime.datetime.now()
    dataNowStr = dateNow.strftime('%Y%m%d')
    #获得明日日期
    dataN = dateNow + timedelta(days=1)
    dataNStr = dataN.strftime('%Y%m%d')
    dataNTime = datetime.datetime.strptime(dataNStr+'000000', '%Y%m%d%H%M%S')
    #计算差异时间
    dataC = dataNTime - dateNow
    return dataC.seconds

def main(): 
    # 获取cookie  
    cookies = get_cookies(cookie_url)
    html = post_one_page(url,cookies,city,1,kd)
    # 自适应获得总页数
    total_size = html.json()['content']['positionResult']['totalCount']
    page_size = html.json()['content']['positionResult']['resultSize']
    max_page_num = int((total_size+ (page_size-1)) / page_size);
    print(f">>>城市:{city},关键词:{kd},查到记录总数:{total_size}")
    for i in range(1,max_page_num):
        print(f'>>>共{max_page_num}页，正在爬取第{i}页的职位信息...')
        cookies = get_cookies(cookie_url)      
        html = post_one_page(url,cookies,city,i,kd)
        print('>>>开始清洗数据')
        data_list = wash_data(html.json())
        write_to_file_txt(data_list)
        write_to_file_csv(data_list)
        write_to_file_json(data_list)
        # 随机延迟，防反爬措施
        delay_time = random.uniform(2, 9)
        #控制随机数的精度round(数值，精度)
        time.sleep(round(delay_time, 1))
    load_to_hbase()
    load_to_hive()
if __name__ == '__main__':  
    while True:      
        print(f">>>程序启动")
        main()
        seconds = getSleepSeconds()
        print(f">>>开始休眠，休眠时间为：{seconds}")
        time.sleep(5)   
