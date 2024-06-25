import hashlib
import hmac
import json
import os
import random
import time

import pymongo
from lxml import etree
from bs4 import BeautifulSoup

import pandas as pd
from playwright.sync_api import sync_playwright  # 同步模式
# from retrying import retry
import requests
import execjs

from Data.mongodb import save, get_unifiedSalary

citys_code_map = {
        '北京': '010000', '上海': '020000', '广州': '030200', '深圳': '040000',
        '武汉': '180200', '西安': '200200', '杭州': '080200', '南京': '070200',
        '成都': '090200', '重庆': '060000', '东莞': '030800', '大连': '230300',
        '沈阳': '230200', '苏州': '070300', '昆明': '250200', '长沙': '190200',
        '合肥': '150200', '宁波': '080300', '郑州': '170200', '天津': '050000',
        '青岛': '120300', '济南': '120200', '哈尔滨': '220200', '长春': '240200',
        '福州': '110200', '珠三角': '01'
}

# 定义列名为全局变量
columns = ['岗位名称', '城市', "薪资", "发布时间", "经验要求", '学历要求', '公司名称','公司类型', '公司规模','行业类型',
           '岗位链接', '工作福利']


Cookie1 = 'tfstk=fUmSKaZmY_f5PHc11zp4c2GnYzZe3XtaJ9wKIvIPpuEJRXGIKyQ8y2lQRSG3azr8a9sIGYlEzvk-RWHUi8Q89yIYM8yQeynUTD9QqmuPY7RuAvhafCRwbhkoEuqp_CrjgTCQvJKU9GQ_99q3vCR2bhkoEkcZw-OkPxpbURyLvzIRkKw_LkI8JgQAMS2LvWhLJSIYKRPdJzEdMUoHG-Y7wYOF4c_LtnJSFSsdfx28kx-gGMKrc81a6YBAvMnbFzmEo4rUjzuKIPmrhHs04AgYDJiHBwNQ52nzD0dvXr4KRXemoB_Q12HrS-n9LNVqc7lKVrpNfyrxbPE-uEAz8uwL2P0DKaPKJAi_4bRcv7usHjaZ0sSaXVcjAPEO4ssa1UahRwVRv-_khK_h8ct4b-jQHSv7y-2qbK9fNwfGvM7mYK_dzze03XpXh_1C.; NSC_ohjoy-bmjzvo-200-159=ffffffffc3a0d61645525d5f4f58455e445a4a…=uqTYYC/heN313BHLKBcpE4VSnTYlE7qpk7t=q44XmNw5nEUvcm4z81beVbW2U5WRwEmhjiYbFGNDRQDx4g2eDA7IRezA7j2DK+aI43UmzW7/lhQFDjfDBf0uixQi0YgwbibteA7GmTBHLK01m5N5Wx5D=1oqoqxnBtt9qDwtjFTFGPdqFWhSi2zzG5LuDt9e3B46TNLwnAb/F427nb7IOfq=OES0b7PTbUKaRwwiYuiBCWyz+LL3uL3xDKqKjDDLxD23ldoG3eD===; Hm_lvt_1370a11171bd6f2d9b1fe98951541941=1713862599,1713869135; acw_tc=ac11000117138691338915890e00dea35977a83bc06e50a21ad53a8f5612a8; acw_sc__v3=662791505f087a925f6a36f5a734bf72eb815d0b; Hm_lpvt_1370a11171bd6f2d9b1fe98951541941=1713869135'
Cookie2 = 'guid=7b4ff9b4d9a113f6cc0b32de6b2cbf3b; sajssdk_2015_cross_new_user=1; Hm_lvt_1370a11171bd6f2d9b1fe98951541941=1714226436; Hm_lpvt_1370a11171bd6f2d9b1fe98951541941=1714226436; acw_tc=ac11000117142264407372112e7fdd2be7a4bcad4e1865d36d3f54c0e4af71; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%227b4ff9b4d9a113f6cc0b32de6b2cbf3b%22%2C%22first_id%22%3A%2218f1fdb92e913b5-01047eb3602634f-26001d51-1327104-18f1fdb92ea10c6%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMThmMWZkYjkyZTkxM2I1LTAxMDQ3ZWIzNjAyNjM0Zi0yNjAwMWQ1MS0xMzI3MTA0LTE4ZjFmZGI5MmVhMTBjNiIsIiRpZGVudGl0eV9sb2dpbl9pZCI6IjdiNGZmOWI0ZDlhMTEzZjZjYzBiMzJkZTZiMmNiZjNiIn0%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%227b4ff9b4d9a113f6cc0b32de6b2cbf3b%22%7D%2C%22%24device_id%22%3A%2218f1fdb92e913b5-01047eb3602634f-26001d51-1327104-18f1fdb92ea10c6%22%7D; acw_sc__v2=662d050c1301b8e9af82acfbe3b4a375db1fbf30; nsearch=jobarea%3D%26%7C%26ord_field%3D%26%7C%26recentSearch0%3D%26%7C%26recentSearch1%3D%26%7C%26recentSearch2%3D%26%7C%26recentSearch3%3D%26%7C%26recentSearch4%3D%26%7C%26collapse_expansion%3D; search=jobarea%7E%60%7C%21recentSearch0%7E%60000000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FAjava%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21; JSESSIONID=3D7370D27D698C11A59F32DC5E62DEE3; ssxmod_itna=iqrtLG7i0LoOo0=dGQaG=qi=D70PPrkrDlpi4A5D8D6DQeGTQ6qDBn2WcagOdaRqGOKoeTok8t62BoPpSDpxrDHxY=DUoj4qkQDeWQD5xGoDPxDeDADYojDAqiOD7qDdA1dsU8DbxYPlDitLD7U/DeDj+q=24eG0DDUaj4G2YC6tDDXtBjw4/4xDxGYL4Qt3sTYrTYxnD0t4xBL=+6qnGWF8MBtrapaV3rQDzT7DtkUP1wbDCO6lAuN1KAm3KGhCf0L=trAejgGtQR8hS7Dt6ODoeRL4Bom4mDwYUz/tDDfPANxxD===; ssxmod_itna2=iqrtLG7i0LoOo0=dGQaG=qi=D70PPrkD6r+xGqDsAoDLG3gIf4nRdIMG1pkhkpt9z=DBiHklEfAmma3jha5GUbnERUETTASy6HbWpBc=IjC1kSf+c/PI1UaZU=weG2e47=D+crD=; tfstk=f3JSO-9G-z45IK-f-_nVhYgN_JXCOUMaAkspjHezvTB8JJ_ffgKFUXAfRELfywuleweB8HJy8pt3dMKp-0bEqj-kq9XK7VJIQ3xlSeS3aqWKvnUhFr-jQA-k2Mb2omDZxKwl_wbdp_C8MrQcYwIJJ_CADZjhvzILemtAoMBLe6F8krIdV8ERpb3Elic5y3gUiOh0VAbk2Zw6mKsRO7xRlJephI1PW-7bpJpfVnXWcNwsTNdGz17Mkxy5dnIBl1RS58TBvhAdGpa-4FKprK6lCXol1Id2__OjyJ_2UM1OpsZLpn6pw6pCBAUf'
# 这个装饰器的作用就是在滑块失败的时候可以重复滑动
def getBaseInfoHtml(url):
    global Cookie2
    if 'we.51job.com' not in url:
        return
    """处理滑块"""
    with sync_playwright() as fp:
        bs = fp.firefox.launch(headless=True)  # 禁用无头模式(也就是启动不启动浏览器的区别)
        page = bs.new_page()  # 新建选项卡
        page.goto(url)  # 加载页面
        if page.locator('#nc_1_n1z').is_visible():
            dropbutton = page.locator('#nc_1_n1z')
            box = dropbutton.bounding_box()  # 获取其边界框
            page.mouse.move(box['x'] + box['width'] / 2, box['y'] + box['height'] / 2)
            page.mouse.down()
            # 模拟人类的鼠标移动轨迹
            steps = 15  # 可以根据需要调整步数
            for step in range(1, steps + 1):
                progress = step / steps
                mov_x = box['x'] + box['width'] / 2 + 300 * progress + random.uniform(-5, 5)
                mov_y = box['y'] + box['height'] / 2 + random.uniform(-5, 5)
                page.mouse.move(mov_x, mov_y)
                time.sleep(random.uniform(0.001, 0.003))  # 模拟人的自然反应时间
            page.mouse.up()
            # 等待加载新页面
            page.wait_for_load_state()

        # 获取当前页面的Cookie
        # cookies = page.context.cookies(page.url)
        # i = 1
        # for cookie in cookies:
        #     print(cookie)
        #     if i == 1:
        #         Cookie2 = cookie['name'] + '=' + cookie['value']
        #         i = 0
        #     else:
        #         Cookie2 = Cookie1 + '; ' + cookie['name'] + '=' + cookie['value']

        html = page.locator('xpath=/html/body/pre')
        qwer = html.inner_text()
        if 'resultbody' in qwer:
            a = json.loads(qwer)
            return a
        else:
            return False

def get_sign(url):
    key = 'abfc8f9dcf8c3f3d8aa294ac5f2cf2cc7767e5592590f39c3f503271dd68562b'
    value = url[len('https://we.51job.com'):]
    key = key.encode('utf-8')
    value = value.encode('utf-8')
    sign = hmac.new(key, value, digestmod=hashlib.sha256).hexdigest()
    return sign

def getBaseInfoHtml2(url):
    if 'we.51job.com' not in url:
        return
    sign = get_sign(url)
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Cookie': Cookie2.encode('utf-8').decode('latin1'),
        'From-Domain': '51job_web',
        'Host': 'we.51job.com',
        'Referer': 'https://we.51job.com/pc/search?jobArea=000000&keyword=java&searchType=2&keywordType=',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'account-id': '',
        'partner': '',
        'property': '%7B%22partner%22%3A%22%22%2C%22webId%22%3A2%2C%22fromdomain%22%3A%2251job_web%22%2C%22frompageUrl%22%3A%22https%3A%2F%2Fwe.51job.com%2F%22%2C%22pageUrl%22%3A%22https%3A%2F%2Fwe.51job.com%2Fpc%2Fsearch%3FjobArea%3D000000%26keyword%3Djava%26searchType%3D2%26keywordType%3D%22%2C%22identityType%22%3A%22%22%2C%22userType%22%3A%22%22%2C%22isLogin%22%3A%22%E5%90%A6%22%2C%22accountid%22%3A%22%22%2C%22keywordType%22%3A%22%22%7D',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sign': sign,
        'user-token': '',
        'uuid': 'e8827e1fde555900b3c2ac84b573d6d4'
    }
    response = requests.get(url, headers=headers)
    qwer = response.text
    if 'resultbody' in qwer:
        a = json.loads(qwer)
        ttt = random.uniform(2,5)
        time.sleep(ttt)
        return a
    else:
        print('一级页面被反爬')
        return getBaseInfoHtml(url)




def get_timestamp_1258(url):
    original = url
    url_list = url.split('?', 1)
    if len(url_list) > 1:
        search = '?' + url_list[1]
    else:
        search = ''
    # 获取.com后面的字符串
    index = url_list[0].find('.com')
    if index != -1:
        pathname = url_list[0][index + 4:]
    else:
        pathname = ''

    with open('timestamp.js', 'r', encoding='utf-8') as f:
        js_code = f.read()
        js = execjs.compile(js_code)
        def get_timestamp(pathname,search,original):
            timestamp = js.call('get_timestamp', pathname, search, original)
            return timestamp
    timestamp_1258 = get_timestamp(pathname,search,original)
    return timestamp_1258

def getJobKeyWords(url):
    global Cookie1
    if 'jobs' not in url:
        return ''# 如果URL不包含'jobs'，直接返回
    """处理滑块"""
    with sync_playwright() as fp:
        bs = fp.firefox.launch(headless=True)  # 禁用无头模式(也就是启动不启动浏览器的区别)
        page = bs.new_page()  # 新建选项卡
        page.goto(url)  # 加载页面
        # 判断是否有滑块
        if page.locator('#nc_1_n1z').is_visible():
            dropbutton = page.locator('#nc_1_n1z')
            box = dropbutton.bounding_box()  # 获取其边界框
            page.mouse.move(box['x'] + box['width'] / 2, box['y'] + box['height'] / 2)
            page.mouse.down()
            # 模拟人类的鼠标移动轨迹
            steps = 15  # 可以根据需要调整步数
            for step in range(1, steps + 1):
                progress = step / steps
                mov_x = box['x'] + box['width'] / 2 + 300 * progress + random.uniform(-5, 5)
                mov_y = box['y'] + box['height'] / 2 + random.uniform(-5, 5)
                page.mouse.move(mov_x, mov_y)
                time.sleep(random.uniform(0.001, 0.003))  # 模拟人的自然反应时间
            page.mouse.up()
            # 等待加载新页面
            try:
                page.wait_for_selector('.bmsg.job_msg.inbox', timeout=10000)
            except:
                return '',''

        # 获取关键字的xpath
        # print(page.locator('p:has-text("关键字")').locator('.el.tdn').inner_text())

        # 获取当前页面的Cookie
        cookies = page.context.cookies()
        i = 1
        for cookie in cookies:
            if i == 1:
                Cookie1 = cookie['name'] + '=' + cookie['value']
                i = 0
            else:
                Cookie1 = Cookie1 + '; ' + cookie['name'] + '=' + cookie['value']
        # for cookie in Cookies:
        #     print(cookie)
        # 定位福利标签
        fl_keywords = ''
        if page.query_selector('.jtag') is not None:
            fl_elements = page.query_selector('.jtag').query_selector('.t1').query_selector_all('.sp4')
            if(len(fl_elements) > 0):
                i = 1
                for fl_element in fl_elements:
                    if i == 1:
                        fl_keywords = fl_element.inner_text()
                        i = 0
                    else:
                        fl_keywords = fl_keywords + '/' + fl_element.inner_text()

        job_keywords = ''
        # 假设要定位所有 class 为 ".el.tdn" 的元素
        if page.query_selector('p:has-text("关键字")') is not None:
            elements = page.query_selector('p:has-text("关键字")').query_selector_all('.el.tdn')
            if len(elements) > 0:
                # 遍历并打印每个元素的文本内容（假设它们是可显示文本的元素）
                i = 1
                for element in elements:
                    text =  element.inner_text()
                    if i == 1:
                        job_keywords = text
                        i = 0
                    else:
                        job_keywords = job_keywords + '/' + text

        return fl_keywords,job_keywords


def getJobKeyWords2(url):
    headers = {
        'Host': 'jobs.51job.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Connection': 'keep-alive',
        'Cookie': Cookie1.encode('utf-8').decode('latin1'),
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1'
    }
    # 获取timestamp_1258
    timestamp_1258 = get_timestamp_1258(url)
    url2 = url + '&timestamp__1258=' + str(timestamp_1258)
    response = requests.get(url=url2,headers=headers)
    response.encoding = 'gbk'
    if '婊戝姩楠岃瘉椤甸潰' in response.text:
        print('二级页面需要滑动')
        return getJobKeyWords(url)

    html = response.text
    soup = BeautifulSoup(html, 'lxml')
    fl_keywords = ''
    if soup.find('div', class_='jtag') is not None:
        fl_elements = soup.find('div', class_='jtag').find('div', class_='t1').find_all('span', class_='sp4')
        if len(fl_elements) > 0:
            i = 1
            for fl_element in fl_elements:
                if i == 1:
                    fl_keywords = fl_element.text
                    i = 0
                else:
                    fl_keywords = fl_keywords + '/' + fl_element.text
    # 查找含有特定文本的标签
    keyword = "关键字"
    paragraph_with_keyword = soup.find_all(class_=['label'])
    elements = []
    for p in paragraph_with_keyword:
        if keyword in p.text:
            parent = p.parent
            elements = parent.find_all(class_=['el tdn'])
            break
        else:
            elements = []
    job_keywords = ''
    if len(elements) > 0:
        # 遍历并打印每个元素的文本内容（假设它们是可显示文本的元素）
        i = 1
        for element in elements:
            text = element.text
            if i == 1:
                job_keywords = text
                i = 0
            else:
                job_keywords = job_keywords + '/' + text
    ttt = random.uniform(2,4)
    time.sleep(ttt)
    return fl_keywords,job_keywords



def save_data(data):
    """保存数据"""
    # 如果文件不存在，则写入列名
    if not os.path.isfile('前程无忧_51Job_岗位信息.csv'):
        df = pd.DataFrame(columns=columns)
        df.to_csv('前程无忧_51Job_岗位信息.csv', index=False)

    # 使用pandas包将列表和列名整合到数据框架中
    df = pd.DataFrame(data, columns=columns)
    # 将数据框架保存为CSV文件
    df.to_csv('前程无忧_51Job_岗位信息.csv', index=False, mode='a', header=False)

def parseDataFields(i):
    """解析数据"""
    jobName = i['jobName']  # 岗位名称
    jobAreaString = i['jobAreaString']  # 城市
    for city in citys_code_map.keys():
        if city in jobAreaString:
            jobAreaString = city
            break
    provideSalaryString = i['provideSalaryString']  # 薪资
    provideSalaryString = get_unifiedSalary(provideSalaryString)
    issueDateString = i['issueDateString']  # 发布时间
    workYearString = i['workYearString']  # 需要工作经验
    degreeString = i['degreeString']  # 学历
    companyName = i['companyName']  # 公司名称
    companyTypeString = i['companyTypeString']  # 企业类型
    companySizeString = i['companySizeString']  # 公司人数
    companyIndustryType1Str = ''
    if 'companyIndustryType1Str' in i.keys():
        companyIndustryType1Str = i['companyIndustryType1Str'] # 工业类型
    # lon = i['lon']  # 纬度
    # lat = i['lat']  # 精度
    jobHref = i['jobHref']  # 岗位链接
    # companyHref = i['companyHref']  # 公司链接
    # jobWelfare,jobKeyWords = getJobKeyWords2(jobHref)
    jobWelfareCodeDataList = i['jobWelfareCodeDataList'] # 工作福利
    jobWelfare = ""
    # 如果列表不为空，遍历获取每个福利合成一个字符串
    if jobWelfareCodeDataList:
        flag = 0
        for j in jobWelfareCodeDataList:
            if flag == 0:
                jobWelfare = j['chineseTitle']
                flag = 1
            else:
                jobWelfare += '/' + j['chineseTitle']


    s = [jobName, jobAreaString, provideSalaryString, issueDateString, workYearString, degreeString,companyName,companyTypeString,
         companySizeString, companyIndustryType1Str, jobHref, jobWelfare]

    document = {}
    for i in range(len(columns)):
        document[columns[i]] = s[i]

    return document


if __name__ == '__main__':
    # 获取当前时间的时间戳（秒）
    timestamp = int(time.time())
    # 将时间戳转换为指定格式的字符串
    formatted_timestamp = "{:06d}".format(timestamp)
    # jobName = input("请输入岗位:")
    # 'Java','C++','PHP','Python','C#','Golang','Node.js','Sql'
    #         'Android','iOS','小程序开发','JavaScript','Vue','Unity',
    # '软件测试','网络安全','算法','人工智能','大数据','全栈','嵌入式'
    job_list = [
        '大数据', '全栈', '嵌入式'
    ]
    client = pymongo.MongoClient('localhost', 27017)
    db = client['jobdb']
    for jobName in job_list:
        for city in citys_code_map.keys():
            code = citys_code_map[city]
            for page in range(1, 6):
                print('正在爬取  ' + city + '  的  ' + jobName + '  的第  ' + str(page) + '  页岗位信息')
                """直接把全量的500条作为一次数据接口了"""
                url = (f'https://we.51job.com/api/job/search-pc?api_key=51job&timestamp={formatted_timestamp}&keyword={jobName}'
                       f'&searchType=2&function=&industry=&jobArea={code}&jobArea2=&landmark=&metro=&salary=&workYear=&degree=&'
                       f'companyType=&companySize=&jobType=&issueDate=&sortType=3&pageNum=1&requestId=71bfbc17ec519737773f2b7e039f1d27'
                       f'&pageSize=200&source=1&accountId=&pageCode=sou%7Csou%7Csoulb')
                html = getBaseInfoHtml(url)
                dataList = []
                bba = html['resultbody']['job']['items']
                for i in bba:
                    data = parseDataFields(i)
                    print(data)
                    dataList.append(data)
                save(db,jobName, dataList)
                if len(dataList) == 0:
                    html = getBaseInfoHtml(url)
                    dataList = []
                    bba = html['resultbody']['job']['items']
                    for i in bba:
                        data = parseDataFields(i)
                        print(data)
                        dataList.append(data)
                    save(db, jobName, dataList)
                if len(dataList) < 200:
                    print('数据不足200条，爬取结束')
                    break
                # time.sleep(random.uniform(5, 10))
    client.close()
# if __name__ == '__main__':
#     c,b = getJobKeyWords2('https://jobs.51job.com/shenzhen-nsq/154253911.html?s=sou_sou_soulb&t=0_0&req=71bfbc17ec519737773f2b7e039f1d27')
#     print(c)
#     print(b)