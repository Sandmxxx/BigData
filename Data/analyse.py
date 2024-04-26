# -*- coding = utf-8 -*-
# @Time: 2023/2/5 22:06
# @Author: Watson
# @File: spider.py
# @Software: PyCharm

import pandas as pd #数据处理模块
import re #正则表达式
import warnings
warnings.filterwarnings("ignore")  # 忽视Series数据类型处理空值数据异常

# 读取表格内容到data
data = pd.read_excel(r'job_data_spider.xls',sheet_name='Job')  # 读取Excel表格中sheet名为“job”的数据
result = pd.DataFrame(data)      # 将数据创建成二维表格，数据类型为DataFrame
table = result.dropna(axis=0,how='any')     # dropna()删除所有包含NaN的数据；axis=0，按行删除；how=‘any’指带缺失值的所有行/列
pd.set_option('display.max_rows',None)  # 输出全部行，'display.max_columns'显示所有列
# 由于删除含有空值的行，参与以下以下处理过程会出错，所以采用try...except...

# 公司地点转换成一样的市级地区
symbol1 = u'-'  # u前缀表示：后面字符串‘’可能含有中文字符
symbol_add = u'省'
add_list = table['公司地点']  # 得到某一列数据，数据类型为Series
for i in range(0,len(result)):
    try:
        if symbol1 in add_list[i]:
            add_div = add_list[i].split(symbol1)   # 以“-”进行拆分
            add_list[i] = add_div[0]
    except:
        pass

# 清洗学历要求的异常数据
symbol2 = u'人'
education_list = table['学历要求']
add_list = table['公司地点']
for i in range(0,len(result)):
    try:
        if symbol2 in education_list[i]:
            table = table.drop(i, axis=0) # drop()删除数据；axis=0，整行删除
        if symbol_add in add_list[i]:  # 将公司含有省份进行剔除
            table = table.drop(i, axis=0)
    except:
        pass

# 转换薪资单位
symbol3 =u'万/年'
symbol4 =u'千/月'
symbol5 =u'元/天'
symbol6 =u'元/小时'
salary_list = table['薪资']
for i in range(0,len(result)):
    try:
        if symbol3 in salary_list[i]:
            x = re.findall(r'\d*\.?\d+',salary_list[i]) #遍历匹配，获取字符串中所有匹配的字符串，返回一个列表
            min_ = format(float(x[0])/12,'.1f') #转换成浮点型并保留一位小数
            max_ = format(float(x[1])/12,'.1f')
            table['薪资'][i] = str(min_+'-'+max_+u'万/月')
        if symbol4 in salary_list[i]:
            x = re.findall(r'\d*\.?\d+',salary_list[i])
            min_ = format(float(x[0])/10,'.1f')
            max_ = format(float(x[1])/10,'.1f')
            table['薪资'][i] = str(min_ + '-' + max_ + u'万/月')
        if symbol5 in salary_list[i]:  #由于单位为'元/小时'和'元/天'数据较少，只做删除处理
            table = table.drop(i, axis=0)
        if symbol6 in salary_list[i]:
            table = table.drop(i, axis=0)
    except:
        pass

# 保存成另一个Excel文件,写入job中，且不覆盖
table.to_excel('job_data_analyses.xlsx', sheet_name='Job', index=False)