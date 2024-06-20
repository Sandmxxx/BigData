# -*- coding = utf-8 -*-
# @Time: 2023/2/5 22:06
# @Author: Watson
# @File: spider.py
# @Software: PyCharm

import pandas as pd  # 数据分析库
import re  # 正则表达式
from pyecharts.charts import Funnel, Pie, Geo, WordCloud, Line, Bar  # 导入pyecharts图表模块的漏斗图、饼图、Geo图等图绘制
from pyecharts import options as opts   # 导入图表配置模块
from pyecharts.globals import SymbolType   # 图片样式设置
import numpy as np   # 数学函数库
import openpyxl

file = pd.read_excel(r'job_data_analyses.xlsx',sheet_name='Job') #读取Excel表格中sheet名为“job”的数据
f = pd.DataFrame(file) # 将数据创建成二维表格，数据类型为DataFrame
pd.set_option('display.max_rows',None)   # 输出全部行，'display.max_columns'显示所有列

# 提取表中谋列数据，数据类型为Series
add = f['公司地点']
cha = f['公司性质']
sal = f['薪资']
edu = f['学历要求']
exp = f['工作经验']
wel = f['公司福利']
# 创建空列表
address =[]
character = []
salary = []
education = []
experience = []
welfare = []
sal_mean = []

# 将数据类型为Series转换成列表形式
for i in range(0,len(f)):
    try:
        address.append(add[i])
        character.append(cha[i])
        temp_sal = re.findall(r'\d*\.?\d+',sal[i]) # 遍历匹配，获取字符串中所有匹配的字符串，返回一个列表
        sal_mean.append(round(float(temp_sal[0]), 1)) # 取最低薪资
        education.append(edu[i])
        experience.append(exp[i])
        temp_wel = wel[i].split(' ')  # 分隔公司福利
        for i in range(len(temp_wel)):
            welfare.append(temp_wel[i])
    except:
        pass

# 统计各地区招聘个数，并对Geo图中不存在的地理位置进行删除
def get_address(list):
    address2 = {}
    for i in set(list):
        address2[i] = list.count(i)  # 计数
    try:
        address2.pop('燕郊开发区')  # 删除地图上可能不合法或者没有的地名
        address2.pop('黔南')
    except:
        pass
    return address2
dir2 = get_address(address)
attr2 = dir2.keys()
value2 = dir2.values()

# 各地区招聘需求分布
c = (
    Geo()
    .add_schema(maptype="china")  # 选择中国地图
    .add("Geo", [list(z) for z in zip(attr2, value2)])   #导入数据
    .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    .set_global_opts(    # 设置图片配置
        visualmap_opts=opts.VisualMapOpts(),
        title_opts=opts.TitleOpts(title="各地区招聘需求分布")
    )
    .render("../templates/各地区招聘需求分布.html")  # 将图片存入HTML中
)

# 对输入列表元素进行统计，返回列表
def list_count(inlist):
    temp_dict = {}
    for i in set(inlist):  # set(list)顾名思义是集合，里面不能包含重复的元素，接收一个list作为参数
        temp_dict[i] = inlist.count(i)  # 返回列表某元素的出现次数
    outlist = []
    for key in temp_dict.items():
        outlist.append(key)
    return outlist

# 调用自定义方法，获取绘图数据
add_word = list_count(address)
#各城市招聘需求词云
c = (
    WordCloud()
    .add("", add_word, word_size_range=[20, 100], shape=SymbolType.DIAMOND)
    .set_global_opts(title_opts=opts.TitleOpts(title="各城市招聘需求词云"))
    .render("../templates/各城市招聘需求词云.html")
)

# 对薪资——学历，薪资——经验等进行统计，返回字典
def statistics(key_list,val_list):
    outdict = {}
    for i in set(key_list):
        outdict[i] = []  # 建立对应字典
    for i in range(len(key_list)):
        outdict[key_list[i]].append(val_list[i])  # 加入薪资数据
    for key in outdict.keys():
        outdict[key] = round(np.mean(outdict[key]),1)   #对应数据求平均
    return outdict

# 将字典数据按特定顺序的键提取数值，返回列表
def data_sort(sort_key,data_dict):
    sort_val = []
    for i in sort_key:
        sort_val.append(data_dict[i])
    return sort_val

# 调用自定义方法，获取绘图数据
sal_add = statistics(address,sal_mean)
add_sort = ['福州', '厦门', '泉州', '北京', '上海', '广州', '深圳',
            '杭州', '南京', '苏州', '重庆', '成都', '武汉', '合肥', '长沙', '济南', '郑州']
sal_exp_sort = data_sort(add_sort,sal_add)
print(sal_exp_sort)
# 主要城市薪资关系
c = (
    Bar()
    .add_xaxis(add_sort)
    .add_yaxis("主要城市薪资关系", sal_exp_sort, color='LightCoral')
    .set_global_opts(
        title_opts=opts.TitleOpts(title="主要城市薪资关系"),
        yaxis_opts=opts.AxisOpts(name="薪资：万元/月"),
    )
    .render("../templates/主要城市薪资关系.html")
)

# 调用自定义方法，获取绘图数据
sal_edu = statistics(education,sal_mean)
edu_sort = ['高中','中专', '大专', '本科', '硕士', '博士']
sal_edu_sort = data_sort(edu_sort,sal_edu)
# 薪资与学历关系
c = (
    Line()
    .add_xaxis(edu_sort)
    .add_yaxis("薪资与学历关系", sal_edu_sort)
    .set_global_opts(
        title_opts=opts.TitleOpts(title="薪资与学历关系"),
        yaxis_opts=opts.AxisOpts(name="薪资：万元/月"),
    )
    .render("../templates/薪资与学历关系.html")
)

# 调用自定义方法，获取绘图数据
sal_exp = statistics(experience,sal_mean)
exp_sort = ['无需经验', '1年经验', '2年经验', '3-4年经验', '5-7年经验', '8-9年经验', '10年以上经验']
sal_exp_sort = data_sort(exp_sort,sal_exp)
print(sal_exp)
print(sal_exp_sort)
# 薪资与工作经验关系
c = (
    Line()
    .add_xaxis(exp_sort)
    .add_yaxis("薪资与工作经验关系", sal_exp_sort)
    .set_global_opts(
        title_opts=opts.TitleOpts(title="薪资与工作经验关系"),
        yaxis_opts=opts.AxisOpts(name="薪资：万元/月"),
    )
    .render("../templates/薪资与工作经验关系.html")
)

# 调用自定义方法，获取绘图数据
edu_per = list_count(education)
print(edu_per)
# 学历要求分析
c = (
    Pie()
    .add(
        "",
        [list(z) for z in edu_per], #导入数据
        radius=["40%", "75%"], #设置中间扇区规格
        rosetype="radius" , # 扇区圆心角展现数据的百分比，半径展现数据的大小
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(title="学历要求分析"), # 设置标题
        legend_opts=opts.LegendOpts(orient="vertical", pos_top="15%", pos_left="2%"), # 设置图例参数
    )
    .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {d}%")) # 数字项名称和百分比
    .render("../templates/学历要求分析.html") # 将图形保存为HTML文件中
)

# 调用自定义方法，获取绘图数据
exp_per = list_count(experience)
print(exp_per)
# 工作经验要求分析
c = (
    Funnel()
    .add(
        "",
        [list(z) for z in exp_per],
        label_opts=opts.LabelOpts(position="inside"), # 显示的占比嵌入到图中
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(title="工作经验要求分析"),
        legend_opts=opts.LegendOpts(orient="vertical", pos_top="15%", pos_left="2%"), #设 置图例参数
    )
    .render("../templates/工作经验要求分析.html")
)

# 调用自定义方法，获取绘图数据
cha_per = list_count(character)
# 公司性质占比分析
c = (
    Pie()
    .add(
        "",
        [list(z) for z in cha_per], # 导入数据
        radius=["40%", "75%"], # 设置中间扇区规格
        rosetype="radius" ,  # 扇区圆心角展现数据的百分比，半径展现数据的大小
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(title="公司性质占比分析"), # 设置标题
        legend_opts=opts.LegendOpts(orient="vertical", pos_top="15%", pos_left="2%"), # 设置图例参数
    )
    .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {d}%")) # 数字项名称和百分比
    .render("../templates/公司性质占比分析.html") # 将图形保存为HTML文件中
)

# 调用自定义方法，获取绘图数据
wel_word = list_count(welfare)
# 公司福利词云
c = (
    WordCloud()
    .add("", wel_word, word_size_range=[20, 100], shape=SymbolType.DIAMOND)
    .set_global_opts(title_opts=opts.TitleOpts(title="公司福利词云"))
    .render("../templates/公司福利词云.html")
)