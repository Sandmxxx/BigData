import numpy as np
import pymongo

import Data.mongodb as data

import logging
from datetime import timedelta
from flask import Flask, render_template, request,send_file,send_from_directory
from pyecharts.charts import Funnel, Pie, Geo, WordCloud, Line, Bar  # 导入pyecharts图表模块的漏斗图、饼图、Geo图等图绘制
from pyecharts import options as opts   # 导入图表配置模块
from pyecharts.globals import SymbolType   # 图片样式设置

from Data.salary_forecast import salary_forecast

#from gevent import pywsgi

app = Flask(__name__)
# 解决缓存刷新问题
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = timedelta(seconds=2)
# 原日志
logging.getLogger('werkzeug').setLevel(logging.ERROR)
# 文件输出日志 自写日志
handler = logging.FileHandler(filename='log.txt', mode='a')
handler.setLevel(logging.ERROR)
app.logger.addHandler(handler)

#数据库配置
client = pymongo.MongoClient('10.242.100.213', 27017)
db = client['jobdb']

#获取所有职业类型名字
@app.route("/")
def list_job():
    job_name_list=data.get_job_name(db)
    return render_template("index.html",list=job_name_list)

#初始职业类型
jobName="PHP"

@app.route("/job_type")
def get_job_name():
    global jobName
    jobName = request.values.get("jobName")
    print(jobName+"!")
    return index()


@app.route("/")
def index():
    return render_template('index.html')

# 各地区招聘需求分布
@app.route("/index")
def index_():
    city_count_map=data.get_city_count_map(db,jobName)
    c=(
        Geo().add_schema(maptype="china")  # 选择中国地图
        .add("Geo", [list(z) for z in zip(city_count_map.keys(), city_count_map.values())])  # 导入数据
        .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
        .set_global_opts(  # 设置图片配置
            visualmap_opts=opts.VisualMapOpts(),
            title_opts=opts.TitleOpts(title="各地区招聘需求分布")
        )
        .render("templates/各地区招聘需求分布.html")  # 将图片存入HTML中
    )
    return render_template('各地区招聘需求分布.html')

# 各城市招聘需求词云
@app.route("/city_recruit")
def city_recruit():
    print(jobName)
    city_word=data.get_city_word(db,jobName)
    c = (
        WordCloud()
        .add("", city_word, word_size_range=[20, 100], shape=SymbolType.DIAMOND)
        .set_global_opts(title_opts=opts.TitleOpts(title="各城市招聘需求词云"))
        .render("templates/各城市招聘需求词云.html")
    )
    return render_template('各城市招聘需求词云.html')

# 主要城市薪资关系
@app.route("/salary_city")
def salary_city():
    salary_city=data.get_primary_city_salary(db,jobName)
    x=[]
    dt=[]
    for key,value in salary_city.items():
        x.append(key)
        dt.append(value)
    c = (
        Bar(opts.InitOpts(width='1400px'))
        .add_xaxis(xaxis_data=x)
        .add_yaxis("主要城市薪资关系", dt, color='LightCoral')
        .set_global_opts(
            title_opts=opts.TitleOpts(title="主要城市薪资关系"),
            yaxis_opts=opts.AxisOpts(name="薪资：万元/月"),
        )
        .render("templates/主要城市薪资关系.html")
    )
    return render_template('主要城市薪资关系.html')


#薪资与学历关系
@app.route("/salary_education")
def salary_education():
    degree_with_salary_relation=data.get_degree_with_salary_relation(db,jobName)
    degree = []
    salary = []
    for key, value in degree_with_salary_relation.items():
        degree.append(key)
        salary.append(value)
    c = (
        Line()
        .add_xaxis(xaxis_data=degree)
        .add_yaxis("薪资与学历关系", salary)
        .set_global_opts(
            title_opts=opts.TitleOpts(title="薪资与学历关系"),
            yaxis_opts=opts.AxisOpts(name="薪资：万元/月"),
        )
        .render("templates/薪资与学历关系.html")
    )
    return render_template('薪资与学历关系.html')

# 薪资与工作经验关系
@app.route("/salary_experience")
def salary_experience():
    experience_with_salary_relation=data.get_experience_with_salary_relation(db,jobName)
    experience = []
    salary = []
    for key, value in experience_with_salary_relation.items():
        experience.append(key)
        salary.append(value)
    # 薪资与工作经验关系
    c = (
        Line(opts.InitOpts(width='1400px'))
        .add_xaxis(xaxis_data=experience)
        .add_yaxis("薪资与工作经验关系", salary)
        .set_global_opts(
            title_opts=opts.TitleOpts(title="薪资与工作经验关系"),
            yaxis_opts=opts.AxisOpts(name="薪资：万元/月"),
        )
        .render("templates/薪资与工作经验关系.html")
    )
    return render_template('薪资与工作经验关系.html')

# 学历要求分析
@app.route("/education_require")
def education_require():
    education_requirements=data.get_education_requirements(db,jobName)
    c = (
        Pie()
        .add(
            "",
            [list(z) for z in education_requirements],  # 导入数据
            radius=["40%", "75%"],  # 设置中间扇区规格
            rosetype="radius",  # 扇区圆心角展现数据的百分比，半径展现数据的大小
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title="学历要求分析"),  # 设置标题
            legend_opts=opts.LegendOpts(orient="vertical", pos_top="15%", pos_left="2%"),  # 设置图例参数
        )
        .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {d}%"))  # 数字项名称和百分比
        .render("templates/学历要求分析.html")  # 将图形保存为HTML文件中
    )
    return render_template('学历要求分析.html')

# 工作经验要求分析
@app.route("/experience_require")
def experience_require():
    experience_requirements = data.get_experience_requirements(db, jobName)
    c = (
        Funnel()
        .add(
            "",
            [list(z) for z in experience_requirements],
            label_opts=opts.LabelOpts(position="inside"),  # 显示的占比嵌入到图中
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title="工作经验要求分析"),
            legend_opts=opts.LegendOpts(orient="vertical", pos_top="15%", pos_left="2%"),  # 设 置图例参数
        )
        .render("templates/工作经验要求分析.html")
    )
    return render_template('工作经验要求分析.html')

# 公司福利词云
@app.route("/cop_welfare")
def cop_welfare():
    welfare_distribution=data.get_welfare_distribution(db,jobName)
    c = (
        WordCloud()
        .add("", welfare_distribution, word_size_range=[20, 100], shape=SymbolType.DIAMOND)
        .set_global_opts(title_opts=opts.TitleOpts(title="公司福利词云"))
        .render("templates/公司福利词云.html")
    )
    return render_template('公司福利词云.html')

# 公司性质占比分析
@app.route("/cop_character")
def cop_character():
    company_type_count_dict=data.get_company_type_count_dict(db,jobName)
    print(company_type_count_dict)
    c = (
        Pie()
        .add(
            "",
            [list(z) for z in company_type_count_dict],  # 导入数据
            radius=["40%", "75%"],  # 设置中间扇区规格
            rosetype="radius",  # 扇区圆心角展现数据的百分比，半径展现数据的大小
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title="公司性质占比分析"),  # 设置标题
            legend_opts=opts.LegendOpts(orient="vertical", pos_top="15%", pos_left="2%"),  # 设置图例参数
        )
        .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {d}%"))  # 数字项名称和百分比
        .render("templates/公司性质占比分析.html")  # 将图形保存为HTML文件中
    )
    return render_template('公司性质占比分析.html')

# 公司规模分布图
@app.route("/cop_scale")
def cop_scale():
    cop_scale=data.get_cop_scale(db,jobName)
    c = (
        Pie(init_opts=opts.InitOpts(width='800px', height='600px'))
        .add('', cop_scale, radius=["10%", "40%"], rosetype='area')
        .set_global_opts(title_opts=opts.TitleOpts(title='公司规模分布图'),
                         legend_opts=opts.LegendOpts(type_='scroll', pos_left="5%", pos_bottom='5%',
                                                    orient="horizontal"))
        .set_series_opts(label_opts=opts.LabelOpts(formatter='{b}: {c}({d}%)'))
        .render("templates/公司规模分布图.html")
    )
    return render_template('公司规模分布图.html')

# 公司行业分布分析
@app.route("/cop_industry")
def cop_industry():
    cop_industry=data.get_cop_industry(db,jobName)
    c = (
        Pie(init_opts=opts.InitOpts(width='800px', height='600px'))
        .add('', cop_industry, radius=["10%", "40%"], rosetype='area')
        .set_global_opts(title_opts=opts.TitleOpts(title='公司行业分布图'),
                         legend_opts=opts.LegendOpts(type_='scroll', pos_left="5%", pos_bottom='5%',
                                                    orient="horizontal"))
        .set_series_opts(label_opts=opts.LabelOpts(formatter='{b}: {c}({d}%)'))
        .render('templates/公司行业分布分析.html')
    )
    return render_template('公司行业分布分析.html')

# 公司规模薪资关系
@app.route("/salary_scale_relation")
def salary_scale_relation():
    salary_scale_relation=data.get_salary_with_company_size_relation(db,jobName)
    x_data_e=[]
    y_data_e=[]
    for key,value in salary_scale_relation.items():
        x_data_e.append(key)
        y_data_e.append(value)
    c = (
        Line(init_opts=opts.InitOpts(width='1000px', height='600px'))
        .add_xaxis(xaxis_data=x_data_e)
        .add_yaxis('最高学历平均薪资', y_data_e, linestyle_opts=opts.LineStyleOpts(width=2))
        .set_global_opts(title_opts=opts.TitleOpts(title='公司规模薪资关系'),
                         xaxis_opts=opts.AxisOpts(name='公司规模'),
                         yaxis_opts=opts.AxisOpts(name='平均薪资（万元/月）'))
        .render('templates/公司规模薪资关系.html')
    )
    return render_template('公司规模薪资关系.html')


# 薪资预测
cities=['北京', '上海', '广州', '深圳', '厦门', '武汉', '西安', '杭州', '南京', '成都', '重庆', '东莞', '大连', '沈阳', '苏州', '昆明', '长沙', '合肥', '宁波', '郑州', '天津', '青岛', '济南', '哈尔滨', '长春', '福州', '珠三角']
experience=['无需经验', '1年', '2年', '3年', '4年', '5年', '6年', '7年', '8年', '9年', '10年', '在校生/应届生', '其他']
degree=['不限', '大专', '本科', '硕士', '博士', '中专', '高中', '初中', '中专/中技', '其他']
company_size=['不限', '少于50人', '50-100人', '150-500人', '500-1000人', '1000-5000人', '5000-10000人', '10000人以上']
Direction=''
City=''
Experience=''
Degree=''
Scale=''
# 薪资预测
@app.route("/salary_forecast1")
def salary_forecast1():
    job_name_list = data.get_job_name(db)
    return render_template('薪资预测.html',list=job_name_list,list1=cities,list2=experience,list3=degree,list4=company_size)
@app.route("/salary_forecast2")
def salary_forecast2():
    global Direction
    Direction = request.values.get("direction")
    global City
    City = request.values.get("city")
    global Experience
    Experience = request.values.get("experience")
    global Degree
    Degree = request.values.get("degree")
    global Scale
    Scale = request.values.get("scale")
    print(Direction+' '+City+' '+Experience+' '+Degree+' '+Scale)
    return str(salary_forecast(db,Direction,City,Experience,Degree,Scale)[0])



if __name__ == '__main__':
    app.run(debug=True,host='127.0.0.1',port=80)