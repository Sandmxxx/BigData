import re

import pymongo
import pandas as pd  # 数据分析库
import re  # 正则表达式
from pyecharts.charts import Funnel, Pie, Geo, WordCloud, Line, Bar  # 导入pyecharts图表模块的漏斗图、饼图、Geo图等图绘制
from pyecharts import options as opts   # 导入图表配置模块
from pyecharts.globals import SymbolType   # 图片样式设置
import numpy as np   # 数学函数库


client = pymongo.MongoClient('10.242.100.213', 27017)
db = client['jobdb']

def save(db,jobName,dataList):
    # 创建集合
    collection = db[jobName]

    if len(dataList)>0:
        # 插入数据
        collection.insert_many(dataList)

    print("数据插入成功")


def delRepeatData(db):
    for jobName in db.list_collection_names():
        collection = db[jobName]
        # 删除集合中工作链接相同的重复数据，保留第一条
        # 查找重复的工作链接及对应的文档
        pipeline = [
            {"$sort": {"岗位链接": 1, "_id": 1}},  # 先按工作链接排序，再按_id（默认按插入顺序）排序
            {"$group": {
                "_id": "$岗位链接",
                "docs": {"$push": "$$ROOT"},
                "count": {"$sum": 1}
            }},
            {"$match": {"count": {"$gt": 1}}}  # 只保留重复的工作链接
        ]
        duplicates = list(collection.aggregate(pipeline, allowDiskUse=True))
        k = 0
        # 删除除第一条以外的重复数据
        for doc in duplicates:
            if doc["count"] > 1:
                keep_doc_id = doc["docs"][0]["_id"]
                for duplicate in doc["docs"][1:]:
                    collection.delete_one({"_id": duplicate["_id"]})
                    k = k + 1

        print(jobName +"数据去重成功" + '，去重的数量：' + str(k))

    # print("数据去重成功")


def get_unifiedSalary(salary):
    if type(salary) != str:
        return salary
    # 1.5千/天
    if re.findall(r'(.*)\千\/天', salary):
        s = re.findall(r'(.*)\千\/天', salary)[0]
        unifiedSalary = float(s) / 10 * 30
        # 保留2位小数
        unifiedSalary = round(unifiedSalary, 2)

    # 40-80万/年
    elif re.findall(r'(.*)\-(.*)\万\/\年', salary):
        s = re.findall(r'(.*)\-(.*)\万\/\年', salary)[0]
        avg = (float(s[0]) + float(s[1])) / 2.0
        unifiedSalary = avg / 12
        # 保留2位小数
        unifiedSalary = round(unifiedSalary, 2)

    # '9.5千-1.4万·13薪'
    elif re.findall(r'(.*)\千\-(.*)\万\·(.*)\薪', salary):
        s = re.findall(r'(.*)\千\-(.*)\万\·(.*)\薪', salary)[0]
        avg = (float(s[0]) / 10 + float(s[1])) / 2
        unifiedSalary = avg * float(s[2]) / 12
        # 保留2位小数
        unifiedSalary = round(unifiedSalary, 2)

    # '2-3万·13薪'
    elif re.findall(r'(.*)\-(.*)\万\·(.*)\薪', salary):
        s = re.findall(r'(.*)\-(.*)\万\·(.*)\薪', salary)[0]
        avg = (float(s[0]) + float(s[1])) / 2
        unifiedSalary = avg * float(s[2]) / 12
        # 保留2位小数
        unifiedSalary = round(unifiedSalary, 2)

    #  6-8千·13薪
    elif re.findall(r'(.*)\-(.*)\千\·(.*)\薪', salary):
        s = re.findall(r'(.*)\-(.*)\千\·(.*)\薪', salary)[0]
        avg = (float(s[0]) / 10 + float(s[1]) / 10) / 2
        unifiedSalary = avg * float(s[2]) / 12
        # 保留2位小数
        unifiedSalary = round(unifiedSalary, 2)

    # 6千-1.2万
    elif re.findall(r'(.*)\千\-(.*)\万', salary):
        s = re.findall(r'(.*)\千\-(.*)\万', salary)[0]
        avg = (float(s[0]) / 10 + float(s[1])) / 2
        unifiedSalary = avg
        # 保留2位小数
        unifiedSalary = round(unifiedSalary, 2)

    #  '6-8千'
    elif re.findall(r'(.*)\-(.*)\千', salary):
        s = re.findall(r'(.*)\-(.*)\千', salary)[0]
        avg = (float(s[0]) / 10 + float(s[1]) / 10) / 2
        unifiedSalary = avg
        # 保留2位小数
        unifiedSalary = round(unifiedSalary, 2)

    #  '6-8万'
    elif re.findall(r'(.*)\-(.*)\万', salary):
        s = re.findall(r'(.*)\-(.*)\万', salary)[0]
        unifiedSalary = (float(s[0]) + float(s[1])) / 2
        # 保留2位小数
        unifiedSalary = round(unifiedSalary, 2)

    else:
        unifiedSalary = 1.0

    return unifiedSalary

def unify_salary(db,jobName):
    collection = db[jobName]
    for doc in collection.find():
        salary = doc['薪资']
        unifiedSalary = get_unifiedSalary(salary)
        collection.update_one({'_id': doc['_id']}, {'$set': {'薪资': unifiedSalary}})
    print(jobName +'薪资统一完成')
def unify_experience(db):
    for jobName in db.list_collection_names():
        collection = db[jobName]
        for doc in collection.find():
            experience = doc['经验要求']
            # 获取经验要求中的出现的第一个数字,经验要求的形式有：3-4年，3年及以上，2年，3年-4年
            if re.findall(r'(.*)\年\-(.*)\年', experience):
                s = re.findall(r'(.*)\年\-(.*)\年', experience)[0]
                experience2 = s + '年'
            elif re.findall(r'(.*)\-', experience):
                s = re.findall(r'(.*)\-', experience)[0]
                experience2 = s + '年'
            elif re.findall(r'(.*)\年', experience):
                s = re.findall(r'(.*)\年', experience)[0]
                experience2 = s + '年'
            elif re.findall(r'(.*)\年\以上', experience):
                s = re.findall(r'(.*)\年\以上', experience)[0]
                experience2 = s + '年'
            else:
                experience2 = experience
            collection.update_one({'_id': doc['_id']}, {'$set': {'经验要求': experience2}})
        print(jobName +'经验要求统一完成')




def get_city_count_map(db,jobName):
    """
    统计指定职位在各个城市的数量。

    参数:
    - jobName: 字符串，指定的职位名称。

    返回值:
    - city_count_map: 字典，键为城市名，值为该职位在该城市的数量。
    """
    collection = db[jobName]

    # 初始化城市数量字典
    city_count_map = {}

    # 聚合操作，按城市分组统计职位数量
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$城市',  # 以城市字段作为分组依据
                'count': {'$sum': 1}  # 计算每组的文档数
            }
        },
        {
            '$sort': {'count': -1}  # 按计数值降序排序结果
        }
    ])

    # 遍历结果，填充城市数量字典
    for doc in result:
        if Geo().get_coordinate(doc['_id']):
            city_count_map[doc['_id']] = doc['count']
    return city_count_map

#城市招聘词云
def get_city_word(db,jobName):
    """
    城市招聘词云
    """
    collection = db[jobName]
    # 初始化城市数量字典
    city_word_map = []

    # 聚合操作，按城市分组统计职位数量
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$城市',  # 以城市字段作为分组依据
                'count': {'$sum': 1}  # 计算每组的文档数
            }
        },
        {
            '$sort': {'count': -1}  # 按计数值降序排序结果
        }
    ])

    # 遍历结果，填充城市数量字典
    for doc in result:
        city_word_map.append((doc['_id'],doc['count']))
    return city_word_map

#主要城市薪资关系
def get_primary_city_salary(db,jobName):
    collection = db[jobName]
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$城市',  # 以经验字段作为分组依据
                'avg_salary': {'$avg': '$薪资'}  # 计算每组的文档数
            }
        },
        {
            '$sort': {'avg_salary': -1}  # 按计数值升序排序结果
        }
    ])

    primary_city_salary = {}
    i=0
    for doc in result:
        if i>9:break
        if doc['_id'] != '':
            i=i+1
            primary_city_salary[doc['_id']] = round(doc['avg_salary'],2)
    return primary_city_salary

def get_company_type_count_dict(db,jobName):
    collection = db[jobName]
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$公司类型',  # 以公司类型字段作为分组依据
                'count': {'$sum': 1}  # 计算每组的文档数
            }
        },
        {
            '$sort': {'count': -1}  # 按计数值降序排序结果
        }
    ])

    company_type_count_dict = {}
    for doc in result:
        if doc['_id'] != '':
            company_type_count_dict[doc['_id']] = doc['count']
    return company_type_count_dict


def get_company_size_count_dict(db,jobName):
    collection = db[jobName]
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$公司规模',  # 以公司人数字段作为分组依据
                'count': {'$sum': 1}  # 计算每组的文档数
            }
        },
        {
            '$sort': {'count': -1}  # 按计数值降序排序结果
        }
    ])
    company_size_count_dict = {}
    for doc in result:
        if doc['_id'] != '':
            company_size_count_dict[doc['_id']] = doc['count']
    return company_size_count_dict


# 统计学历和薪资的关系
def get_degree_with_salary_relation(db,jobName):
    collection = db[jobName]
    # 统计学历和薪资的关系
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$学历要求',  # 以学历字段作为分组依据
                'avg_salary': {'$avg': '$薪资'}  # 计算每组的文档数
            }
        },
        {
            '$sort': {'avg_salary': 1}  # 按计数值升序排序结果
        }
    ])
    degree_with_salary_relation = {}
    for doc in result:
        if doc['_id'] != '':
            degree_with_salary_relation[doc['_id']] = round(doc['avg_salary'],2)
    return degree_with_salary_relation

# 统计工作经验和薪资的关系
def get_experience_with_salary_relation(db,jobName):
    collection = db[jobName]
    # 统计工作经验和薪资的关系
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$经验要求',  # 以学历字段作为分组依据
                'avg_salary': {'$avg': '$薪资'}  # 计算每组的文档数
            }
        },
        {
            '$sort': {'avg_salary': 1}  # 按计数值升序排序结果
        }
    ])
    degree_with_salary_relation = {}
    for doc in result:
        if doc['_id'] != '':
            degree_with_salary_relation[doc['_id']] = round(doc['avg_salary'],2)
    return degree_with_salary_relation


def get_experience_requirements(db,jobName):
    collection = db[jobName]
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$经验要求',  # 以经验字段作为分组依据
                'count': {'$sum': 1}  # 计算每组的文档数
            }
        },
        {
            '$sort': {'count': -1}  # 按计数值降序排序结果
        }
    ])
    experience_requirements = []
    for doc in result:
        if doc['_id'] != '':
            experience_requirements.append((doc['_id'],round(doc['count'],2)))
    return experience_requirements


#公司规模-薪资关系
def get_salary_with_company_size_relation(db,jobName):
    collection = db[jobName]
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$公司规模',  # 以经验字段作为分组依据
                'avg_salary': {'$avg': '$薪资'}  # 计算每组的文档数
            }
        },
        # {
        #     '$sort': {'avg_salary': -1}  # 按计数值降序排序结果
        # }
    ])
    size=['少于50人','50-150人','150-500人','500-1000人','1000-5000人','5000-10000人','10000人以上']
    salary_with_comany_size_relation = {}
    for doc in result:
        if doc['_id'] != '':
            salary_with_comany_size_relation[doc['_id']] = round(doc['avg_salary'],2)
    res={}
    for i in size:
        if salary_with_comany_size_relation.get(i) != None:
            res[i]=salary_with_comany_size_relation[i]
        else: res[i]=0
    return res

def get_industry_distribution(db,jobName):
    collection = db[jobName]
    # 获取每个行业出现的次数
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$行业类型',  # 以行业字段作为分组依据
                'count': {'$sum': 1}  # 计算每组的文档数
            }
        },
        {
            '$sort': {'count': -1}  # 按计数值降序排序结果
        }
    ])
    industry_distribution = {}
    for doc in result:
        if doc['_id'] != '':
            industry_distribution[doc['_id']] = doc['count']
    return industry_distribution


def get_welfare_distribution(db,jobName):
    collection = db[jobName]
    result = collection.aggregate([
        {
            "$project": {
                "welfare": {"$split": ["$工作福利", "/"]}
            }
        },
        {
            "$unwind": "$welfare"
        },
        {
            "$group": {
                "_id": "$welfare",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        }
    ])
    welfare_distribution = []
    for doc in result:
        if doc['_id'] != '':
            welfare_distribution.append((doc['_id'],doc['count']))
    return welfare_distribution

def get_job_salary_distribution(db):
    job_salary_distribution = {}
    for jobName in db.list_collection_names():
        job_salary_distribution[jobName] = get_experience_requirements(db,jobName)
    return job_salary_distribution

def get_education_requirements(db,jobName):
    collection = db[jobName]
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$学历要求',  # 以行业字段作为分组依据
                'count': {'$sum': 1}  # 计算每组的文档数
            }
        },
        {
            '$sort': {'count': -1}  # 按计数值降序排序结果
        }
    ])
    education_requirements=[]
    for doc in result:
        if doc['_id'] != '':
            education_requirements.append((doc['_id'],doc['count']))
    return education_requirements


def get_job_name(db):
    job_name_list = db.list_collection_names()
    return job_name_list
# if __name__ == '__main__':
#     # test()
#     delRepeatData()

def getAllData(db,jobName):
    # 获取所有数据到Pandas的DataFrame
    df = pd.DataFrame()

    delBadData(db,jobName)
    collection = db[jobName]
    result = collection.find()
    df = df._append(pd.DataFrame(list(result)))

    # 删除id列
    df = df.drop('_id', axis=1)
    df.dropna(inplace=True)
    return df

def delBadData(db,jobName):
    collection = db[jobName]
    # 删除数据库中城市带“·”的数据
    result1 = collection.delete_many({'城市': {'$regex': '·'}})
    result2 = collection.delete_many({'薪资': {'$not': {'$type': 'double'}}})
    # 打印删除的数据条数
    # 打印被删除的文档数量
    print(f"Deleted {result1.deleted_count} documents with '·' in the city field.")
    print(f"Deleted {result2.deleted_count} documents with non-double values in the salary field.")
    # 删除数据库中薪资不是double类型的数据





#公司规模
def get_cop_scale(db,jobName):
    collection=db[jobName]
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$公司规模',  # 以行业字段作为分组依据
                'count': {'$sum': 1}  # 计算每组的文档数
            }
        }
    ])
    cop_scale = []
    for doc in result:
        if doc['_id'] != '':
            cop_scale.append((doc['_id'], int(doc['count'])))
    return cop_scale


#公司行业分布
def get_cop_industry(db,jobName):
    collection=db[jobName]
    result = collection.aggregate([
        {
            "$project": {
                "industry": {"$split": ["$行业类型", "/"]}
            }
        },
        {
            "$unwind": "$industry"
        },
        {
            "$group": {
                "_id": "$industry",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        }
    ])
    cop_industry=[]
    # 只取前五十
    i=0
    for doc in result:
        if doc['_id'] != '' and i<50:
            cop_industry.append((doc['_id'], int(doc['count'])))
            i=i+1
    return cop_industry

