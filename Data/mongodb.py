import re

import pymongo

def save(db,jobName,dataList):
    # 创建集合
    collection = db[jobName]

    # 插入数据
    collection.insert_many(dataList)

    print("数据插入成功")


def delRepeatData(db):
    for jobName in db.list_collection_names():
        collection = db[jobName]
        # 删除集合中工作链接相同的重复数据，保留第一条
        # 查找重复的工作链接及对应的文档
        pipeline = [
            {"$sort": {"jobLink": 1, "_id": 1}},  # 先按工作链接排序，再按_id（默认按插入顺序）排序
            {"$group": {
                "_id": "$jobLink",
                "docs": {"$push": "$$ROOT"},
                "count": {"$sum": 1}
            }},
            {"$match": {"count": {"$gt": 1}}}  # 只保留重复的工作链接
        ]
        duplicates = list(collection.aggregate(pipeline, allowDiskUse=True))

        # 删除除第一条以外的重复数据
        for doc in duplicates:
            if doc["count"] > 1:
                keep_doc_id = doc["docs"][0]["_id"]
                for duplicate in doc["docs"][1:]:
                    collection.delete_one({"_id": duplicate["_id"]})

    print("数据去重成功")


def get_unifiedSalary(salary):
    unifiedSalary = 0.0
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
        unifiedSalary = salary

    return unifiedSalary
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
        city_count_map[doc['_id']] = doc['count']
    return city_count_map


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
            '$sort': {'avg_salary': -1}  # 按计数值降序排序结果
        }
    ])
    degree_with_salary_relation = {}
    for doc in result:
        if doc['_id'] != '':
            degree_with_salary_relation[doc['_id']] = round(doc['avg_salary'],2)
    return degree_with_salary_relation

def get_salary_with_experience_relation(db,jobName):
    collection = db[jobName]
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$经验要求',  # 以经验字段作为分组依据
                'avg_salary': {'$avg': '$薪资'}  # 计算每组的文档数
            }
        },
        {
            '$sort': {'avg_salary': -1}  # 按计数值降序排序结果
        }
    ])
    salary_with_experience_relation = {}
    for doc in result:
        if doc['_id'] != '':
            salary_with_experience_relation[doc['_id']] = round(doc['avg_salary'],2)
    return salary_with_experience_relation

def get_salary_with_comany_size_relation(db,jobName):
    collection = db[jobName]
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$公司规模',  # 以经验字段作为分组依据
                'avg_salary': {'$avg': '$薪资'}  # 计算每组的文档数
            }
        },
        {
            '$sort': {'avg_salary': -1}  # 按计数值降序排序结果
        }
    ])
    salary_with_comany_size_relation = {}
    for doc in result:
        if doc['_id'] != '':
            salary_with_comany_size_relation[doc['_id']] = round(doc['avg_salary'],2)
    return salary_with_comany_size_relation

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

def get_skill_requirements_distribution(db,jobName):
    collection = db[jobName]
    result = collection.aggregate([
        {
            "$project": {
                "skills": {"$split": ["$技能要求", "/"]}
            }
        },
        {
            "$unwind": "$skills"
        },
        {
            "$group": {
                "_id": "$skills",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        }
    ])
    skill_distribution = {}
    for doc in result:
        if doc['_id'] != '':
            skill_distribution[doc['_id']] = doc['count']
    return skill_distribution

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
    welfare_distribution = {}
    for doc in result:
        if doc['_id'] != '':
            welfare_distribution[doc['_id']] = doc['count']
    return welfare_distribution

def get_job_salary_distribution(db):
    job_salary_distribution = {}
    for jobName in db.list_collection_names():
        job_salary_distribution[jobName] = get_salary_with_experience_relation(db,jobName)
    return job_salary_distribution


# if __name__ == '__main__':
#     # test()
#     delRepeatData()
