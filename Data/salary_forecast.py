import numpy as np
import pandas as pd
import pymongo
from skimage.metrics import mean_squared_error
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from sklearn.tree import DecisionTreeRegressor
from sklearn.model_selection import train_test_split

from Data.mongodb import getAllData


# 变量转换
citys_code = {
    '北京': 1, '上海': 2, '广州': 3, '深圳': 4,
    '厦门': 5, '武汉': 6, '西安': 7, '杭州': 8, '南京': 9,
    '成都': 10, '重庆': 11, '东莞': 12, '大连': 13,
    '沈阳': 14, '苏州': 15, '昆明': 16, '长沙': 17,
    '合肥': 18, '宁波': 19, '郑州': 20, '天津': 21,
    '青岛': 22, '济南': 23, '哈尔滨': 24, '长春': 25,
    '福州': 26, '珠三角': 27
}
experience_code = {
    '无需经验': 0, '1年': 1, '2年': 2, '3年': 3, '4年': 4, '5年': 5, '6年': 6, '7年': 7, '8年': 8,
    '9年': 9, '10年': 10, '在校生/应届生': 11, '其他': 12
}
degree_code = {
    '不限': 0, '大专': 1, '本科': 2, '硕士': 3, '博士': 4, '中专': 5, '高中': 6, '初中': 7, '中专/中技': 8, '其他': 9
}
company_size_code = {
    '不限': 0, '少于50人': 1, '50-100人': 2, '150-500人': 3, '500-1000人': 4, '1000-5000人': 6,
    '5000-10000人': 7, '10000人以上': 8
}


# 根据爬取的招聘信息，进行薪资预测
def get_salary_forecast_model(df):
    # 提取数据
    X = df[['城市', '经验要求', '学历要求', '公司规模', '薪资']]
    XX = X.copy()
    # 变量转换
    XX['城市'] = X['城市'].map(citys_code)
    XX['经验要求'] = X['经验要求'].map(experience_code)
    XX['学历要求'] = X['学历要求'].map(degree_code)
    XX['公司规模'] = X['公司规模'].map(company_size_code)
    # 删除缺失值，更新数据
    XX = XX.dropna()
    XX.reset_index(drop=True, inplace=True)
    X = XX[['城市', '经验要求', '学历要求', '公司规模']]
    print(X)

    y = XX['薪资']
    print(y)
    # 模型训练
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    linReg = LinearRegression()
    linReg.fit(X_train, y_train)
    print('线性回归模型训练完成')
    # 模型评估
    print('模型评估：', linReg.score(X_test, y_test))
    # print('模型预测：', model.predict([[1, 3, 2, 8]]))
    y_pred_linReg = linReg.predict(X_test)
    r2_linReg = r2_score(y_test, y_pred_linReg)
    mse_linReg = mean_squared_error(y_test, y_pred_linReg)
    print("Linear Regression - R²: {:.2f}, MSE: {:.2f}".format(r2_linReg, mse_linReg))

    return linReg

    # decTree = DecisionTreeRegressor()
    # decTree.fit(X_train, y_train)
    # print('决策树模型训练完成')
    # # 模型评估
    # print('模型评估：', decTree.score(X_test, y_test))
    # y_pred_decTree = decTree.predict(X_test)
    # r2_decTree = r2_score(y_test, y_pred_decTree)
    # mse_decTree = mean_squared_error(y_test, y_pred_decTree)
    # print("Decision Tree - R²: {:.2f}, MSE: {:.2f}".format(r2_decTree, mse_decTree))
    #
    # randomForest = RandomForestRegressor()
    # randomForest.fit(X_train, y_train)
    # print('随机森林模型训练完成')
    # print('模型评估：', randomForest.score(X_test, y_test))
    # y_pred_randomForest = randomForest.predict(X_test)
    # r2_randomForest = r2_score(y_test, y_pred_randomForest)
    # mse_randomForest = mean_squared_error(y_test, y_pred_randomForest)
    # print("Random Forest - R²: {:.2f}, MSE: {:.2f}".format(r2_randomForest, mse_randomForest))

def salary_forecast(jobName,city,experience,degree,company_size):
    client = pymongo.MongoClient('localhost', 27017)
    db = client['jobdb']
    df = getAllData(db,jobName)
    linReg = get_salary_forecast_model(df)
    salary = linReg.predict([[citys_code[city], experience_code[experience], degree_code[degree], company_size_code[company_size]]])
    print('预测薪资为：',salary)
    return salary

if __name__ == '__main__':
    salary_forecast('Java', '北京', '3年', '本科', '50-100人')
