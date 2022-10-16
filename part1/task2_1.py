from copy import deepcopy
from pyspark import SparkContext
import time
import collections
import math
from itertools import combinations
import sys
from hash_family import UniversalStringHashFamily
sys.stdout = open("log.txt", "w")

if __name__ == "__main__":
    train_file_path = sys.argv[1]
    test_file_path = sys.argv[2]
    output_file_path = sys.argv[3]
    SparkContext.setSystemProperty('spark.executor.memory', '4g')
    # SparkContext.setSystemProperty('spark.executor.cores', '8')
    SparkContext.setSystemProperty("spark.driver.memory", "4g")
    # SparkContext.setSystemProperty('spark.executor.instances', '20')
    start = time.time()
    sc = SparkContext('local[*]', 'task2_1')
    trainRDD = sc.textFile(train_file_path)
    # header = trainRDD.first()

    # RDD = trainRDD
    # unique_user_ids = RDD.map(lambda element: element.split(',')[0]).distinct().collect()
    # unique_user_ids = sorted(unique_user_ids[1:])
    # print("name: ", unique_user_ids[0])

    def user(element):
        temp = element.split(',')
        user_id = temp[0]
        business_id = temp[1]
        rating = temp[2]
        return user_id, (business_id, rating)

    def business(element):
        temp = element.split(',')
        # user_id = temp[0]
        business_id = temp[1]
        rating = temp[2]
        return business_id, float(rating)

    # {"business_id":[(user_id, rating)]}
    # [{'3MntE_HWbNNoyiLGxywjYA': [('T13IBpJITI32a1k41rc-tg', '5.0'), ('xhlcoVm3FOKcxZ0phkdO6Q', '4.0')]}
    # list_train = trainRDD.map(f).groupByKey().map(lambda element: {element[0]: list(element[1])})

    train_mean = trainRDD.filter(lambda row: row != "user_id,business_id,stars")\
        .map(business)\
        .groupByKey()\
        .map(lambda element: (element[0], sum(list(element[1]))/len(list(element[1]))))

    list_train_mean = train_mean.collectAsMap()
    print(list_train_mean['3MntE_HWbNNoyiLGxywjYA'])

    # train = trainRDD.map(business).groupByKey()
    # list_train_business = train.collect()
    # '3MntE_HWbNNoyiLGxywjYA'



    # testRDD = sc.textFile(test_file_path)
    # list_test = testRDD.map(f).groupByKey().map(lambda element: {element[0]: list(element[1])})
    # list_test = list_test.collect()
    # print(list_test[0])
