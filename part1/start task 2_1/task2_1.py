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
    sc = SparkContext('local[*]', 'task1')
    trainRDD = sc.textFile(train_file_path)


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
        return business_id, rating

    # {"business_id":[(user_id, rating)]}
    # [{'3MntE_HWbNNoyiLGxywjYA': [('T13IBpJITI32a1k41rc-tg', '5.0'), ('xhlcoVm3FOKcxZ0phkdO6Q', '4.0')]}
    # list_train = trainRDD.map(f).groupByKey().map(lambda element: {element[0]: list(element[1])})
    train = trainRDD.map(business).groupByKey().map(lambda element: (element [0],list(element[1])))

    sum_count_rdd = train.aggregateByKey((0, 0), lambda U, v: (U[0] + v, U[1] + 1), lambda U1, U2: (U1[0] + U2[0], U1[1] + U2[1]))
    part_A_output = sum_count_rdd.map(lambda element: (element[0], element[1][0] / (element[1][1]))).sortByKey().collect()
    print(part_A_output)
    # list_train = train.collect()

    # train = trainRDD.map(business).groupByKey()
    # list_train_business = train.collect()
    #
    # print(list_train[0])

    # for each in list_train:
    #     print(each)
    #     break

    # testRDD = sc.textFile(test_file_path)
    # list_test = testRDD.map(f).groupByKey().map(lambda element: {element[0]: list(element[1])})
    # list_test = list_test.collect()
    # print(list_test[0])
