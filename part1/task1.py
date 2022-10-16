from copy import deepcopy
from pyspark import SparkContext
import time
import collections
import math
from itertools import combinations
import sys
from hash_family import UniversalStringHashFamily
sys.stdout = open("log.txt", "w")

NUM_HASH_FUNCTIONS = 50
BUCKETS = 100
GLOBAL = ""
if __name__ == "__main__":
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]
    SparkContext.setSystemProperty('spark.executor.memory', '4g')
    # SparkContext.setSystemProperty('spark.executor.cores', '8')
    SparkContext.setSystemProperty("spark.driver.memory", "4g")
    # SparkContext.setSystemProperty('spark.executor.instances', '20')
    start = time.time()
    sc = SparkContext('local[*]', 'task1')
    textRDD = sc.textFile(input_file_path)

    RDD = textRDD
    unique_user_ids = RDD.map(lambda element: element.split(',')[0]).distinct().collect()
    unique_user_ids = sorted(unique_user_ids[1:])
    print("name: ",unique_user_ids[0])

    def f(element):
        temp = element.split(',')
        return (temp[1], temp[0])
        # return (temp[1], (temp[0],temp[2])) # Dont need the stars

    ushf = UniversalStringHashFamily(NUM_HASH_FUNCTIONS, BUCKETS)
    print(ushf)
    for i in range(NUM_HASH_FUNCTIONS):
        ushf(i, baskets[0])
    header = textRDD.first()

    signature_matrix = RDD.filter(lambda row: row != header).map(f).groupByKey().map(lambda element: {element[0]: list(element[1])})
        # .map(lambda element: min_hash(element, ushf, unique_user_ids)).persist()
    signature_matrix = signature_matrix.collect()
    # signature_matrix = RDD.map(f).groupByKey().map(lambda element: list(element)).collect()
    print(signature_matrix[0])

    M_ic_col = [[math.inf for i in range(len(unique_user_ids))] for j in range(NUM_HASH_FUNCTIONS)]





    # rdd_cleaned_data = rdd_cleaned_data[1:]
    # print(rdd_cleaned_data)

    end = time.time()
    print("Duraiton: ", end-start)

