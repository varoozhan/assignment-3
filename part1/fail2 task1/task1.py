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
    # header = textRDD.first()
    # def f(element):
    #     temp = element.split(',')
    #     return (temp[1], temp[0])
    #
    # def f1(element):
    #     list(element[1])
    #     return (element[0],)
    #
    # signature_matrix = RDD.filter(lambda row: row != header).map(f).groupByKey().map(f1).take(10)
    # print(signature_matrix)

    # .map(f).groupByKey()
        # .map(lambda element: min_hash(element, ushf, unique_user_ids)).persist()




    def f(element):
        temp = element.split(',')
        return (temp[1], temp[0])
        # return (temp[1], (temp[0],temp[2])) # Dont need the stars

    ushf = UniversalStringHashFamily(NUM_HASH_FUNCTIONS, BUCKETS)

    def min_hash(baskets, ushf, uuid):
        sig_m = []
        #                     BUSINESS_ID                   ROW
    # each basket is: ['3MntE_HWbNNoyiLGxywjYA', <pyspark.resultiterable>]
    # each row is USER_IDs: ['T13IBpJITI32a1k41rc-tg', 'xhlcoVm3FOKcxZ0phkdO6Q', '4o0KkpAkyO6r0NHXmobTeQ', 'TQXtrSpsUyvHMriX8hvNWQ', 'kjaUSiRWhR9bF9KxOMbVvg']
        for each_value in list(baskets[1]):
            M_ic_col = [math.inf for j in range(len(uuid))]
            idx = uuid.index(each_value)
            hash_table = collections.defaultdict(int)
            for i in range(NUM_HASH_FUNCTIONS):
                hash_table[ushf(i, baskets[0])] = each_value
            # sig_m = []
            hash_table = dict(sorted(hash_table.items()))
            for key, hash_value in hash_table.items():
                # sig_m.append([k for k, v in uuid.items() if v == hash_value])
                # sig_m.append(uuid.index(hash_value))
                if key < M_ic_col[idx]:
                    M_ic_col[idx] = key
            sig_m.append(M_ic_col)
        return sig_m

    header = textRDD.first()

    signature_matrix = RDD.filter(lambda row: row != header).map(f).groupByKey().map(lambda element: {element[0]: list(element[1])})
        # .map(lambda element: min_hash(element, ushf, unique_user_ids)).persist()
    signature_matrix = signature_matrix.collect()
    # signature_matrix = RDD.map(f).groupByKey().map(lambda element: list(element)).collect()
    print(signature_matrix[0])

    M_ic_col = [[math.inf for i in range(len(unique_user_ids))] for j in range(NUM_HASH_FUNCTIONS)]

    sig_m = []
    #                     BUSINESS_ID                   ROW
    # each basket is: ['3MntE_HWbNNoyiLGxywjYA', <pyspark.resultiterable>]
    # each row is USER_IDs: ['T13IBpJITI32a1k41rc-tg', 'xhlcoVm3FOKcxZ0phkdO6Q', '4o0KkpAkyO6r0NHXmobTeQ', 'TQXtrSpsUyvHMriX8hvNWQ', 'kjaUSiRWhR9bF9KxOMbVvg']
    x=0
    for each_user in signature_matrix:
        for each_user_id, each_list in each_user.items():
            print("row: ",each_user_id)
            print("col: ",each_list)
        if x==3:
            break
        x+=1
            # hash_values = []
            # for i in range(NUM_HASH_FUNCTIONS):
            #     hash_values.append(ushf(i, each_user_id[0]))
            # for each_business_id in each_list:
            #     # print(each_business_id)
            #
            #     idx = unique_user_ids.index(each_business_id)
            #     # print("idx: ", idx)
            #     for hash_value in hash_values:
            #         # print("hash_value: ",hash_value)
            #         if (hash_value < M_ic_col[idx]):
            #             # print("M_ic_col[idx]: ", M_ic_col[idx])
            #             M_ic_col[hash_value][idx] = hash_value
            #             # print("M_ic_col[idx]: ", M_ic_col[idx])
            #             # print("M_ic_col: ", M_ic_col)

        sig_m.append(M_ic_col)


    # print(sig_m)
                # print(idx)
                # print(M_ic_col[idx])


        # hash_table = collections.defaultdict(int)
        # for i in range(NUM_HASH_FUNCTIONS):
        #     hash_table[ushf(i, baskets[0])] = each_value
        # # sig_m = []
        # hash_table = dict(sorted(hash_table.items()))
        # for key, hash_value in hash_table.items():
        #     # sig_m.append([k for k, v in uuid.items() if v == hash_value])
        #     # sig_m.append(uuid.index(hash_value))
        #     if key < M_ic_col[idx]:
        #         M_ic_col[idx] = key
        # sig_m.append(M_ic_col)




    # rdd_cleaned_data = rdd_cleaned_data[1:]
    # print(rdd_cleaned_data)

    end = time.time()
    print("Duraiton: ", end-start)

