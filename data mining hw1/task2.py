import json
import time
from pyspark import SparkConf, SparkContext
import sys

conf = SparkConf().setAppName("553hw1").setMaster('local[*]') 
sc = SparkContext(conf=conf)

review_filepath = sys.argv[1]
output_filepath = sys.argv[2]
n_partition = int(sys.argv[3])
Data2 = sc.textFile(review_filepath)
data2 = Data2.map(lambda i:json.loads(i)).map(lambda x:(str(x['business_id']),1))

##Default
def num_partition(data2):
    return data2.getNumPartitions()

def items_count(data2):
    return data2.glom().map(len).collect()

def top10_business(data2):
    count_business = data2.reduceByKey(lambda i,j:i+j)
    return count_business.takeOrdered(10,key=lambda i:(-i[1],i[0]))

def default_time(data2):
    start = time.time()
    process_1f = top10_business(data2)
    end = time.time()-start
    return end

##Custom
customized_data = data2.partitionBy(n_partition,lambda i:ord(i[0])-ord(i[-1]))

def num_partition_customized(customized_data):
    return customized_data.getNumPartitions()

def items_count_customized(customized_data):
    return customized_data.glom().map(len).collect()

def top10_business_customized(customized_data):
    count_business = customized_data.reduceByKey(lambda i,j:i+j)
    return count_business.takeOrdered(10,key=lambda i:(-i[1],i[0]))

def customized_time(customized_data):
    start = time.time()
    process_1f = top10_business(customized_data)
    end = time.time()-start
    return end

output_structure = {'default':{'n_partition':num_partition(data2),
                     'n_items': items_count(data2),
                     'exe_time': default_time(data2)
                     },
          'customized':{'n_partition':num_partition_customized(customized_data),
                     'n_items': items_count_customized(customized_data),
                     'exe_time': customized_time(customized_data)
                     }
          }

def output_datastructure(output_path,data_output):
    with open(output_path, "w") as outputtwo:
        json.dump(data_output,outputtwo)

   
    
output_datastructure(output_filepath,output_structure)               

    
