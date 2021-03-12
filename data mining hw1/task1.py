import sys
import json
from pyspark import SparkContext, SparkConf
import time

conf = SparkConf().setAppName("553hw1").setMaster('local[*]') 
sc = SparkContext(conf=conf)
review_filepath = sys.argv[1]

Data1 = sc.textFile(review_filepath)
data1 = Data1.map(json.loads).map(lambda i:(i['user_id'],i['business_id'],i['date'])).persist()

##A. The total number of reviews
def total_reviews(data1):
    return data1.count()

##B. The number of reviews in 2018
def review_2018(data1):
    return data1.filter(lambda i:(i[2][0:4]=='2018')).count()


##C. The number of distinct users who wrote reviews
def distinct_user(data1):
    return data1.map(lambda i:(i[0])).distinct().count()


##D. The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
def top_10_user(data1):
    count_user = data1.map(lambda i:(i[0],1)).reduceByKey(lambda i,j:i+j)
    return count_user.takeOrdered(10,key=lambda i:(-i[1],i[0]))

##E. The number of distinct businesses that have been reviewed
def business_distinct_id(data1):
    return data1.map(lambda i:(i[1])).distinct().count()

##F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
def top10_business(data1):
    count_business = data1.map(lambda i:(i[1],1)).reduceByKey(lambda i,j:i+j)
    return count_business.takeOrdered(10,key=lambda i:(-i[1],i[0]))


output_structure = {'n_review': total_reviews(data1),
                    'n_review_2018': review_2018(data1),
                    'n_user': distinct_user(data1),
                    'top10_user': top_10_user(data1),
                    'n_business': business_distinct_id(data1),
                    'top10_business': top10_business(data1)
                    }
output_filepath = sys.argv[2]



def output_datastructure(output_path,data_output):
    with open(output_path, "w") as outputt:
        json.dump(data_output,outputt)

   
    
output_datastructure(output_filepath,output_structure)




