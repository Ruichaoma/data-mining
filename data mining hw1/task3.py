import json
import time
from pyspark import SparkConf, SparkContext
import sys


conf = SparkConf().setAppName("553hw1").setMaster('local[*]') 
sc = SparkContext(conf=conf)

review_filepath = sys.argv[1]
business_filepath = sys.argv[2]
output_filepath_question_a = sys.argv[3]
output_filepath_question_b = sys.argv[4]


Data3_review = sc.textFile(review_filepath)
Data3_business = sc.textFile(business_filepath)

data3_review = Data3_review.map(lambda i:json.loads(i)).map(lambda x:(x['business_id'],x['stars'])).persist()
data3_business = Data3_business.map(lambda i:json.loads(i)).map(lambda x:(x['business_id'],x['city'])).persist()


combined_data = data3_business.join(data3_review).map(lambda i:i[1])

City_avg_stars =data3_business.join(data3_review).map(lambda i:(i[1][0],i[1][1])).groupByKey().map(lambda i: (str(i[0]),list(i[1]))).map(lambda i:[i[0],sum(i[1])/len(i[1])]).sortBy(lambda i:(-i[1],i[0]))

        
                 
start = time.time()
city_avg_stars_data = City_avg_stars.collect()

num = 1
for i in city_avg_stars_data:
    print(str(i))
    if num == 10:
        break
    num+=1

time_m1 = time.time()-start

start_m2 = time.time()
city_avg_m2 = City_avg_stars.take(int(10))
print(city_avg_m2)
time_m2 = time.time()-start_m2

def dataset_a(path,dataset):
    with open(path,'w') as a:
        a.write("city,stars\n")
        for data in dataset:
            a.write(data[0]+','+str(data[1])+'\n')

def dataset_b(path,dataset):
    with open(path,'w') as b:
        json.dump(dataset,b)

structure_b = {'m1':time_m1,'m2':time_m2}



dataset_a(output_filepath_question_a,city_avg_stars_data)
dataset_b(output_filepath_question_b,structure_b)


