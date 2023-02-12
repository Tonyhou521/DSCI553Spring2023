from pyspark import SparkContext
import pyspark
import sys
import time
import json
from operator import add

## You will work on test_review.json, which contains the review information from users, and write a program to automatically answer the following questions:


##A. The total number of reviews
def TotalReview(RDD):
    return RDD.count()

##B. The number of reviews in 2018
def TotalReview2018(RDD):
    return RDD.filter(lambda x:'2018' in x['date']).count()

##C. The number of distinct users who wrote the reviews
def DistinctUsers(RDD):
    return RDD.map(lambda x: x['user_id']).distinct().count()

#D. The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote.
def Top10Users(RDD):
    return RDD.map(lambda x: [x['user_id'],1]).reduceByKey(add).takeOrdered(10,key = lambda x: [-x[1],x[0]])

#E. The number of distinct businesses that have been reviewed
def DistinctBus(RDD):
    return RDD.map(lambda x:x['business_id']).distinct().count()

#F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
def Top10Bus(RDD):
    return RDD.map(lambda x: [x['business_id'],1]).reduceByKey(add).takeOrdered(10,key = lambda x: [-x[1],x[0]])


def main():
    ## Set up a timer variable to track the efficiency of the program
    start_time = time.time()
    review_filepath = sys.argv[1]
    output_filepath = sys.argv[2]

    sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")
    task1_RDD = sc.textFile(review_filepath).map(lambda row: json.loads(row)).cache()

    output = {}
    output['n_review'] = TotalReview(task1_RDD)
    output['n_review_2018'] = TotalReview2018(task1_RDD)
    output['n_user'] = DistinctUsers(task1_RDD)
    output['top10_user'] = Top10Users(task1_RDD)
    output['n_business'] = DistinctBus(task1_RDD)
    output['top10_business'] = Top10Bus(task1_RDD)
    
    with open(output_filepath, 'w') as f:
        json.dump(output, f,sort_keys = True, indent = 4,ensure_ascii = False)


    end_time = time.time()
    print("Time taken: ", end_time - start_time)

if __name__ == "__main__":
    main()
