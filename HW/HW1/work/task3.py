from pyspark import SparkContext
import time
import sys
import json
from operator import add




def SparkSort(RDD):
    return_RDD = RDD.sortBy(lambda x: [-x[1],x[0]]).take(10)
    Spark_endTime = time.time()
    return return_RDD, Spark_endTime
    
        
    
def NoSparkSort(RDD):
    result = RDD.collect()
    result_sorted = sorted(result,key=lambda element: (-element[1],element[0]))
    result_sorted = result_sorted[:10]
    NoSpark_endTime = time.time()
    return result, NoSpark_endTime


def main():
    review_filepath = sys.argv[1]
    business_filepath = sys.argv[2]
    output_filepath_qa = sys.argv[3]
    output_filepath_qb = sys.argv[4]
    
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")
    
    ## Start_timestamp
    Start_Time = time.time()
    ## What are the average stars for each city
    review_RDD = sc.textFile(review_filepath).map(lambda rows: json.loads(rows)).map(lambda x: (x['business_id'],x['stars']))
    business_RDD = sc.textFile(business_filepath).map(lambda rows: json.loads(rows)).map(lambda x: (x['business_id'],x['city']))
    
    join_RDD = review_RDD.join(business_RDD.map(lambda x: (x[0],x[1]))).map(lambda x: (x[0],)+ (x[1]))
    score_RDD = join_RDD.map(lambda x:(x[2],x[1])).reduceByKey(add)
    count_RDD = join_RDD.map(lambda x:(x[2],1)).reduceByKey(add)
    Avg_RDD = score_RDD.join(count_RDD).map(lambda x: (x[0],(x[1][0]/x[1][1])))
    
    PrepTime = time.time()
    PrepDuration = PrepTime - Start_Time
    
    
    Spark_TimeStamp = SparkSort(Avg_RDD)[1]
    Time_durationSpark = Spark_TimeStamp - Start_Time
    
    NoSpark_TimeStamp = NoSparkSort(Avg_RDD)[1]
    Time_durationNoSpark = (NoSpark_TimeStamp - Spark_TimeStamp+ PrepDuration)
    
    PB_out = {
        "m1": Time_durationNoSpark,
    "m2": Time_durationSpark,
        "reason": "Even though here it took longer for the buildin sort for Spark RDD to finish its operation when comparing to just using the build in sort function in Python, When file size increases, eventually RDD sort will become the superior solution.",
        }

    
    PartA_RDD = Avg_RDD.sortBy(lambda x: [-x[1],x[0]]).collect()
    
    header = "city,stars"

    with open(output_filepath_qa, 'w') as f:
        f.write(header + "\n")
        f.close
        
    with open(output_filepath_qa, 'a') as f:
        for x in PartA_RDD:
                f.write(x[0]+ ","+ str(x[1])+"\n")#the line I needed to have printed via a loop
        f.close
    
    with open(output_filepath_qb, 'w') as f:
        json.dump(PB_out,f,indent = 4,ensure_ascii = False)
        
        
            
    
if __name__ == "__main__":
    main()
