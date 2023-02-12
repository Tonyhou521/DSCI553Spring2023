from pyspark import SparkContext
import time
import sys
import json


def ItemCountPartition(iterator):
  yield sum(1 for item in iterator)

def Default(RDD):
    d_start_time = time.time()
    NumPar = RDD.getNumPartitions()
    ItemPar = RDD.mapPartitions(ItemCountPartition).collect()
    d_end_time = time.time()
    E_time = d_end_time - d_start_time
    return(NumPar,ItemPar,E_time)


def Customized(RDD,Partition):
    C_start_time = time.time()
    RDD = RDD.partitionBy(Partition,lambda keyv: hash(keyv[0]))
    NumPar = RDD.getNumPartitions()
    ItemPar = RDD.mapPartitions(ItemCountPartition).collect()
    C_end_time = time.time()
    E_C_time = C_end_time - C_start_time
    return(NumPar,ItemPar,E_C_time)


def main():
    ##you will show the number of partitions for the RDD used for Task 1 Question F and the number of items per partition.
    start_time = time.time()
    review_filepath = sys.argv[1]
    output_filepath = sys.argv[2]
    n_partition = sys.argv[3]

    sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")
    
    Default_RDD = sc.textFile(review_filepath).map(lambda row: json.loads(row))
    Task1F_RDD = Default_RDD.map(lambda x: [x['business_id'],1]).cache()
    
    
    output = {}
    default = {}
    customized = {}
    
    d = Default(Task1F_RDD)
    c = Customized(Task1F_RDD,int(n_partition))
    
    default['n_partition'],default['n_items'],default['exe_time'] = d[0],d[1],d[2]
    customized['n_partition'],customized['n_items'],customized['exe_time'] = c[0],c[1],c[2]
    
    output['default'],output['customized'] = default,customized
    
    with open(output_filepath, 'w') as f:
        json.dump(output, f,sort_keys = False, indent = 4,ensure_ascii = False)
    
    
    end_time = time.time()
    print("Time taken for the whole file to run: ", end_time - start_time)
   

if __name__ == "__main__":
    main()