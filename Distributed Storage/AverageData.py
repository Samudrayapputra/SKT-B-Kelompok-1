################################
# Program Perhitungan Rerata
# Suhu dan Hydrogen
################################

from pyspark import SparkContext
import json

sc = SparkContext.getOrCreate()

rdd = sc.textFile("hdfs://192.168.56.30:9000/sensor_dataset.json")

def parse_temp_json(line):
    data = json.loads(line)
    return data["temp"]

def parse_hydrogen_json(line):
    data = json.loads(line)
    return data["hydrogen"]

rddA = rdd.map(parse_temp_json)
rddB = rdd.map(parse_hydrogen_json)

hasil_temp = rddA.mean()
hasil_hydrogen = rddB.mean()

rddHasilTemp = sc.parallelize([hasil_temp])
rddHasilHydrogen = sc.parallelize([hasil_hydrogen])
rddHasilTemp.saveAsTextFile("hdfs://192.168.56.30:9000/hasil_rerata_suhu")
rddHasilHydrogen.saveAsTextFile("hdfs://192.168.56.30:9000/hasil_rerata_hydrogen")