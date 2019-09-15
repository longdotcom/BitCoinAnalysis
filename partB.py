import pyspark
from pyspark import SparkContext
SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')

def cleanVIN(line):
    try:
        fields = line.split(',')
        if len(fields)!=3:
            return False

        return True
    except:
        return False

def cleanVOUT(line):
    try:
        fields = line.split(',')
        if len(fields)!=4:
            return False

        return True
    except:
        return False

def filterVOUT(line):
        fields = line.split(',')
        if fields[3] == "{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}":
            return True
        return False

sc = pyspark.SparkContext()

vout = sc.textFile("/data/bitcoin/vout.csv")
voutFiltered = vout.filter(cleanVOUT).filter(filterVOUT).map(lambda x: x.split(","))
voutJoined = voutFiltered.map(lambda fields: (fields[0],(fields[1], fields[2], fields[3])))

vin = sc.textFile("/data/bitcoin/vin.csv")
vinFiltered = vin.filter(cleanVIN).map(lambda y: y.split(","))
vinJoined = vinFiltered.map(lambda fields: (fields[0],(fields[1], fields[2])))

firstJoin = voutJoined.join(vinJoined)


voutFiltered1 = vout.filter(cleanVOUT).map(lambda x: x.split(","))
voutJoined1 = voutFiltered1.map(lambda fields: ((fields[0], fields[2]), (fields[1], fields[3])))

secondJoin = firstJoin.map(lambda secondJoin: ((secondJoin[1][1][0], secondJoin[1][1][1]), (secondJoin[0], secondJoin[1][0][0], secondJoin[1][0][1], secondJoin[1][0][2])))

finalJoin = secondJoin.join(voutJoined1)

data = finalJoin.map(lambda sss: (sss[1][1][1],float(sss[1][1][0])))

finalData= data.reduceByKey(lambda a,b: a+b)

top10=finalData.takeOrdered(10, key= lambda c: -c[1])

for w in top10:
    print(w)
