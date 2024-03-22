from pyspark import SparkContext
import sys

def main():
	sc=SparkContext(appName='avgdistant')
	
	inputad=sc.textFile('/user/hadoop/input1/ProjectData/*.gz')

	filteredad=inputad.filter(lambda line1: line1 [78:84] != '999999' and int(line1[84:85]) in [0,1,4,5,9])
	
	id_distad= filteredad.map(lambda line1 : (line1 [4:10] , int(line1[78:84])))

	avgvisdist_rdd=id_distad.groupByKey().map(lambda y : (y[0], sum(y[1])/len(y[1])))

	avgvisdist_rdd.saveAsTextFile('/user/hadoop/projectoutput')

if __name__ == '__main__':
	main()

