# import pydevd_pycharm
# pydevd_pycharm.settrace('localhost', port=9999, stdoutToServer=True, stderrToServer=True)
from sys import platform


# from pyspark import SparkConf, SparkContext
#
# conf = SparkConf().setMaster("yarn-client").setAppName("WordCount")
# sc = SparkContext(conf=conf)
#
# inputData = sc.textFile("file:///apps/pysparkExercises/data/book.txt")
# words = inputData.flatMap(lambda x: x.split())
# wordCounts = words.countByValue()
#
# for word, count in wordCounts.items():
#     cleanWord = word.encode('ascii', 'ignore')
#     if cleanWord:
#         print(cleanWord.decode() + " " + str(count))


from sparkClassesMethods import sparkClassesMethods as scm
sparkSessionObject = scm.sparkClass(master='local', appName='WordCountExercise')
sparkSession = sparkSessionObject.sparkSession()
sc = sparkSession.sparkContext

if platform == "linux" or platform == "linux2":
    inputData = sc.textFile('file:///apps/pysparkExercises/data/book.txt')
elif platform == "darwin":
    inputData = sc.textFile('file:///apps/pysparkExercises/data/book.txt')
elif platform == "win32":
    inputData = sc.textFile(r'E:\MyLearning\DataScience\GitHub\PySparkExercises\data\book.txt')

words = inputData.flatMap(lambda x: x.split())
wordCounts = words.countByValue()


for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(cleanWord.decode() + " " + str(count))






