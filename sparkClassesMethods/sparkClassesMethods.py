from pyspark.sql import SparkSession

class sparkClass:
    def __init__(self, master, appName, loginfo=False):
        self.master = master
        self.appName = appName

    # @staticmethod
    def sparkSession(self):
        spark = SparkSession \
            .builder \
            .appName(self.appName) \
            .master(self.master) \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.shuffle.service.enabled", "true") \
            .config("hive.exec.dynamic.partition", "true") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict") \
            .config("spark.sql.shuffle.partitions", "50") \
            .enableHiveSupport() \
            .getOrCreate()

        return spark

