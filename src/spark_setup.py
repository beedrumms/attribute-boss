#%% IMPORT PACKAGES
import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf

# global variable to hold the SparkSession object
spark = None
### 

#%% CENTRAL SPARKSESSION
def MySparkSession(master="local", executor_memory='2g', executor_cores="1", executor_instances="1", shuffle_partitions="1"):
    """
    Create a SparkSession for PySpark usage.

    ARGS
    master (str) Specifies the URL of the cluster manager to connect to. It determines the cluster mode in which your Spark application runs, such as "local" for local mode or the URL of a cluster manager like "spark://host:port" for cluster mode. (default is "local")
    executor_memory (str) Determines the amount of memory allocated to each executor in your Spark application. It specifies the maximum amount of memory that can be used for caching and storing intermediate data during the execution. The value should be specified with a unit of memory (default is "1g") (e.g., "1g" for 1 gigabyte).
    executor_cores (str) Specifies the number of CPU cores to allocate for each executor. It determines the parallelism and concurrency of task execution within each executor. The total number of CPU cores across all executors should be carefully planned based on the available resources and the workload characteristics. (default is "1g")
    executor_instances (str) Defines the number of executor instances to launch in your Spark application. Each executor runs in a separate JVM and can have multiple cores. The number of instances impacts the overall parallelism and resource utilization of your application. It depends on the available resources, the size of the dataset, and the desired performance. (default is "1g")
    shuffle_partitions (str) Determines the number of partitions used during shuffling of data in Spark. It affects the parallelism of data shuffling and the number of tasks during the execution. A higher number of shuffle partitions can increase parallelism but also incurs additional memory overhead. It is recommended to set this parameter based on the size of your data and the available resources. (default is "1g")

    Returns:
        spark (pyspark.sql.session.SparkSession): A SparkSession object.

    NOTES
    default config args are set for local development and should be changed according to your use case

    """
    global spark

    conf = SparkConf().setAppName("AttributeBoss").setMaster(master).set("spark.executor.memory", executor_memory).set("spark.executor.cores", executor_cores).set("spark.executor.instances", executor_instances).set("spark.sql.shuffle.partitions", shuffle_partitions)

    try:
 
        if spark is None:

            # Create a SparkSession object
            spark = SparkSession.builder.config(conf=conf).getOrCreate()

            print("New SparkSession has been created --> ", conf.get("spark.app.name"), "\n", "Configs:")
            print("spark.master = ", conf.get("spark.master"))
            print("spark.executor.memory = ", conf.get("spark.executor.memory"))

            return spark

        else:

            print("SparkSession --> ", conf.get("spark.app.name"), "\n", "Configs:")
            print("   spark.master = ", conf.get("spark.master"))
            print("   spark.executor.memory = ", conf.get("spark.executor.memory"))

            return spark
                        
    except Exception as e:

        print("SparkSession failed to initialize: --> ", str(e))
        print("Review doc strings for config set up", str(MySparkSession.__doc__))
        raise

    finally:

        spark.stop()
###
