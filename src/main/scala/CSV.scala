import org.apache.spark.{SparkConf,SparkContext}

object CSV {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("CSV").setMaster("local")
        val sc = new SparkContext(conf)
        // 从HDFS中读取数据
        val rdd = sc.textFile("/user/hadoop/data/input/5.csv")
        val rdd1 = rdd.map(x => {
              val arr = x.split(",")
              (arr(0), arr(1))
        }) 
        //将结果保存到本地文件系统中
        rdd1.saveAsTextFile("file:///home/hadoop/spark/src/main/resources/data/out/vsv")
    }
}