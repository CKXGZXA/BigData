import org.apache.spark.{SparkConf,SparkContext}

object CSV {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("CSV").setMaster("local")
        val sc = new SparkContext(conf)
        val rdd = sc.textFile("file:///home/hadoop/spark/src/main/resources/data/in/1.csv")
        val rdd1 = rdd.map(line => {
            val arr = line.split(",")
            (arr(0).toInt+arr(1).toInt+arr(2).toInt)
        })
        //将结果保存到文件中
        rdd1.saveAsTextFile("file:///home/hadoop/spark/src/main/resources/data/out/")
    }
}
