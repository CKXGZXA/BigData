import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
   def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("WordCount").setMaster("local")
        val sc = new SparkContext(conf)
        val lines = sc.textFile("/user/hadoop/data/input/6.txt")
        val words = lines.flatMap(line => line.split(" "))
        val pairs = words.map(word => (word, 1))
        val wordCounts = pairs.reduceByKey(_ + _)
        wordCounts.saveAsTextFile("file:///home/hadoop/spark/src/main/resources/data/out/wordcount")
   }
}
