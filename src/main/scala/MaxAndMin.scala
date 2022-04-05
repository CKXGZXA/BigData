import org.apache.spark.{SparkConf, SparkContext}

object MaxAndMin {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MaxAndMin").setMaster("local")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val lines = sc.textFile("file:///home/hadoop/spark/src/main/resources/data/3.txt")
        val result = lines.filter(_.trim().length>0).map(line => 
            ("key",line.trim.toInt)).groupByKey().map(x => {
                var min = Integer.MAX_VALUE
                var max = Integer.MIN_VALUE
                for(num <- x._2) {
                    if(num < min) {
                        min = num
                    }
                    if(num > max) {
                        max = num
                    }
                }
                (max,min)
            }).collect().foreach(x => {
                println("max\t"+x._1)
                println("min\t"+x._2)
            })
    }
}
