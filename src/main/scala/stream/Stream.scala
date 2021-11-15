package stream
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Stream extends Serializable {
//  @transient
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[ String ]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var spark = SparkSession.builder()
      .master("local[3]")
      .appName("Streaming")
      .config("spark.streaming.stopGracefullyShutdown", "true")
      .config("spark.sql.shuffle.partitions",3)
      .getOrCreate()
    var linesDF = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","9999")
      .load()

//    linesDF.printSchema()
    val wordsDF = linesDF.selectExpr("explode(split(value,'')) as word")
    val countsDF = wordsDF.groupBy("word").count()

    val wordCountQuery = countsDF.writeStream.format("console").option("checkpointLocation","chk-point-dir")
      .outputMode("complete")
      .start()
    wordCountQuery.awaitTermination()


  }

}
