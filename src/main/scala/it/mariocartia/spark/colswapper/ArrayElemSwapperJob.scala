package it.mariocartia.spark.colswapper

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, udf}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success, Try}

object ArrayElemSwapperJob {

  val logger = LoggerFactory.getLogger(ArrayElemSwapperJob.getClass)

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      logger.error("Usage "+ArrayElemSwapperJob.getClass.getName+" configFile inPath outPath [partitionColumn] [format] [compression]...")
      System.exit(0);
    }
    val cfgFile = Source.fromFile(args(0)).getLines.filter(f => !f.trim.isEmpty)
    val colMappings = cfgFile.map(x => (x.split("->")(0), x.split("->")(1))).toMap[String,String]

    val inPath = args(1)
    logger.info("Input path: "+inPath)
    val outPath = args(2)
    logger.info("Output path: "+outPath)

    val partitionColumn = Try(args(3)).getOrElse("")
    logger.info("Partition column: "+partitionColumn)

    val fileFormat = Try(args(4)).getOrElse("parquet")
    logger.info("File format: "+fileFormat)

    val compression = Try(args(5)).getOrElse("snappy")
    logger.info("Compression: "+compression)

    val spark = SparkSession.builder.appName("ColumnSwapper").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val swapArrayElemUDF = udf( (colName: String, arr: mutable.WrappedArray[AnyVal]) => swapArrayElem(colName,arr,colMappings) )

    val srcDF = spark.read.format(fileFormat).option("header", "true").load(inPath).cache
    logger.info("Original columns sample:")
    srcDF.select(colMappings.keys.toList.map(col):_*).limit(5).show

    var outDF = srcDF
    colMappings.map(x=>x._1).foreach(x => {
      outDF = outDF
        .withColumn( x + "_tmp", swapArrayElemUDF(lit(x),col(x)))
        .withColumn(x, col(x + "_tmp"))
        .drop(x + "_tmp")
    })

    var outDFWriter = outDF.write.format(fileFormat).option("compression", compression)

    if (partitionColumn.length>0) {
      logger.info("Partitioning by: "+partitionColumn)
      outDFWriter = outDFWriter.partitionBy(partitionColumn)
    }

    Try(outDFWriter.save(outPath)) match {
      case Success(i) => {
        logger.info("Final columns sample:")
        outDF.select(colMappings.keys.toList.map(col):_*).limit(5).show
      }
      case Failure(s) => logger.error(s"Failed: ${s.getMessage}")

    }
  }

  def swapArrayElem(colName: String, arr: mutable.WrappedArray[AnyVal], cfg: Map[String,String]): mutable.WrappedArray[AnyVal] = {
    //get configuration for column to swap
    val columnConf = cfg.get(colName).get
    //convert from String (csv) to Array[Int]
    val swappedOrder = columnConf.split(",").map(_.toInt).toArray
    //check if original array size matches with swapped array size from configuration (else return original array)
    if (arr.length == swappedOrder.size) {
      var outArr = Array[AnyVal]()
      //create new array with ordering read from configuration
      (0 to arr.length-1).foreach( x => {
        outArr = outArr :+ arr(swappedOrder(x))
      })
      return outArr
    } else {
      return arr
    }
  }
}
