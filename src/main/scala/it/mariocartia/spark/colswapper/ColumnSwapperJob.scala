package it.mariocartia.spark.colswapper

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.{Failure, Success, Try}

object ColumnSwapperJob {

  val logger = LoggerFactory.getLogger(ColumnSwapperJob.getClass)

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      logger.error("Usage "+ColumnSwapperJob.getClass.getName+" configFile inPath outPath [format] [compression]...")
      System.exit(0);
    }
    val cfgFile = Source.fromFile(args(0)).getLines.filter(f => !f.trim.isEmpty)
    val colMappings = cfgFile.map(x => (x.split("->")(0), x.split("->")(1))).toMap

    val inPath = args(1)
    logger.info("Input path: "+inPath)
    val outPath = args(2)
    logger.info("Output path: "+outPath)

    val fileFormat = Try(args(3)).getOrElse("parquet")
    logger.info("File format: "+fileFormat)
    val compression = Try(args(4)).getOrElse("snappy")
    logger.info("Compression: "+compression)

    val spark = SparkSession.builder.appName("ColumnSwapper").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val srcDF = spark.read.format(fileFormat).option("header","true").load(inPath).cache
    logger.info("Original columns sample:")
    srcDF.select((colMappings.keys.toList:::colMappings.values.toList).map(col):_*).limit(5).collect().foreach(row=>logger.info(rowAsStr(row)))

    val outDF = swapColumn(srcDF, colMappings).cache
    Try(outDF.write.format(fileFormat).option("compression", compression).save(outPath)) match {
      case Success(i) => {
        logger.info("Final columns sample:")
        outDF.select((colMappings.keys.toList:::colMappings.values.toList).map(col):_*).limit(5).collect().foreach(row=>logger.info(rowAsStr(row)))
      }
      case Failure(s) => logger.error(s"Failed: ${s.getMessage}")
    }
  }

  def swapColumn(df: DataFrame, colMap: Map[String,String]): DataFrame = {
    var outDF = df

    colMap.foreach( x => {
      outDF = outDF.withColumn(x._1+"_tmp",col(x._2))
      .withColumn(x._2, col(x._1))
      .withColumn(x._1,col(x._1+"_tmp"))
      .drop(x._1+"_tmp")
    })

    return outDF
  }

  def rowAsStr(row: Row): String = {
    val colNames = row.schema.fields.map(_.name)
    var rowStr = ""
    colNames.zip(row.mkString(",").split(",")).foreach(x=> { rowStr = rowStr+x._1+": "+x._2+" " })
    return rowStr
  }
}