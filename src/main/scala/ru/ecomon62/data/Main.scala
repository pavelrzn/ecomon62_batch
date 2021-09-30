package ru.ecomon62.data

import com.sun.tools.javac.util.Log
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import ru.ecomon62.data.Constants.areas_table



object Main extends App {

  val spark = SparkSession.builder()
    .appName("air")
    .config("spark.master", "local")
    .getOrCreate()

//  spark.sparkContext.hadoopConfiguration.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
  spark.sparkContext.hadoopConfiguration.set("hive.metastore.uris", "thrift://84.201.148.203:9083")

  spark.sparkContext.setLogLevel("WARN")

  import java.util.Properties
  implicit val jdbcProps = new Properties()
  val jdbcUser = args(0)
  val jdbcPass = args(1)
  val schema = args(2)
  val srcTable = args(3)
  val lastLoadedId = args(4)
  jdbcProps.put("user", jdbcUser)
  jdbcProps.put("password", jdbcPass)
  jdbcProps.put("Driver", "org.postgresql.Driver")
  implicit val jdbcUrl = "jdbc:postgresql://80.78.247.249:5432/ecomonitor"

  println("log: starting... Last loaded id: " + lastLoadedId)

  // датафрейм сырых данных
  val srcDf = spark.read
    .jdbc(jdbcUrl, s"$schema.$srcTable", jdbcProps)
    .where(col("comment_id") > lastLoadedId)

  val stores = new Stores()

  println("log: defining areas")
  // полынй набор данных, + со столбцом указывающим район города, без личных данных (ФИО)
  val dfWithAreas = stores.defineAreas(srcDf)
    .cache()

  println(s"log: writing $areas_table table")
  // запись в витрину
  dfWithAreas
    .write
    .mode(SaveMode.Append)
    .jdbc(jdbcUrl, s"$schema.$areas_table", jdbcProps)


  stores.processFenol(dfWithAreas)
  stores.processUsersSensitivity(dfWithAreas)

  println("log: The end !")


}
