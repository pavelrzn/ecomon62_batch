package ru.ecomon62.data

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_unixtime}
import ru.ecomon62.data.util.UdfLib.defineArea

import java.util.Properties

class Stores(implicit jdbcUrl: String, jdbcProps: Properties) {

  /**
   * возращает полный набор данных, за исключением ФИО, + определяет район города по координатам
   */
  def defineAreas(df: DataFrame): DataFrame = {
    val ecomon62AreasDf = df
      .drop("user_fio")
      .withColumnRenamed("comment_time", "comment_unix_time")
      .withColumn("comment_date", from_unixtime(col("comment_unix_time"), "yyyyMMdd"))
      .withColumn("comment_time", from_unixtime(col("comment_unix_time"), "HHmmss"))
      .withColumn("area", defineArea(col("latitude"), col("longitude")))
      .na.fill("undefined", Seq("area"))

    ecomon62AreasDf
  }


  /**
   * заполняет витрину рейтингов запахов - какие запахи ощущает чаще всего конкретный пользователь
   * */
  def processUsersSensitivity(df: DataFrame): Unit = {
    //    # какие запахи ощущает чаще всего конкретный пользователь
    //    SELECT comment, count(*) as count
    //    FROM air_complaints_rzn
    //      where user_id = 574
    //    group by comment
    //    order by count
    ;
  }

  /**
   * заполняет статистику по жалобам на фенол по районам города
   * */
  def processFenol(df: DataFrame): Unit = {
    //    # рейтинг жалоб в каждом из районов города на конкретное вещество (в примере на фенол)
    //    SELECT area, count(*) as count
    //    FROM air_complaints_rzn
    //      where
    //    UPPER(comment) like UPPER('%фенол%') OR
    //    UPPER(comment) like UPPER('%сладковат%') OR
    //    UPPER(comment) like UPPER('%краск%') OR
    //    UPPER(comment) like UPPER('%гуаш%')
    //    group by area
    //    order by count
    ;
  }

  //  # рейтинг самых "вонючих" дней
  //  # рейтинг самых популярных часов у заводов для выбросов
  //  # в какой части города пользователь с id = 4 скорее всего работает ( это я =) )
  //  # какие запахи ощущает чаще всего конкретный пользователь
  //  # как часто поступают жалобы на конкретное вещество (в примере все описания соответствуют сероводороду)
  //  # рейтинг жалоб в каждом из районов города (на любые вещества)
  //  # рейтинг жалоб в каждом из районов города на конкретное вещество (в примере на фенол)

}
