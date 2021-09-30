package ru.ecomon62.data.util

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object UdfLib extends Serializable {

  // часть кального, олимпийский, часть соколовки, ДП
  val polygonSE: RznPolygon = new RznPolygon()
  polygonSE.addPoint(54.626823, 39.793990)
  polygonSE.addPoint(54.606077, 39.776514)
  polygonSE.addPoint(54.589725, 39.816530)
  polygonSE.addPoint(54.610113, 39.855568)
  // канищево, приокский, московский от м5 молла до дягилево
  val polygonNW: RznPolygon = new RznPolygon()
  polygonNW.addPoint(54.710203, 39.600976)
  polygonNW.addPoint(54.670848, 39.705156)
  polygonNW.addPoint(54.634902, 39.620188)
  polygonNW.addPoint(54.662467, 39.549083)
  // центр
  val polygonCenter: RznPolygon = new RznPolygon()
  polygonCenter.addPoint(54.638579, 39.736846)
  polygonCenter.addPoint(54.627778, 39.766358)
  polygonCenter.addPoint(54.615365, 39.744175)
  polygonCenter.addPoint(54.623411, 39.701591)
  // Солотча
  val polygonSolotcha: RznPolygon = new RznPolygon()
  polygonSolotcha.addPoint(54.821764, 39.834384)
  polygonSolotcha.addPoint(54.741431, 39.727631)
  polygonSolotcha.addPoint(54.707524, 39.842400)
  polygonSolotcha.addPoint(54.804325, 39.954779)

  def defineArea: UserDefinedFunction = udf((x: Double, y: Double) => {
    if (polygonCenter.isContains(x, y)) "Центр"
    else if (polygonNW.isContains(x, y)) "Северо-Запад"
    else if (polygonSE.isContains(x, y)) "Юго-Восток"
    else if (polygonSolotcha.isContains(x, y)) "Солотча"
    else null

  })

}
