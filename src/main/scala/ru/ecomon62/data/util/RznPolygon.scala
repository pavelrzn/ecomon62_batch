package ru.ecomon62.data.util

import java.awt.Polygon

class RznPolygon() extends Polygon {

  def addPoint(dx: Double, dy: Double): Unit = {
    val xy = d2Int(dx, dy)
    super.addPoint(xy._1, xy._2)
  }

  def isContains(x: Double, y: Double): Boolean = {
    val xy = d2Int(x, y)
    super.contains(xy._1, xy._2)
  }

  private def d2Int(x: Double, y: Double): (Int, Int) = {
    ((x * 100000).toInt, (y * 100000).toInt)
  }
}
