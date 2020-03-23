package com.yeph.bigdata.dga

import java.util

import java.io.File
import scala.util.Random
import org.apache.commons.io.FileUtils

object GetData {
  def main(args: Array[String]): Unit = {
    val lines = new util.ArrayList[String]()
    val rand = new Random()
    var count = 1
    var randNum = -1
    for (i <- 1 until 1000) {
      for (j <- i + 1 until 1000) {
        randNum = rand.nextInt(498500)
        if (randNum < 10000) {
          lines.add(s"$i $j ${rand.nextInt(100) + 1} ${rand.nextInt(5)} ${rand.nextInt(2)}")
        }
      }
    }

    FileUtils.writeLines(new File("D:/test.txt"), lines)
  }

}
