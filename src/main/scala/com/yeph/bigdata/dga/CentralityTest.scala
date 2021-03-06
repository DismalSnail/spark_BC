package com.yeph.bigdata.dga

import com.yeph.bigdata.dga.centrality.{GraphFactory, KBetweenness}
import it.unimi.dsi.fastutil.longs.LongSets.Singleton
import org.apache.spark.sql.SparkSession

object CentralityTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("AlgorithmTest")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
      .sparkContext
    val path = "D:/test.txt"
    val graphFactory = new GraphFactory
    Singleton

    val coeff = Array(0.1, 0.2, 0.3)
    val graph = graphFactory.build(spark, path, coeff)
    val result = KBetweenness.run(graph)
    println(result.collect().take(200).mkString("\n"))
    Thread.sleep(10000000)
    spark.stop()
  }
}
