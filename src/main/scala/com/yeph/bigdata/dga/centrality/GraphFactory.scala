package com.yeph.bigdata.dga.centrality

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

class GraphFactory {
  def build(sc: SparkContext, path: String, coeff: Array[Double]): Graph[None.type, Double] = {
    val fileRDD: RDD[String] = sc.textFile(path)
    val edgeRDD: RDD[Edge[Double]] = fileRDD.map((line: String) => {
      val token: Array[String] = line.split(" ")
      var score = 0.0
      for (i <- coeff.indices) {
        score = score + coeff(i) * token(i + 2).toDouble
      }
      Edge(token(0).toLong, token(1).toLong, score)
    })
    Graph.fromEdges(edgeRDD, None)
  }
}
