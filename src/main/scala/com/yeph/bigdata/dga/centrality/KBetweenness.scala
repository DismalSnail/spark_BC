package com.yeph.bigdata.dga.centrality

import it.unimi.dsi.fastutil.longs.LongSets
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object KBetweenness {
  def run[VD: ClassTag](graph: Graph[VD, Double]): RDD[((VertexId, VertexId), Double)] = {
    var graphlets = graph.mapVertices((_, _) => Array[(VertexId, VertexId, Double)]())
    graphlets.vertices.count()

    var graphK = graphlets.aggregateMessages[Array[(VertexId, VertexId, Double)]](
      e => {
        e.sendToDst(Array((e.srcId, e.dstId, e.attr)))
        e.sendToSrc(Array((e.srcId, e.dstId, e.attr)))
      },
      _ union _
    )

    var graphletsNew = graphlets.joinVertices(graphK)((_, _, newAttr) => newAttr)

    graphletsNew.vertices.count()
    graphletsNew.edges.count()

    graph.unpersistVertices(blocking = false)
    graph.edges.unpersist(blocking = false)

    graphK.unpersist(blocking = false)
    graphlets.unpersistVertices(blocking = false)
    graphlets.edges.unpersist(blocking = false)

    graphK = graphletsNew.aggregateMessages[Array[(VertexId, VertexId, Double)]](
      e => {
        val src = e.srcAttr.toSet
        val dst = e.dstAttr.toSet
        e.sendToDst((src -- dst).toArray)
        e.sendToSrc((dst -- src).toArray)
      },
      (a: Array[(VertexId, VertexId, Double)], b: Array[(VertexId, VertexId, Double)]) =>
        a.union(b).distinct
    )

    graphlets = graphletsNew.joinVertices(graphK)((_, oldAttr, newAttr) => oldAttr ++ newAttr)

    graphlets.vertices.count()
    graphlets.edges.count()

    graphK.unpersist(blocking = false)
    graphletsNew.unpersistVertices(blocking = false)
    graphletsNew.edges.unpersist(blocking = false)

    graphK = graphlets.aggregateMessages[Array[(VertexId, VertexId, Double)]](
      e => {
        val src = e.srcAttr.toSet
        val dst = e.dstAttr.toSet
        e.sendToDst((src -- dst).toArray)
        e.sendToSrc((dst -- src).toArray)
      },
      (a: Array[(VertexId, VertexId, Double)], b: Array[(VertexId, VertexId, Double)]) =>
        a.union(b).distinct
    )

    graphletsNew = graphlets.joinVertices(graphK)((_, oldAttr, newAttr) => oldAttr ++ newAttr)

    val ver: VertexRDD[Array[(VertexId, VertexId, Double)]] = graphletsNew.vertices
    ver.count()

    graphK.unpersist(blocking = false)
    graphlets.unpersistVertices(blocking = false)
    graphlets.edges.unpersist(blocking = false)
    graphletsNew.edges.unpersist(blocking = false)

//        Thread.sleep(10000000)

    ver.mapPartitions((data: Iterator[(VertexId, Array[(VertexId, VertexId, Double)])]) => {

      val S = mutable.Stack[VertexId]()
      val P = new mutable.HashMap[VertexId, ListBuffer[VertexId]]()
      val ord = Ordering.by[(Double, VertexId, VertexId), Double](_._1).reverse
      val Q = new mutable.PriorityQueue[(Double, VertexId, VertexId)]()(ord)

      val dist = new mutable.HashMap[VertexId, Double]()
      val sigma = new mutable.HashMap[VertexId, Double]()
      val delta = new mutable.HashMap[VertexId, Double]()
      val vlistSet = mutable.HashSet[VertexId]()
      var neighborMap: mutable.HashMap[VertexId, Array[VertexId]] = null
      val medBC = new mutable.HashMap[(VertexId, VertexId), Double]()


      while (data.hasNext) {
        val node = data.next()

        val vid = node._1
        var elist: Array[(VertexId, VertexId, Double)] = null
        var vlist: Array[VertexId] = null

        elist = node._2

        for (edge <- elist) {
          if (!medBC.contains((edge._1, edge._2))) {
            medBC((edge._1, edge._2)) = 0.0
          }
          vlistSet.add(edge._1)
          vlistSet.add(edge._2)
        }

        vlist = vlistSet.toArray
        vlistSet.clear()

        neighborMap = getNeighborMap(vlist, elist)

        for (vertex <- vlist) {
          dist.put(vertex, -1)
          sigma.put(vertex, 0.0)
          delta.put(vertex, 0.0)
          P.put(vertex, ListBuffer[VertexId]())
        }

        vlist = null

        sigma(vid) = 1.0
        val seen = new mutable.HashMap[VertexId, Double]()
        seen(vid) = 0
        Q.enqueue((0.0, vid, vid))

        def getDist(v: VertexId, w: VertexId) = {
          elist.find(e => (e._1 == v && e._2 == w) || (e._2 == v && e._1 == w)).get._3
        }

        while (Q.nonEmpty) {
          val (d, pred, v) = Q.dequeue()
          if (dist(v) > 0) { //节点v已经访问过了
            null
          }
          else {
            sigma(v) += sigma(pred)
            S.push(v)
            dist(v) = d
            for (w <- neighborMap(v)) {
              val vw_dist = d + getDist(v, w)
              if (dist(w) < 0 && (!seen.contains(w) || vw_dist < seen(w))) {
                seen(w) = vw_dist
                Q.enqueue((vw_dist, v, w))
                sigma(w) = 0.0
                P(w) = ListBuffer[VertexId](v)
              }
              else if (vw_dist == seen(w)) {
                sigma(w) += sigma(v)
                P(w).+=(v)
              }
            }
          }
        }

        dist.clear()
        neighborMap.clear()
        elist = null

        while (S.nonEmpty) {
          val w = S.pop()
          val coeff = (1 + delta(w)) / sigma(w)
          for (v <- P(w)) {
            val c = sigma(v) * coeff
            if (!medBC.contains((v, w))) {
              medBC((w, v)) += c / 2
            } else {
              medBC((v, w)) += c / 2
            }
            delta(v) += c
          }
        }

        S.clear()
        P.clear()
        Q.clear()
        sigma.clear()
        delta.clear()
      }
      medBC.toIterator
    }).reduceByKey(_ + _)
  }

  def runOne[VD: ClassTag](sc: SparkContext, number: Long, graph: Graph[VD, Double]) = {
    var bGraph = graph.mapVertices((id, _) => if (id == number) true else false)
    var bRDD: VertexRDD[Boolean] = null
    var count = 3
    while (count > 0) {
      bRDD = bGraph.aggregateMessages[Boolean](
        e => {
          if (e.srcAttr ^ e.dstAttr) {}
          if (e.srcAttr) {
            e.sendToDst(true)
          } else {
            e.sendToSrc(true)
          }
        },
        _ || _
      )

      bGraph = bGraph.joinVertices(bRDD)(
        (_, oldB, newB) => oldB || newB
      )

      count = count - 1
    }

    bGraph = bGraph.subgraph(triplet => triplet.srcAttr && triplet.dstAttr, (id, attr) => attr)
    val elist = bGraph.edges.map(edge => (edge.srcId, edge.dstId, edge.attr)).collect()

    val vertexSet = mutable.HashSet[VertexId]()
    val medBC = new mutable.HashMap[(VertexId, VertexId), Double]()

    val S = mutable.Stack[VertexId]()
    val P = new mutable.HashMap[VertexId, ListBuffer[VertexId]]()
    val ord = Ordering.by[(Double, VertexId, VertexId), Double](_._1).reverse
    val Q = new mutable.PriorityQueue[(Double, VertexId, VertexId)]()(ord)

    val dist = new mutable.HashMap[VertexId, Double]()
    val sigma = new mutable.HashMap[VertexId, Double]()
    val delta = new mutable.HashMap[VertexId, Double]()
    val vlistSet = mutable.HashSet[VertexId]()

    for (edge <- elist) {
      vertexSet.add(edge._1)
      vertexSet.add(edge._2)
      if (edge._1 == number || edge._2 == number) {
        medBC((edge._1, edge._2)) = 0.0
      }
    }

    val neighborMap = getNeighborMap(vertexSet.toArray, elist)

    for (node <- vertexSet) {

      for (vertex <- vertexSet) {
        dist.put(vertex, -1)
        sigma.put(vertex, 0.0)
        delta.put(vertex, 0.0)
        P.put(vertex, ListBuffer[VertexId]())
      }

      sigma(node) = 1.0
      val seen = new mutable.HashMap[VertexId, Double]()
      seen(node) = 0
      Q.enqueue((0.0, node, node))

      def getDist(v: VertexId, w: VertexId) = {
        elist.find(e => (e._1 == v && e._2 == 2) || (e._2 == v && e._1 == 2)).get._3
      }

      while (Q.nonEmpty) {
        val (d, pred, v) = Q.dequeue()
        if (dist(v) > 0) { //节点v已经访问过了
          null
        }
        else {
          sigma(v) += sigma(pred)
          S.push(v)
          dist(v) = d
          for (w <- neighborMap(v)) {
            val vw_dist = d + getDist(v, w)
            if (dist(w) < 0 && (!seen.contains(w) || vw_dist < seen(w))) {
              seen(w) = vw_dist
              Q.enqueue((vw_dist, v, w))
              sigma(w) = 0.0
              P(w) = ListBuffer[VertexId](v)
            }
            else if (vw_dist == seen(w)) {
              sigma(w) += sigma(v)
              P(w).+=(v)
            }
          }
        }
      }

      dist.clear()

      while (S.nonEmpty) {
        val w = S.pop()
        val coeff = (1 + delta(w)) / sigma(w)
        for (v <- P(w)) {
          val c = sigma(v) * coeff
          if (!medBC.contains((v, w))) {
            medBC((w, v)) += c / 2
          } else {
            medBC((v, w)) += c / 2
          }
          delta(v) += c
        }
      }

      S.clear()
      P.clear()
      Q.clear()
      sigma.clear()
      delta.clear()
      vlistSet.clear()
    }
    medBC.toArray
  }

  def getNeighborMap(vlist: Array[VertexId], elist: Array[(VertexId, VertexId, Double)]): mutable.HashMap[VertexId, Array[VertexId]] = {
    val neighborList = new mutable.HashMap[VertexId, Array[VertexId]]()
    vlist.map(v => {
      val nlist = (elist.filter(e => (e._1 == v || e._2 == v))).map(e => {
        if (v == e._1) e._2
        else e._1
      })
      neighborList.+=((v, nlist.distinct))
    })
    neighborList
  }
}
