package org.distributeddub.sparkdub

import java.io.PrintWriter

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.math._

/**
  * Created by root on 11/3/16.
  */

class DistributedDUB extends Serializable {
  def minimum(a: Int*) = a.min

  def editDistanceSimilarityPercent(stra: String, strb: String): Double = {
    if (stra == null && strb == null) 1
    else if (stra == null || strb == null) 0
    else {
      val a = stra.toLowerCase
      val b = strb.toLowerCase
      val length1 = a.length
      val length2 = b.length
      val distance = Array.tabulate(length1 + 1, length2 + 1) { (i, j) => if (i == 0) j else if (j == 0) i else 0 }
      for (i <- 1 to length1; j <- 1 to length2) {
        val d = if (a(i - 1) == b(j - 1)) 0 else 1 // if a ith == b jth, then don't need to modify the last character
        distance(i)(j) = minimum(distance(i)(j - 1) + 1, distance(i - 1)(j) + 1, distance(i - 1)(j - 1) + d)
      } //end for
      1 - distance(length1)(length2) / max(length1, length2).toDouble
    } //end else
  }

  // compute the similarity of two numbers.
  // stra, strb are numeric like string
  def numSimilarityPercent(stra: String, strb: String): Double = {
    val num1 = if (stra == "") 0 else stra.replace("\"", "").replace("gbp", "").toDouble
    val num2 = if (strb == "") 0 else strb.replace("\"", "").replace("gbp", "").toDouble

    if (max(num1, num2) == 0) 1 else min(num1, num2) / max(num1, num2)
  }

  //compute the square distance of two points
  def sqdist(a: Array[Double], b: Array[Double]): Double =
  a.zip(b).map { case (x1, x2) => (x1 - x2) * (x1 - x2) }.sum

  // compute the density of the given point p
  //  var densityCount = 0
  def density(p: Array[Double], points: Array[Array[Double]], r: Double) = {
    //    densityCount += 1
    points.count(x => sqdist(p, x) < r * r)

  }


  //transform num to its binary representation,
  //then return a list that indicates which positions contain 1
  def onePositionSet(num: Int, pos: Int): List[Int] = {
    num match {
      case 0 => Nil
      case x => if (x % 2 == 1) pos :: onePositionSet(num / 2, pos + 1) else onePositionSet(num / 2, pos + 1)
    }
  }

  //  var searchCount = 0
  /**
    * Phase1: Binary Search for the boundary points set in virtual points region
    *
    * @param minPoint : minimum point of current searching region
    * @param maxPoint : maximum point of current searching region
    * @param k        : virtual point limit defined by user.(granularity parameter)
    * @param r        : the Euler distance in the density definition
    * @param T1       : the density threshold
    * @param points   : the RDD that contains all of the record pair points
    * @return Boundary Points set M
    */
  def search(minPoint: Array[Int], maxPoint: Array[Int], k: Int, r: Double,
             T1: Int, points: Array[Array[Double]]): Vector[Array[Int]] = {

    //    searchCount += 1
    def searchHelper(n: Int, boundPoint: Array[Int]): Vector[Array[Int]] = {
      val min = minPoint.clone() //min point of the current space
      val max = boundPoint map (_ - 1) //max point of the current space
      for (e <- onePositionSet(n, 0)) {
        min.update(e, boundPoint(e) + 1)
        max.update(e, maxPoint(e))
      }
      search(min, max, k, r, T1, points) ++ {
        if (n >= 2) searchHelper(n - 1, boundPoint) else Vector()
      }

    } //end function searchHelper

    // firstly determine if there are no virtual points in current space
    val midPoint = (minPoint zip maxPoint).map(x => (x._1 + x._2) / 2)
    val maxGreaterThanMin: Boolean = (maxPoint zip minPoint) forall (x => x._1 >= x._2)

    if (!maxGreaterThanMin || minPoint.sameElements(midPoint) || density(minPoint map (_.toDouble / k), points, r) < T1) Vector()
    else {
      if (density(maxPoint map (_.toDouble / k), points, r) >= T1) Vector(maxPoint)
      else {
        val MaxBoundP = findMaxBound(minPoint, maxPoint, k, r, T1, points)
        MaxBoundP +: searchHelper(pow(2, MaxBoundP.length).toInt - 2, MaxBoundP)
      }
    }
  }

  def phase1Search(k: Int, r: Double, T1: Int, points: Array[Array[Double]], dim: Int) = {
    val minPoint = new Array[Int](dim)
    val maxPoint = new Array[Int](dim)
    for (i <- 0 until dim) {
      minPoint(i) = 0
      maxPoint(i) = k
    }
    search(minPoint, maxPoint, k, r, T1, points)
  }

  /** Find MaxPoint of the current region. The region is represented as the minimum point and the maximum point
    * All virtual points between minPoint and maxPoint belong to the current region
    *
    * @param minPoint : the minimum point of the current region and density(minPoint) >= T1
    * @param maxPoint : the maximum point of the current region and density(maxPoint) < T1
    * @param k        : granularity parameter
    * @param r        : the Euler distance in the density definition
    * @param T1       : the density threshold
    * @param points   : the RDD that contains all of the record pair points
    * @return maxPoint of the current region
    */
  def findMaxBound(minPoint: Array[Int], maxPoint: Array[Int], k: Int, r: Double,
                   T1: Int, points: Array[Array[Double]]): Array[Int] = {

    if (density(minPoint map (_.toDouble / k), points, r) < T1) throw new Exception("There's no MaxBoundPoint in current space whose density >T1")

    val p = minPoint.clone()
    for (i <- p.indices) {
      var lo = p(i)
      var hi = maxPoint(i)
      if (density(p.updated(i, hi) map (_.toDouble / k), points, r) >= T1) {
        p(i) = hi
      }
      else {
        // lo <= the i-th dimension of MaxPoint < hi
        while (hi - lo >= 2) {
          val midValue = (hi + lo) / 2
          if (density(p.updated(i, midValue) map (_.toDouble / k), points, r) >= T1)
            lo = midValue
          else
            hi = midValue
        } // end while
        p(i) = lo
      }

    } // end for
    p // return p as the MaxPoint
  }

  /** filter out the elements which are within the Boundary Set
    * The remaining points are those who are not dominated by any points in the boundary set.
    *
    * @param pointsArray : the original points set before filter
    * @param boundarySet : the boundary set
    * @param k           : granularity parameter
    * @return : the remaining points
    */
  def exclude(pointsArray: Array[Array[Double]], boundarySet: Vector[Array[Int]], k: Int)
  : Array[Array[Double]] = {
    def notDominateBy(a: Array[Double], b: Array[Double]) = (a zip b).exists { case (x, y) => x > y }

    pointsArray.filter(p => boundarySet.forall(b => notDominateBy(p, b map (_.toDouble / k))))
  }
}

object test{
  def main(args: Array[String]): Unit = {
    val warehousePath = "file:///root/software/RecordMatching/data/"

    val dim = 4 // 4 features
    val k = 50
    val r = 0.25
    val T1 = 250
    val T2 = 5000
    val numPartitions = 2

    val ss = SparkSession.builder().master("spark://10.1.0.23:7077").appName("DistributedDUB").getOrCreate()

    // points's element is type of DataSet[(String, String, Array[Double](4))]
//    import ss.implicits._
//    val points = ss.read.parquet(warehousePath+"points2.parquet").map(row=> (row.getString(0),
//      row.getString(1), row.getSeq[Double](2).toArray) ).persist(StorageLevel.MEMORY_AND_DISK)

    val points = ss.sparkContext.textFile("hdfs://10.0.0.23:9000/dblp_vs_acm").repartition(numPartitions)
      .map(line=>line.split(",").take(dim)).map(pArray=>pArray.map(_.toDouble))
    points.persist(StorageLevel.MEMORY_AND_DISK)

    val totalCount = points.count

    println("total:"+totalCount)

    val dubObj = new DistributedDUB()
    val broadcastDUB = ss.sparkContext.broadcast(dubObj)

    val boundarySet = points.mapPartitionsWithIndex( (partitionId, iter) => {
      val pointsArray = iter.toArray

      val localBoundarySet = broadcastDUB.value.phase1Search(k, r, T1, pointsArray, dim)

      Seq((partitionId,localBoundarySet)).iterator
    })

    val globalBoundarySet = boundarySet.collect()


    // print the result boundary set by per partition
    globalBoundarySet.foreach{ case(id,vec) => {
      println("(partitionId:"+id)
      vec.foreach(arr => println(arr.mkString("[",",","], ")))
      println(")") }
    }

    ss.stop()

  }
}
