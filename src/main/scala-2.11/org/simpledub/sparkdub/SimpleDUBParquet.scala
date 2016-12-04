package org.simpledub.sparkdub
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import java.io.PrintWriter

import scala.collection.mutable
import scala.math._
/**
  * Created by root on 11/4/16.
  */
object SimpleDUBParquet {
    def minimum(a: Int*) = a.min

    def editDistanceSimilarityPercent(stra:String, strb:String): Double = {
      if(stra == null && strb == null) 1
      else if(stra == null || strb == null) 0
      else{
        val a = stra.toLowerCase
        val b = strb.toLowerCase
        val length1 = a.length
        val length2 = b.length
        val distance = Array.tabulate(length1+1,length2+1){(i,j)=>if(i==0) j else if(j == 0) i else 0}
        for(i<- 1 to length1; j<- 1 to length2){
          val d = if(a(i-1) == b(j-1)) 0 else 1 // if a ith == b jth, then don't need to modify the last character
          distance(i)(j) = minimum(distance(i)(j-1)+1, distance(i-1)(j)+1, distance(i-1)(j-1)+d)
        } //end for
        1- distance(length1)(length2) / max(length1,length2).toDouble
      } //end else
    }

    // compute the similarity of two numbers.
    // stra, strb are numeric like string
    def numSimilarityPercent(stra:String, strb:String): Double = {
      val num1 = if(stra == "") 0 else stra.replace("\"", "").replace("gbp","").toDouble
      val num2 = if(strb == "") 0 else strb.replace("\"", "").replace("gbp","").toDouble

      if(max(num1,num2) == 0) 1 else min(num1,num2) / max(num1,num2)
    }

    //compute the square distance of two points
    def sqdist(a:Array[Double], b:Array[Double]) :Double =
    a.zip(b).map{case(x1,x2)=>(x1-x2)*(x1-x2)}.sum

    // compute the density of the given point p
    var densityCount = 0
    def density(p:Array[Double], points:Dataset[ (String,String,Array[Double]) ], r:Double)={
      densityCount += 1
      points.filter(x=>sqdist(p,x._3) < r*r).count

    }


    //transform num to its binary representation,
    //then return a list that indicates which positions contain 1
    def onePositionSet(num: Int, pos:Int): List[Int]={
      num match {
        case 0 => Nil
        case x => if(x%2 == 1) pos::onePositionSet(num/2,pos+1) else onePositionSet(num/2,pos+1)
      }
    }

    var searchCount = 0
    /**
      * Phase1: Binary Search for the boundary points set in virtual points region
      * @param minPoint: minimum point of current searching region
      * @param maxPoint: maximum point of current searching region
      * @param k: virtual point limit defined by user.(granularity parameter)
      * @param r: the Euler distance in the density definition
      * @param T1: the density threshold
      * @param points: the RDD that contains all of the record pair points
      * @return Boundary Points set M
      */
    def search(minPoint:Array[Int], maxPoint:Array[Int], k:Int, r:Double,
               T1:Int, points:Dataset[(String,String,Array[Double])]) : Vector[Array[Int]]={

      searchCount += 1
      def searchHelper(n:Int, boundPoint:Array[Int]):Vector[Array[Int]]={
        val min = minPoint.clone() //min point of the current space
        val max = boundPoint map (_ -1) //max point of the current space
        for(e<- onePositionSet(n,0)){
          min.update(e,boundPoint(e)+1)
          max.update(e,maxPoint(e))
        }
        search(min,max,k,r,T1,points) ++ {if(n >= 2) searchHelper(n - 1, boundPoint) else Vector()}

      } //end function searchHelper

      // firstly determine if there are no virtual points in current space
      val midPoint = (minPoint zip maxPoint).map(x=>(x._1+x._2)/2)
      val maxGreaterThanMin:Boolean = (maxPoint zip minPoint) forall(x=>x._1>=x._2)

      if(!maxGreaterThanMin || minPoint.sameElements(midPoint) || density(minPoint map (_.toDouble/k),points,r)<T1) Vector()
      else{
        if(density(maxPoint map (_.toDouble/k), points,r) >= T1) Vector(maxPoint)
        else {
          val MaxBoundP = findMaxBound(minPoint, maxPoint, k, r, T1, points)
          MaxBoundP +: searchHelper(pow(2, MaxBoundP.length).toInt - 2, MaxBoundP)
        }
      }
    }

    /**Find MaxPoint of the current region. The region is represented as the minimum point and the maximum point
      * All virtual points between minPoint and maxPoint belong to the current region
      * @param minPoint: the minimum point of the current region and density(minPoint) >= T1
      * @param maxPoint: the maximum point of the current region and density(maxPoint) < T1
      * @param k: granularity parameter
      * @param r: the Euler distance in the density definition
      * @param T1: the density threshold
      * @param points: the RDD that contains all of the record pair points
      * @return maxPoint of the current region
      */
    def findMaxBound(minPoint:Array[Int], maxPoint:Array[Int], k:Int, r:Double,
                     T1:Int, points:Dataset[(String,String,Array[Double])]) : Array[Int] ={

      if(density(minPoint map (_.toDouble/k), points, r)<T1) throw new Exception("There's no MaxBoundPoint in current space whose density >T1")

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

    /**filter all the elements which are within the Boundary Set
      *
      * @param points
      * @param boundarySet
      * @param k
      * @return the remaining points
      */
    def exclude(points:Dataset[(String,String,Array[Double])], boundarySet:Vector[Array[Int]], k:Int)
    :Dataset[(String,String,Array[Double])]={
      def notLessThan(a:Array[Double], b:Array[Double])= (a zip b).exists{case(x,y)=> x>y}

      points.filter(p=>boundarySet.forall(b=>notLessThan(p._3,b map(_.toDouble/k))))
    }

    def main(args: Array[String]): Unit = {
      val fileResultMapping = "file:///root/software/RecordMatching/Amzon_GoogleProducts_perfectMapping.csv"
      val warehousePath = "file:///root/software/RecordMatching/data/"
      val out = new PrintWriter("/root/software/RecordMatching/data/parquetMatch.txt")

      val ss = SparkSession.builder().appName("ParquetDUB").getOrCreate()
//      ss.conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

      val dim = 4 // 4 features

      // points's element is type of DataSet[(String, String, Array[Double](4))]
      import ss.implicits._
      val points = ss.read.parquet(warehousePath+"points2.parquet").map(row=> (row.getString(0),
        row.getString(1), row.getSeq[Double](2).toArray) ).persist(StorageLevel.MEMORY_AND_DISK)

      val totalCount = points.count

      out.println("total:"+totalCount)
//      out.close()

      val k = 50
      val r = 0.15
      val T1 = 6000
      val T2 = 5000

      val boundarySet = search(Array(0,0,0,0),Array(k,k,k,k), k, r, T1,points)


      val similarPoints = exclude(points,boundarySet,k).persist(StorageLevel.MEMORY_AND_DISK) // remove the points inside the boundary

      //***** Output boundarySet to external file**********
      out.println("**************result list******************")
      out.println("k:"+k)
      out.println("r:"+r)
      out.println("T1:"+T1)
      out.println("T2:"+T2)
      out.println("Phase1 boundarySet size:"+boundarySet.length)
      out.println("boundarySet:")
      out.println("(")
      for(arr<-boundarySet){
        out.println(arr.mkString("[", ",", "]"))
      }
      out.println(")")

      // initialize the neighbour points set which density > T2
      val q = new mutable.Queue[Array[Int]]()
      q ++= boundarySet

      val phase2Boundary = new mutable.Queue[Array[Int]]()
//      phase2Boundary ++= boundarySet

      out.println("before:")
      out.println("phase2Boundary size:"+phase2Boundary.length)
      out.println("q size:"+q.size)

      while(q.nonEmpty){
        val current = q.dequeue()
        for(i<- current.indices; j<- -1 to 1 by 2){
          val neighbour = current.clone()
          neighbour(i) += j
          if(neighbour(i)>=0 && neighbour(i)<=k && !containsElem(phase2Boundary,neighbour)
            && !containsElem(q,neighbour)){
            val den = density(neighbour map (_.toDouble/k),similarPoints,r)
            if(den>=T2 && den<T1){
              q += neighbour
              phase2Boundary += neighbour
            }
          } //end outer if

        }//end for
      }// end while

      //    points.unpersist(false) //Unpersist points DataSet which is no longer used
      val matchedPair = similarPoints.filter(x=>phase2Boundary forall (p=>sqdist(p map (_.toDouble/k), x._3)>r*r) )

      val result = matchedPair.drop("_3").persist(StorageLevel.MEMORY_AND_DISK)
      val perfectMapping = ss.read.option("header","true").csv(fileResultMapping)

      val mappingCount = perfectMapping.count

      val resultCount = result.count
      result.write.option("header","true").mode("overwrite").csv(warehousePath+"resultPairTest")

      val trueMatchedCount = perfectMapping.intersect(result).count
      val recall = trueMatchedCount.toDouble/mappingCount

      val B = totalCount - resultCount
      val B_match = mappingCount - trueMatchedCount
      val B_non = B-B_match
      val reductionRatio = B_non.toDouble/(totalCount-mappingCount)
//      val reductionRatio = 1- (resultCount-trueMatchedCount).toDouble/(totalCount-mappingCount)

      out.println("searchCount:"+searchCount)
      out.println("densityCount:"+densityCount)

      out.println("After phase2:")
      out.println("phase2 Boundary size:"+phase2Boundary.length)
      out.println("phase2Boundary:")
      out.println("(")
      for(arr<-phase2Boundary){
        out.println(arr.mkString("[", ",", "]"))
      }
      out.println(")")
      out.println("q:"+q)

      out.println("Total record count:"+totalCount)
      out.println("resultPair:"+resultCount)
      out.println("true Matched:"+trueMatchedCount)
      out.println("mappingCount:"+mappingCount)
      out.println("Recall:"+ recall)
      out.println("Reduction ratio:"+ reductionRatio)
      out.close()

      ss.stop()
    } //end main

    def containsElem(c:mutable.Iterable[Array[Int]], elem:Array[Int]):Boolean={
      for(e<-c){
        if(e sameElements elem) return true
      }
      false
    }

}
