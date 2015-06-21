package edu.utdallas
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.util.Vector
import scala.io.Source
import java.util.Properties

object Kmeans {
  def parseVector(line: String): Vector = {
      return new Vector(line.split(' ').map(_.toDouble))
      }
  def Cluster(p:Vector,centroid: Array[Vector]) :Int = {
    var Index =0
    var temp = Double.PositiveInfinity
    for(i<-0 until centroid.length) 
    {
      val dist = p.squaredDist(centroid(i))
      if(dist<temp) 
      {
        temp = dist
        Index =i
      }
    }
    return Index
  }
  def avg(p:Iterable[Vector]) : Vector = 
  {
    val size = p.size
    var cent =new Vector(p.head.elements)
    p.foreach(cent+=_)
 
    cent/size
  }
  def main(args: Array[String]) {
 
    val conf = new SparkConf().setAppName("Simple Application")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)


    val lines = sc.textFile("/Users/cy/Desktop/Q1.txt").cache()
    val data = lines.map(parseVector).cache()
    


    var centroids = data.takeSample(false,3,8)
    //centroids.foreach { print }
    for(i<-1 until 10)
    {
      var cluster = data.map(p=>(Cluster(p,centroids),p)) //calculate cluster index of one point in this iteration
      var group = cluster.groupByKey()
      var newcentroid = group.mapValues(p=>avg(p)).collectAsMap()
      // replace old centroid with new one
      for(newp<- newcentroid)
      {
        centroids(newp._1) = newp._2
      }
    }
    var cluster = data.map(p=>(p,Cluster(p,centroids)))
    println("After caculate each point with cluster:")
    cluster.foreach(println)
    centroids.foreach(print)
    
    //print(centroids(0))
//   centroids.foreach(print)
    
  }

}