package edu.utdallas
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.util.Vector
import org.apache.spark.rdd.RDD
import scala.io.Source
import java.util.Properties


object Resystem {
  def parse(a:Array[String]) : Array[String]=
  {
    var z = new Array[String](5)
     for(i<-0 until 5)
     {
       z(i) = a(i).split(":")(0)
     }
    return z
   }
  def find_name1( n : String, m: scala.collection.Map[String,String]) = {
    println(n+":"+m.get(n).mkString)
    
  }
  def find_name2(n:Array[String], m: scala.collection.Map[String,String]) = {
    
    for(i<-0 until n.length)
    {
      print(n(i)+":"+m.get(n(i)).mkString+",")
    }
    
  }
  def main(args : Array[String])
  {
    val conf = new SparkConf().setAppName("recommend system")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    var lines = sc.textFile("/Users/cy/Desktop/part-00000").cache()
    val rating  = sc.textFile("/Users/cy/Desktop/ratings.dat") 
    val movie = sc.textFile("/Users/cy/Desktop/movies.dat")
    val m = movie.map(x=>(x.split("::")(0),x.split("::")(1))).collectAsMap
    val key= args(0)
    val r = rating.map(x=>(x.split("::")(0),x.split("::")(1),x.split("::")(2))).filter(_._1==key).filter(_._3=="3").map(p=>(p._2)).toArray()
    
    lines = lines.filter{x=> var key = x.split("\t")(0)
      r.contains(key)}
    val data = lines.map(x=>(x.split("\t")(0),x.split("\t")(1))).mapValues(b=>b.split(" ")).mapValues(c=>parse(c))
    //val d = data.mapValues(p=>parse(p)).collectAsMap()
//   data.flatMapValues(x=>x)
//   data.foreach(println)
    
    //val d =  data.map(x=>( x._1+: x._2))
    data.foreach{x=>find_name1(x._1,m)
      print("here is what we recommend:")
     find_name2(x._2,m)
     println()}
 //   data.foreach(x=>println(x._1))
    //data.foreach(println)
 //       val bcTable = sc.broadcast(m.collectAsMap)
//    val flattenMD = d.mapPartitions { iter=>
//      val table = bcTable.value
//      iter.flatMap(ele=>(ele,table(ele)))}
//    val res = d.flatMap { arr => arr.map(ele => (ele,bcTable.value(ele)))}
//    res.foreach(println)
 
//    val flat = d.flatMap(_.toSeq).keyBy { x => x }
//    val res = flat.join(m).map{ case (k,v) => v}
//    res.foreach(println)
    
    
  }

}