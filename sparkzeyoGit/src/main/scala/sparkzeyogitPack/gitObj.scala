package sparkzeyogitPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object gitObj {
  
  def main(args:Array[String]):Unit={
  
  val conf = new SparkConf().setAppName("First").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					
		val spark=SparkSession.builder().getOrCreate()
		import spark.implicits._
		
		val df=spark.read.format("json").option("multiLine","true").load("file:///C:/data/JsonArraydata.json")
		df.show()
		df.printSchema()
		
		println("----Explode----")
    val explodedf=df.select("No","Students","address.permanent_address","address.temporary_address",
                            "firstname","lastname").withColumn("Students",explode(col("Students")))
    explodedf.show()
    explodedf.printSchema()
                          
    println("----explode complex generation----")
    val explodegeneration=explodedf.groupBy("No","firstname","lastname","permanent_address","temporary_address")
                           .agg(collect_list("Students").alias("Students"))
    explodegeneration.show()
    explodegeneration.printSchema()
    println("-----Bact to complex----")
    val complexgeneration=explodegeneration.select(
                                               col("No"),
                                               col("firstname"),
                                               col("lastname"),
                                               col("Students"),
                                               struct(
                                                   col("permanent_address"),col("temporary_address")).alias("address")
                                                   )
     complexgeneration.show()
     complexgeneration.printSchema()
     println("----DataFrame2----")
    val df2=spark.read.format("json").option("multiLine","true").load("file:///C:/data/JsonArraydata2.json")
		df2.show()
		df2.printSchema()
		val explodedf2=df2.select("No","Students","address.permanent_address","address.temporary_address","firstname","lastname","year")
		                   .withColumn("Students",explode(col("Students")))
		
		explodedf2.show()
		explodedf2.printSchema()
		val explodestruct=explodedf2.select("No","Students.Gender","Students.name","permanent_address","temporary_address",
		                                     "firstname","lastname","year")
		explodestruct.show()
		explodestruct.printSchema()
		println("----Complex back----")
		val backtoComplex= explodestruct.groupBy("No","permanent_address","temporary_address",
		                                     "firstname","lastname","year")
		                                     .agg(collect_list(
		                                         struct(
		                                             col("Gender"),
		                                             col("name"))).alias("Students"))
		                                       .select(
		                                           col("Students"),
		                                               struct(
		                                                   col("permanent_address"),
		                                                   col("temporary_address")).alias("address"),
		                                                   
		                                                   col("firstname"),
		                                                   col("lastname"),
		                                                   col("year"),
		                                                   col("No")) 
		                                                   
		                                                   
		 backtoComplex.show()
		 backtoComplex.printSchema()
		println("----DataFrame3----")
		val df3=spark.read.format("json").option("multiLine","true").load("file:///C:/data/JsonArraydata3.json")
		df3.show()
		df3.printSchema()
		
		val explodestruct2=df3.select("No","Students","address.permanent_address","address.temporary_address","firstname","lastname","year")
		                   .withColumn("Students",explode(col("Students")))
		explodestruct2.show()
		explodestruct2.printSchema()  //Here we can go with Students.name.first
		
		val explodestruct3=explodestruct2.select("No","Students.Gender","Students.name","permanent_address","temporary_address",
		                    "firstname","lastname","year")
		explodestruct3.show()
		explodestruct3.printSchema()
		val explodestruct4=explodestruct3.select("No","Gender","name.first","name.last","name.title","permanent_address","temporary_address",
		                    "firstname","lastname","year")
		explodestruct4.show()
		explodestruct4.printSchema()
		
}}
