package SparkPack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.io._



object Spark_Prasad_Obj {
  
def main(args:Array[String]):Unit={ 


			val conf=new SparkConf().setAppName("spark_integration").setMaster("local[*]")
					val sc=new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
					import spark.implicits._

					println()
					println("============Raw  DF============")
					println()

					val url = "https://randomuser.me/api/0.8/?results=10"
					val Result = scala.io.Source.fromURL(url).mkString
					val RDD = sc.parallelize(List(Result))
					val RawDF = spark.read.json(RDD)
					RawDF.show()
					RawDF.printSchema()
					
					println()
					println("============First Explode  DF============")
					println()	
					
					val ExpDF1 = RawDF.withColumn("results", explode(col("results")))
					
					ExpDF1.show()
					ExpDF1.printSchema()
					
					println()
					println("============Final DF============")
					println()	
					
					val FinalDF = ExpDF1.select(
					    
					col("nationality").alias("user_nationality"),
					col("results.user.cell").alias("user_cell"),
					col("results.user.dob").alias("user_dob"),
					col("results.user.email").alias("user_email"),
					col("results.user.location.city").alias("user_city"),
					col("results.user.location.state").alias("user_state"),
					col("results.user.location.street").alias("user_street"),
					col("results.user.location.zip").alias("user_zip"),
					col("results.user.md5").alias("user_md5"),
					col("results.user.name.first").alias("user_first_name"),
					col("results.user.name.last").alias("user_last_name"),
					col("results.user.name.title").alias("user_title_name"),
					col("results.user.username").alias("user_username"),
					col("results.user.password").alias("user_password"),
					col("results.user.phone").alias("user_phone"),
					col("results.user.salt").alias("user_salt"),
					col("results.user.sha1").alias("user_sha1"),
					col("results.user.sha256").alias("user_sha256"),
					col("results.user.picture.large").alias("user_picture_large"),
					col("results.user.picture.medium").alias("user_picture_medium"),
					col("results.user.picture.thumbnail").alias("user_picture_thumbnail"),
					col("results.user.registered").alias("user_registered"),
					col("seed"),
					col("version")
					)
					FinalDF.show()
					FinalDF.printSchema()
	

	}

}