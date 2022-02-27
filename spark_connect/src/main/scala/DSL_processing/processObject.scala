package DSL_processing

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.io.FileNotFoundException
import java.io.IOException


trait df_trait {
  val bkMap = Map(101 -> "JOB SUCCESS", 102 -> "FAIL")
  val stringLiteral = "Set-Up is Successful"
  def toUpper(word:String):String = word.toUpperCase()
}

case class comp_class() extends df_trait {
    def StringOps(s:String,n:String => String) = {
    if (s != null) n(s)
    else
      bkMap.get(102).getOrElse("Not Available")
  }
    
    def factorial(inputNum:Int):BigInt = {
      if (inputNum==1 || inputNum==0) {
        return 1 } else
        { inputNum * factorial(inputNum-1)
        }
      }
    }

object processObject extends comp_class with df_trait {
  def main(args:Array[String]) = {
    
    val conf = new SparkConf().setAppName("Connect Apprenticeship Project").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val classInstance = comp_class()
    println(classInstance.StringOps(stringLiteral,toUpper))
    
    //////////RDD PROCESSING//////////
    
    val nestedList = List("State-Alabama,Capital-Montgomery,Code-36066", "State-Alaska,Capital-Jeneau,Code-99824", 
                          "State-Wyoming,Capital-Cheyenne,Code-80802", "State-Wisconsin,Capital-Madison,Code-53593",
                          "State-California,Capital-Sacramento,Code-95608", "State-Georgia,Capital-Atlanta,Code-30004",
                          "State-Hawaii,Capital-Honolulu,Code-96712", "State-Kansas,Capital-Topeka,Code-94604",
                          "State-Maryland,Capital-Annapolis,Code-21403", "State-Montana,Capital-Helena,Code-111207",
                          "State-Nebraska,Capital-Lincoln,Code-68372", "State-Maine,Capital-Augusta,Code-94553",
                          "State-Washington,Capital-Olympia,Code-98506", "State-Texas,Capital-Austin,Code-78702", 
                          "State-Tennessee,Capital-Nashville,Code-37072")
    println
    println("(RAW RDD)")
    val rawRDD = sc.parallelize(nestedList)
    rawRDD.foreach(println)
    println
    
    //Separating States//
    val stateRDD = rawRDD.map(nested => nested.split(",")).map(split => split(0)) 
    //another way// val stateRDD = rawRDD.flatMap(nested => nested.split(",")).filter(split => split.contains("State"))
    
    //Separating Capitals//
    val capitalRDD = rawRDD.map(nested => nested.split(",")).map(split => split(1))
    //another way// val capitalRDD = rawRDD.flatMap(nested => nested.split(",")).filter(split => split.contains("Capital"))

    
    
    //State Names//
    val stateName = stateRDD.map(nestState => nestState.split("-")).map(name => name(1))
    //another way// val stateName = stateRDD.map(nestState => nestState.replace("State-",""))
    
    //Capital Names//
    val capitalName = capitalRDD.map(nestCapital => nestCapital.split("-")).map(name => name(1))
    //another way// val capitalName = capitalRDD.map(nestCapital => nestCapital.replace("Capital-",""))
    
    println("(STATE-CAPITALS)")
    val unionRDD = stateName.union(capitalName)
    unionRDD.foreach(println)
    //val u = unionRDD.collect.toList.map(_.toString)
    
    println
    
    //Extracting codes divisible by 2//
    val codes = rawRDD.map(x => x.split(",")).map(x => x(2)).map(x => x.split("-")).map(x => x(1))
    val codesInt = codes.collect.toList.map(_.toInt)
    
    
    println("(CODES DIVISIBLE BY 2)")
    val amountColumn = for (i <- codesInt) yield {
    if (i % 2 == 0) println(i)
  }
      
    println

    //////////DATAFRAME PROCESSING//////////
    
    val structVal = 
      StructType {
      StructField("txnNo",IntegerType,true) ::
      StructField("txndate",StringType,true) ::
      StructField("customerNo",IntegerType,true) ::
      StructField("amount",DoubleType,true) ::
      StructField("category",StringType,true) ::
      StructField("product",StringType,true) ::
      StructField("city",StringType,true) ::
      StructField("state",StringType,true) ::
      StructField("spendBy",StringType,true) :: Nil }
    
    try{
    val raw_DF = spark.read.format("csv").schema(structVal).load("C:/Users/Shwetalee Gadekar/Desktop/process_file/txs.csv")
    
    println("(RAW DATAFRAME)")
    raw_DF.show(false)
    
    raw_DF.printSchema()
    
    println("(RAW DATAFRAME COUNT)")
    println(raw_DF.count()) //----> 95904
    
    val txn_DF = raw_DF.filter($"txnNo">=10000 && $"txnDate">"07-01-2011" && $"amount">100.00)
    
    val txn_DF_2 = txn_DF.withColumn("customerNo", expr("substring(customerNo,4,4)"))
    
    val txn_DF_3 = txn_DF_2.filter($"category" like "%Sports%").orderBy("txnNo")
    
    val txn_DF_4 = txn_DF_3.withColumn("spendBy_binary",expr("case when spendBy='credit' then '1' else '0' end as spendBy"))
    
    val txn_DF_5 = txn_DF_4.withColumn("status",lit("fine"))
    
    
    println
    println("(PROCESSED DATAFRAME)")
    txn_DF_5.show(50,false)
    
    
    println("(PROCESSED DATAFRAME COUNT)")
    println(txn_DF_5.count()) //----> from 95904 records filtered down to 8234 required records
    
    val output = txn_DF_5.count().toInt
    
    println
    println(if (output > 5000) "Count of records are acceptable" else "Try a different approach")
    
    println
    println("FACTORIAL OF MAX VALUE OF AMOUNT")
    val txn_DF_6 = txn_DF_5.agg(max("amount").alias("maxAmount"))
    val roundValue = txn_DF_6.first().getDouble(0).round.toInt
    println(classInstance.factorial(roundValue))
    }
    catch {
    case ex: org.apache.spark.sql.AnalysisException => println(s"File not found"+ex)
    case ex1: java.io.IOException => println(s"Input path does not exist"+ex1)
    case _ :Throwable => println(s"Unknown exception")
    }
    
    println
    println(bkMap.get(101).getOrElse(""))
  }
}