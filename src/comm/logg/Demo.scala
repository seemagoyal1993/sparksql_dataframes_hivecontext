package comm.logg
import org.apache.spark.SparkContext 
import org.apache.spark.sql.hive.orc._
import org.apache.spark.SparkContext._ 
import org.apache.spark._ 
//Represents one row of output from a relational operator.
import org.apache.spark.sql.Row
//SQLContext :The entry point for working with structured data (rows and columns) in Spark. Allows the creation of DataFrame objects as well as the execution of SQL queries
//Dataframe: A distributed collection of data organized into named columns.
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
//For a StructType object, one or multiple StructFields can be extracted by names
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrameWriter
import org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler

/**
 * @author seema
 */
object Demo {
  //val sc = SparkCommon.sparkContext
  
  /*
Spark properties can be set directly on a SparkConf passed to your 
SparkContext. SparkConf allows you to configure some of the common 
properties (e.g. master URL and application name), as well as arbitrary 
key-value pairs through the set() method. For example, we 
could initialize an application with two threads as follows:Note that 
we run with local[2], meaning two threads - which represents “minimal” 
parallelism, which can help detect bugs that only exist when we run in
 a distributed context.
   * 
   * 
   * 
   */
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g");
  
 //We pass the SparkContext constructor a SparkConf object which contains information about our application.
val sc = new SparkContext(conf)
    //val schemaOptions = Map("header" -> "true", "inferSchema" -> "true")
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)


 def main(args:Array[String]){
   import sqlContext.implicits._
    //val logg = sc.textFile("file:////home/cloudera/Desktop/i.txt")
   println("enter path of file that has to be read")
   val logg = sc.textFile(args(0))
 
 // The schema is encoded in a string
 val schemaString = "start_date session_id " //sample schema string wd 20 headers
 // Generate the schema based on the string of schema
 val schema =StructType(
 schemaString.split(" ").map(fieldName=>StructField(fieldName, StringType, true)))
 
 schema.foreach(println)
 // Convert records of the RDD (fruit) to Rows.
 val rowRDD = logg.map(_.split("\\{\\}")).filter(_.length== 20).map(p => Row(p(0).substring(0,10), p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11),p(12),p(13),p(14),p(15),p(16),p(17),p(18),p(19)))
 rowRDD.foreach(println)
 
 // Apply the schema to the RDD.
 val loggDataFrame = sqlContext.createDataFrame(rowRDD, schema)
 
 sqlContext.setConf("hive.exec.dynamic.partition", "true")
sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
sqlContext.setConf("hive.enforce.bucketing", "true")
sqlContext.setConf("hive.exec.max.dynamic.partitions","2500")
sqlContext.setConf("hive.mapred.mode","nonstrict") 
 // Register the DataFrames as a table.
 loggDataFrame.registerTempTable("7logg")
 //loggDataFrame.saveAsTable("7logg")
 val t=sqlContext.sql("SELECT * FROM 7logg")
 
 
 
 //loggDataFrame.saveAsTable("5logg");
//loggDataFrame.toDF.select("gtp_start_date"," gtp_session_id","t_session_id","event_tm","request_id","uid","host_name","uri_path"," uri_params"," referer_scheme"," referer_hostname"," referer_uri_path"," referer_uri_params"," user_agent content_type"," content_length ","response_code ","http_method ","http_body","cookie").write.mode("append").partitionBy(gtp_start_date).saveAsTable("5logg"); //internal table with partitioning
 sqlContext.sql("CREATE TABLE IF NOT EXISTS MAIN1(gtp_session_id STRING,t_session_id STRING,event_tm STRING,request_id STRING,uid STRING,host_name STRING,uri_path STRING,uri_params STRING,referer_scheme STRING,referer_hostname STRING,referer_uri_path STRING,referer_uri_params STRING,user_agent STRING, content_type STRING, content_length STRING, response_code STRING,http_method STRING, http_body STRING, cookie STRING) PARTITIONED BY(gtp_start_date STRING) row format delimited fields terminated by '\n'")
t.write.partitionBy("gtp_start_date").insertInto("MAIN1")
//loggDataFrame.write.format("orc").mode("overwrite").partitionBy("gtp_start_date").orc("/user/hive/warehouse")
//sqlContext.sql("FROM 7logg l1 INSERT OVERWRITE TABLE MAIN1 PARTITION(gtp_start_date) SELECT l1.*")
val t1=sqlContext.sql("CREATE VIEW  V1 AS SELECT * FROM MAIN1 where gtp_start_date='30.08.2016 09:25:10' OR gtp_start_date='30.08.2016'")
val t11=sqlContext.sql("select * from V1")

/*val t2=sqlContext.sql("CREATE INDEX I1 ON TABLE 7logg(gtp_start_date) AS 'COMPACT' WITH DEFERRED REBUILD")
t2.write.format("orcl")text("/user/cloudera")*/
/**
 * SQL statements can be run by using the sql methods provided by sqlContext.
 */
 /*val results = sqlContext.sql("CREATE TABLE IF NOT EXISTS 4logg(gtp_start_date STRING,gtp_session_id STRING,t_session_id STRING,event_tm STRING,request_id STRING,uid STRING,host_name STRING,uri_path STRING,uri_params STRING,referer_scheme STRING,referer_hostname STRING,referer_uri_path STRING,referer_uri_params STRING,user_agent STRING,content_type STRING,content_length STRING,response_code STRING,http_method STRING,http_body STRING,cookie STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' LOCATION '/user/cloudera/' AS SELECT * FROM 4logg")
 results.show()*/
        
     System.out.println("OK");
     
  //external table with partitioning
     

sqlContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS DEMO1(gtp_session_id STRING,t_session_id STRING,event_tm STRING,request_id STRING,uid STRING,host_name STRING,uri_path STRING,uri_params STRING,referer_scheme STRING,referer_hostname STRING,referer_uri_path STRING,referer_uri_params STRING,user_agent STRING, content_type STRING, content_length STRING, response_code STRING,http_method STRING, http_body STRING, cookie STRING) PARTITIONED BY(gtp_start_date STRING) CLUSTERED BY (http_method) into 4 buckets row format delimited fields terminated by '\n' LOCATION '/user/cloudera/ext_tables' ")
val n=sqlContext.sql("SELECT * FROM MAIN1")
n.write.partitionBy("start_date").insertInto("DEMO1")
 //bucketing : Tables/Partitions can be further subdivided into Clusters or Buckets.
//Data in each partition may in turn be divided into Buckets based on the value of a hash function of some column of the Table.
  
  
}


 
}
