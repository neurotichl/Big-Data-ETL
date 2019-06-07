import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._ //Row
import org.apache.hadoop.hbase.{HBaseConfiguration,TableName,HTableDescriptor,HColumnDescriptor} 
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.{ConnectionFactory,Put, Result} 
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.mapreduce.Job

case class Person(key: Integer, name: String, age: Integer, gender: String, occupation: String)

object Main extends App {

  ///////////////////////  Create Dummy Dataframe   ///////////////////////////
  
  val hconf = HBaseConfiguration.create()
  val tablename = TableName.valueOf("table123")

  val spark = SparkSession.builder().appName("Test Insert Hbase").config("spark.sql.warehouse.dir","/user/hive/warehouse").enableHiveSupport().getOrCreate()
  import spark.implicits._

  val df = Seq(Person(1, "Andy", 32 ,"Male","Teacher"), 
               Person(2, "Harry",16,"Male","Student"),
               Person(3, "Jane", 20, "Female","Pianist"),
               Person(4, "Janice", 40 ,"Female","Housewife")).toDF()
  df.show()
  /*
  +---+------+---+------+----------+
  |key|  name|age|gender|occupation|
  +---+------+---+------+----------+
  |  1|  Andy| 32|  Male|   Teacher|
  |  2| Harry| 16|  Male|   Student|
  |  3|  Jane| 20|Female|   Pianist|
  |  4|Janice| 40|Female| Housewife|
  +---+------+---+------+----------+
  */
  /////////////////////// Create Table if not exist //////////////////////////
  
  val conn = ConnectionFactory.createConnection(hconf)
  val admin = conn.getAdmin()
  
  if(!admin.isTableAvailable(tablename)){
    println("Create Table")
    val tableDesc = new HTableDescriptor(tablename)
    tableDesc.addFamily(new HColumnDescriptor("cf"))
    admin.createTable(tableDesc)
  } else {println("Table alread exists")}

  //////////////////////// Insert Table into HBase //////////////////////////
  
  val colnames: Array[String]  = df.columns
  val hbasePuts= df.rdd.map((row: Row) => {
    val  put = new Put(row.get(0).toString.getBytes())
    put.addColumn("cf".getBytes(), colnames(1).getBytes(), row.getString(1).getBytes())
    put.addColumn("cf".getBytes(), colnames(2).getBytes(), Bytes.toBytes(row.getInt(2)))
    put.addColumn("cf".getBytes(), colnames(3).getBytes(), row.getString(3).getBytes())
    put.addColumn("cf".getBytes(), colnames(4).getBytes(), row.getString(4).getBytes())      
    (new ImmutableBytesWritable(), put)
  })

  val job = Job.getInstance(hconf)
  job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
  job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tablename.getNameAsString())
  
  hbasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration())

  //////////////////////// Read Table from HBase ///////////////////////////

  hconf.set(TableInputFormat.INPUT_TABLE, tablename.getNameAsString())
  var rdd = spark.sparkContext.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
  rdd.count()
  rdd.map{case(k,v) => k.get}.collect()
  rdd.map{case(k,v) => (v.getValue("cf".getBytes(),"name".getBytes()),
                        v.getValue("cf".getBytes(), "age".getBytes()))}
     .map(x => (Bytes.toString(x._1), Bytes.toInt(x._2))).toDF("name","age").show()

  
}
