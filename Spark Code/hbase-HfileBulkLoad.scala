import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, KeyValueUtil, TableName, HTableDescriptor,HColumnDescriptor}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.client.{HTable,ConnectionFactory}
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2,LoadIncrementalHFiles}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.permission.FsPermission

case class Person(key: Integer, name: String, age: Integer, gender: String, occupation: String)
case class SaltedDF(salted: String, colQualifier: String, value: String)

object SaltedKeyExample extends App {

  val spark = SparkSession
    .builder()
    .appName("Salted Key Example")
    .getOrCreate()
  import spark.implicits._
  val hconf = HBaseConfiguration.create()
  
  def tostring(x: Any) : String = if(x!=null) x.toString  else ""
  
  def digitsRequired(modulus: Int): Int = {
    (Math.log10(modulus - 1) + 1).asInstanceOf[Int]
  }

  def salt(modulus: Int)(key: String): String = {
    val saltAsInt = Math.abs(key.hashCode) % modulus

    // left pad with 0's (for readability of keys)
    val charsInSalt = digitsRequired(modulus)
    ("%0" + charsInSalt + "d").format(saltAsInt) + ":" + key
  }
  
  ///////////////////////  Create Dummy Dataframe   ///////////////////////////
  val tableName = TableName.valueOf( "tablesalted")
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
  val col_list = df.columns.toList
  val partition_no = 2
  val id_col = 1
  // val salterudf = udf(salt(partition_no) _)
  val salting = salt(2) _
  var salted_df = df.rdd.map(x => (x.get(id_col-1).toString, (col_list zip x.toSeq.toList.map(y=>tostring(y)))))
                        .flatMapValues(x => x)
                        .map(x => (salting(x._1), x._2._1, x._2._2))
                        .toDF("salted","colQualifier","value").filter('value =!= "")
  salted_df = salted_df.withColumn("partition", split($"salted", ":").getItem(0))
  val sorted_df = salted_df.orderBy("salted","colQualifier","value").repartition(partition_no, $"partition")
  sorted_df.show()
  /*
  +------+------------+---------+---------+
  |salted|colQualifier|    value|partition|
  +------+------------+---------+---------+
  |   1:1|         age|       32|        1|
  |   1:1|      gender|     Male|        1|
  |   1:1|         key|        1|        1|
  |   1:1|        name|     Andy|        1|
  |   1:1|  occupation|  Teacher|        1|
  |   1:3|         age|       20|        1|
  |   1:3|      gender|   Female|        1|
  |   1:3|         key|        3|        1|
  |   1:3|        name|     Jane|        1|
  |   1:3|  occupation|  Pianist|        1|
  |   0:2|         age|       16|        0|
  |   0:2|      gender|     Male|        0|
  |   0:2|         key|        2|        0|
  |   0:2|        name|    Harry|        0|
  |   0:2|  occupation|  Student|        0|
  |   0:4|         age|       40|        0|
  |   0:4|      gender|   Female|        0|
  |   0:4|         key|        4|        0|
  |   0:4|        name|   Janice|        0|
  |   0:4|  occupation|Housewife|        0|
  +------+------------+---------+---------+
  */
  val saltedRDD = sorted_df.rdd
  // to see respective partitioned rdd: sorted_df.rdd.glom().collect()(0) for 1st partition

  val cells = saltedRDD.map(r => {
    val saltedRowKey = Bytes.toBytes(r.getString(0))
    val cell = new KeyValue(
        saltedRowKey,
        Bytes.toBytes("cf"), 
        Bytes.toBytes(r.getString(1)), 
        Bytes.toBytes(r.getString(2))
    )
    //(KeyValueUtil.length(cell), r)
    (new ImmutableBytesWritable(saltedRowKey), cell)
    })

  val filePath: String = "/user/oracle/test/hfiles"

  cells.saveAsNewAPIHadoopFile(
    filePath,
    classOf[ImmutableBytesWritable],
    classOf[KeyValue],
    classOf[HFileOutputFormat2],
    hconf)
  
  val hfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  hfs.setPermission(new Path(filePath+"/cf"),FsPermission.valueOf("drwxrwxrwx"))

  /////////////////////// Create Table if not exist //////////////////////////

  val conn = ConnectionFactory.createConnection(hconf)
  val admin = conn.getAdmin()
  
  if(!admin.isTableAvailable(tableName)){
    println("Create Table")
    val tableDesc = new HTableDescriptor(tableName)
    tableDesc.addFamily(new HColumnDescriptor("cf"))
    admin.createTable(tableDesc)
  } else {println("Table alread exists")}

  /////////////////// Bulk Load /////////////////////

  val locator = conn.getRegionLocator(tableName)
  val table = conn.getTable(tableName)
  val loader = new LoadIncrementalHFiles(hconf)
  val loadPath =  new Path(filePath)
  loader.doBulkLoad(loadPath, admin, table, locator)
}

