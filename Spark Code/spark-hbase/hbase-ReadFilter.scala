import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.{HBaseConfiguration,TableName,CellUtil}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat,TableMapReduceUtil}
import org.apache.hadoop.hbase.client.{Result,ConnectionFactory,Scan}
import org.apache.hadoop.hbase.filter.{SingleColumnValueFilter,CompareFilter,BinaryPrefixComparator}
import org.apache.hadoop.hbase.util.{Bytes,Base64}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import scala.collection.JavaConverters._

object Main extends App {
  
  val spark = SparkSession.builder().appName("Read HBase").getOrCreate()
  import spark.implicits._

  def convertScanToString(scan : Scan) = {
      //https://stackoverflow.com/questions/30765026/spark-and-hbase-snapshots
      val proto = ProtobufUtil.toScan(scan);
      Base64.encodeBytes(proto.toByteArray());
    }

  /////////////////// Set Configuration /////////////////////
  // Read Table https://stackoverflow.com/questions/27122409/using-spark-to-read-specific-columns-data-from-hbase
  @transient val hconf = HBaseConfiguration.create()
  hconf.set("hbase.zookeeper.quorum", "bigdatalite")
  hconf.set("hbase.zookeeper.port","2181")
  hconf.set(TableInputFormat.INPUT_TABLE, "table123")
  hconf.set(TableInputFormat.SCAN_COLUMNS, "cf:name cf:occupation") 

  /////////////////// List available table /////////////////////
  val connection = ConnectionFactory.createConnection(hconf)
  val admin = connection.getAdmin()
  val listtables = admin.listTables()
  listtables.foreach(println)
  connection.close()

 /*
 'table123', {NAME => 'cf', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}
 'tablesalted', {NAME => 'cf', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}
 */

  /////////////////// Read /////////////////////
  var col_rdd = spark.sparkContext.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

  col_rdd.map{case(k,v) => k.get}.toDF().show()
  /*
  +-----+
  |value|
  +-----+
  | [31]|
  | [32]|
  +-----+
  */
  col_rdd.map{case(k,v) => 
    v.listCells().asScala.toList.map{cell=>
      val family = CellUtil.cloneFamily(cell)
      val column = CellUtil.cloneQualifier(cell)
      val value = CellUtil.cloneValue(cell)
      (Bytes.toString(column), Bytes.toString(value))
    }
  }.flatMap(x=>x).toDF("column","value").show

  /*
  +----------+--------+
  |    column|   value|
  +----------+--------+
  |      name|    Andy|
  |occupation| Teacher|
  |      name|   Harry|
  |occupation|Engineer|
  +----------+--------+
  */
  
  val scan = new Scan()
  val TableFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator("An".getBytes()))
  scan.setFilter(TableFilter)
  hconf.set(TableInputFormat.SCAN_COLUMNS, "cf:name cf:occupation cf:age") 
  hconf.set(TableInputFormat.SCAN, convertScanToString(scan))
  val prefix_filter_rdd = spark.sparkContext.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
  
  prefix_filter_rdd.map{case(k,v) => 
    val name = v.getValue("cf".getBytes(), "name".getBytes())
    val occupation = v.getValue("cf".getBytes(), "occupation".getBytes())
    val age = v.getValue("cf".getBytes(), "age".getBytes())

    (Bytes.toString(name), Bytes.toString(occupation), Bytes.toInt(age))
  }.toDF("name","occupation","age").show

  /*
  +----+----------+---+
  |name|occupation|age|
  +----+----------+---+
  |Andy|   Teacher| 32|
  +----+----------+---+
  */
  
  // val table = connection.getTable(TableName.valueOf("table123"))
}


