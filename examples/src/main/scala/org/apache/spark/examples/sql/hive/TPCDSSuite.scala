package org.apache.spark.examples.sql.hive

import java.io.File

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.sql._
import org.apache.spark.sql.hive.online.OnlineSQLConf._
import org.apache.spark.sql.hive.online.RandomSeed

import scala.sys.process.Process

/**
 * Usage:
 *
 * 1. Use "create-hdfs [scale]" to generate a TPCDS dataset of the given scale
 * and put it in HDFS
 *
 * 2. Use "create [hdfs-tpcds-dir]" to create external hive tables using the
 * pre-loaded TPDCS dataset.
 *
 * 3. Use "parquet [hdfs-tpcds-dir parquet-output-dir bytes-per-file]" to
 * generate preprocessed parquet dataset where each file is of the given size.
 *
 * This is required for now, as:
 *
 * (1) SparkSQL can only collect statistics for parquet relations,
 * and statistics are very important for join optimization.
 *
 * (2) Preprocessing will insert a "__seed__" column for bootstrap,
 * which is required for a very important optimization and hard-coded in the planner.
 * (TODO: Make this optional)
 *
 * 4. "drop" the previously created external tables.
 *
 * 5. Use "create-parquet [parquet-output-dir]" to create external hive tables
 * using the preprocessed parquet data.
 */
object TPCDSSuite extends {
  val appName = "TPCDS Suite"
  val suiteName = "tpcds"
} with SuiteHarness {

  val numExecutors = sparkConf.get("spark.number.executors").toInt

  val hadoopCmd = "/root/persistent-hdfs/bin/hadoop"

  // Create the hadoop filesystem
  val conf = new Configuration()
  conf.set("fs.default.name", hdfsUrl)
  val hadoop_fs = FileSystem.get(conf)

  override def dispatch(args: Array[String]): Unit = args(0) match {
    case GENERATE_CMD => generateHDFS(args(1))

    case PARQUET_CMD => parquet(args(1), args(2), args(3).toInt)
  }

  def generateHDFS(scale: String): Unit = {
    // partition_size * 8 <= 100 GB ==> num_partitions <= scale/12.5
    val numParts = math.max(math.ceil(scale.toInt/12.5).toInt, 8 * numExecutors)
    val toolsDir = "/root/tpcds-kit/tools"
    sparkContext.parallelize(1 to numParts, numParts).foreach { i =>
      // Create the TPCDS destination dir
      val localDir = s"/mnt/tpcds-$scale-$i"
      Process(s"mkdir -p $localDir").!

      val dgenCmd = s"./dsdgen -dir $localDir -scale $scale -parallel $numParts -child $i"
      Process(dgenCmd, new File(toolsDir)).!

      val hdfsDir = s"/tpcds-$scale"
      new File(localDir).listFiles.foreach { datFile =>
        val fileName = datFile.getName
        val table = fileName.substring(0, fileName.indexOf(s"_${i}_$numParts.dat"))
        val dstPath = new Path(s"$hdfsDir/$table/$fileName")
        val srcPath = new Path(datFile.getAbsolutePath)

        val conf = new Configuration()
        conf.set("fs.default.name", hdfsUrl)
        val fs = FileSystem.get(conf)

        if (!fs.exists(dstPath)) {
          fs.copyFromLocalFile(
            true, /* delSrc */
            false, /* overwrite */
            srcPath, dstPath
          )
        }
      }

      Process(s"rm -r -f $localDir").!
    }
  }

  //  size of catalog_sales = 13006781071 bytes
  //  size of customer_address = 15133412 bytes
  //  size of store_returns = 1488378487 bytes
  //  size of web_sales = 6538306516 bytes
  //  size of item = 14746870 bytes
  //  size of store_sales = 16796276339 bytes
  //  size of web_returns = 467741353 bytes
  //  size of store = 33333 bytes
  //  size of customer = 80539100 bytes
  //  size of date_dim = 1052084 bytes
  //  size of catalog_returns = 987422351 bytes

  def parquet(hdfsDir: String, parquet: String, partitionBytes: Int): Unit =
    tables.keys.foreach { table =>
      val fileSize = hadoop_fs.getContentSummary(new Path(s"$hdfsUrl/$hdfsDir/$table")).getLength
      // Converting to Parquet will compress the data by about compressFactor,
      // so we divide the number of partitions appropriately
      val compressFactor = 2.6
      val numPartitions = scala.math.ceil(
        fileSize.toDouble / partitionBytes.toDouble / compressFactor).toInt
      println(s"################### $table of size $fileSize gets $numPartitions partitions")
      sqlContext.table(table).withColumn(SEED_COLUMN, new Column(RandomSeed()))
        .repartition(numPartitions)
        .write.format("parquet").save(s"$hdfsUrl/$parquet/$table")
    }

  // store_returns of size 3483867155 gets 80 partitions
  // catalog_sales of size 31016462258 gets 712 partitions
  // web_sales of size 15463513086 gets 355 partitions
  // store of size 106820 gets 1 partitions
  // customer_address of size 111154196 gets 3 partitions
  // customer of size 269515941 gets 7 partitions
  // catalog_returns of size 2279225314 gets 53 partitions
  // web_returns of size 1053529104 gets 25 partitions
  // store_sales of size 40959624908 gets 939 partitions
  // date_dim of size 10317438 gets 1 partitions
  // item of size 58366791 gets 2 partitions
  // customer_demographics of size 80660096 gets 2 partitions

  val GENERATE_CMD = "gen"
  val PARQUET_CMD = "parquet"
}
