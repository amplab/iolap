name := "bootstrap-sql"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.1.0"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

initialCommands in console :=
  s"""
    |import org.apache.spark._
    |import org.apache.spark.sql.catalyst.analysis._
    |import org.apache.spark.sql.catalyst.dsl._
    |import org.apache.spark.sql.catalyst.errors._
    |import org.apache.spark.sql.catalyst.expressions._
    |import org.apache.spark.sql.catalyst.plans.logical._
    |import org.apache.spark.sql.catalyst.rules._
    |import org.apache.spark.sql.catalyst.types._
    |import org.apache.spark.sql.catalyst.util._
    |import org.apache.spark.sql.execution
    |import org.apache.spark.sql.parquet.ParquetTestData
    |
    |val conf = new SparkConf()
    |             .setMaster("local")
    |             .setAppName("bootstrap-sql")
    |             .set("spark.executor.memory", "1g")
    |val sc = new SparkContext(conf)
    |val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    |
    |import sqlContext.createSchemaRDD
    |
    |""".stripMargin

scalacOptions += "-deprecation"
