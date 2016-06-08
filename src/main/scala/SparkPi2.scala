/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark._

import scala.math.random
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructField, StructType,StringType}

/** Computes an approximation to pi */
object SparkPi2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi2").setMaster(args(0))
    val spark = new SparkContext(conf)

    //
    val sqlContext = new SQLContext(spark)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("Addresses.csv")

    df.printSchema()

    df.registerTempTable("Table1")

    var df2 = sqlContext.sql("select pao from Table1")
      .map(x => x.toString().toLowerCase()).map(p=>Row(p))

    import org.apache.spark.sql.types.{StructField, StructType,StringType}

    var schema = StructType("Address".split(",")
      .map(fieldName => StructField(fieldName, StringType, true)
    ))

    val aDataFrame = sqlContext.createDataFrame(df2, schema)

    aDataFrame.registerTempTable("Table2")

    aDataFrame.map(r=>println(r))
    //aDataFrame.printSchema()
  }
}
// scalastyle:on println
