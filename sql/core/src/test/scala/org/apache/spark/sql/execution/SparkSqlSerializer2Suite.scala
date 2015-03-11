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

package org.apache.spark.serializer

import org.apache.spark.sql.{Row, QueryTest}

import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.test.TestSQLContext.implicits._

class SparkSqlSerializer2Suite extends QueryTest {

  test("simple shuffle") {
    val df =
      (1 to 10)
        .map(i => (i, i.toDouble, i.toLong, i.toString))
        .toDF("intCol", "doubleCol", "longCol", "stringCol")

    sql("SET spark.sql.useSerializer2=true")

    checkAnswer(
      df.orderBy("intCol"),
      df.collect().toSeq
    )

    checkAnswer(
      df.groupBy("stringCol").sum(),
      (1 to 10).map(i => Row(i.toString, i, i.toDouble, i.toLong))
    )

    sql("SET spark.sql.useSerializer2=false")
  }

  /*
  test("simple serializer2 v2") {
    val df =
      jsonRDD(sparkContext.parallelize((1 to 10).map(i =>
        s"""
           |{"intCol":${i},
           |"doubleCol":${i.toDouble},
           |"longCol":${i.toLong},
           |"stringCol1":"${i.toString}",
           |"stringCol2":"${i.toString}"}""".stripMargin)))

    df.registerTempTable("serializer2v2")

    sql("SET spark.sql.useSerializer2=true")
    sql("SET spark.sql.useSerializer2.version=1")

    sql("SELECT stringCol1, sum(doubleCol) FROM serializer2v2 GROUP BY stringCol1").explain()

    checkAnswer(
      sql("SELECT stringCol1, sum(doubleCol) FROM serializer2v2 GROUP BY stringCol1"),
      (1 to 10).map(i => Row(i.toString, i.toDouble))
    )

    sql(
      """
        |SELECT x.stringCol2, avg(y.intCol), sum(x.doubleCol)
        |FROM serializer2v2 x JOIN serializer2v2 y ON (x.stringCol1 = y.stringCol1)
        |GROUP BY x.stringCol2
      """.stripMargin).explain()

    checkAnswer(
      sql(
        """
          |SELECT x.stringCol2, avg(y.intCol), sum(x.doubleCol)
          |FROM serializer2v2 x JOIN serializer2v2 y ON (x.stringCol1 = y.stringCol1)
          |GROUP BY x.stringCol2
        """.stripMargin),
      (1 to 10).map(i => Row(i.toString, i.toDouble, i.toDouble))
    )

    df.orderBy("stringCol1", "doubleCol", "doubleCol").explain()

    checkAnswer(
      df.orderBy("stringCol1", "doubleCol", "doubleCol"),
      df.collect().toSeq
    )

    sql("SET spark.sql.useSerializer2=false")
    sql("SET spark.sql.useSerializer2.version=1")
  }
  */

}