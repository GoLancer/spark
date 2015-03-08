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
  }
}