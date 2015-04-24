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

package org.apache.spark.sql.hive.execution

import java.util.{Locale, TimeZone}

import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.util.Utils
import org.scalatest.BeforeAndAfter

/**
 * The test suite for window functions. To actually compare results with Hive,
 * every test should be created by `createQueryTest`. Because we are reusing tables
 * for different tests and there are a few properties needed to let Hive generate golden
 * files, every `createQueryTest` calls should explicitly set `reset` to `false`.
 */
class HiveWindowFunctionQuerySuite extends HiveComparisonTest with BeforeAndAfter {
  private val originalTimeZone = TimeZone.getDefault
  private val originalLocale = Locale.getDefault
  private val testTempDir = Utils.createTempDir()
  import org.apache.spark.sql.hive.test.TestHive.implicits._

  override def beforeAll() {
    TestHive.cacheTables = true
    // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
    // Add Locale setting
    Locale.setDefault(Locale.US)

    // Create the table used in windowing.q
    sql("DROP TABLE IF EXISTS part")
    sql(
      """
        |CREATE TABLE part(
        |  p_partkey INT,
        |  p_name STRING,
        |  p_mfgr STRING,
        |  p_brand STRING,
        |  p_type STRING,
        |  p_size INT,
        |  p_container STRING,
        |  p_retailprice DOUBLE,
        |  p_comment STRING)
      """.stripMargin)
    val testData = TestHive.getHiveFile("data/files/part_tiny.txt").getCanonicalPath
    sql(
      s"""
        |LOAD DATA LOCAL INPATH '$testData' overwrite into table part
      """.stripMargin)
    // The following settings are used for generating golden files with Hive.
    // We have to use kryo to correctly let Hive serialize plans with window functions.
    // This is used to generate golden files.
    sql("set hive.plan.serialization.format=kryo")
    // Explicitly set fs to local fs.
    sql(s"set fs.default.name=file://$testTempDir/")
    //sql(s"set mapred.working.dir=${testTempDir}")
    // Ask Hive to run jobs in-process as a single map and reduce task.
    sql("set mapred.job.tracker=local")
  }

  override def afterAll() {
    TestHive.cacheTables = false
    TimeZone.setDefault(originalTimeZone)
    Locale.setDefault(originalLocale)
    TestHive.reset()
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests from windowing.q
  /////////////////////////////////////////////////////////////////////////////
  createQueryTest("windowing.q -- 1. testWindowing",
    s"""
      |select p_mfgr, p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |sum(p_retailprice) over
      |(distribute by p_mfgr sort by p_name rows between unbounded preceding and current row) as s1
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 2. testGroupByWithPartitioning",
    s"""
      |select p_mfgr, p_name, p_size,
      |min(p_retailprice),
      |rank() over(distribute by p_mfgr sort by p_name)as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |p_size, p_size - lag(p_size,1,p_size) over(distribute by p_mfgr sort by p_name) as deltaSz
      |from part
      |group by p_mfgr, p_name, p_size
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 3. testGroupByHavingWithSWQ",
    s"""
      |select p_mfgr, p_name, p_size, min(p_retailprice),
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |p_size, p_size - lag(p_size,1,p_size) over(distribute by p_mfgr sort by p_name) as deltaSz
      |from part
      |group by p_mfgr, p_name, p_size
      |having p_size > 0
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 4. testCount",
    s"""
      |select p_mfgr, p_name,
      |count(p_size) over(distribute by p_mfgr sort by p_name) as cd
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 5. testCountWithWindowingUDAF",
    s"""
      |select p_mfgr, p_name,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |count(p_size) over(distribute by p_mfgr sort by p_name) as cd,
      |p_retailprice, sum(p_retailprice) over (distribute by p_mfgr sort by p_name
      |                                  rows between unbounded preceding and current row) as s1,
      |p_size, p_size - lag(p_size,1,p_size) over(distribute by p_mfgr sort by p_name) as deltaSz
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 6. testCountInSubQ",
    s"""
      |select sub1.r, sub1.dr, sub1.cd, sub1.s1, sub1.deltaSz
      |from (select p_mfgr, p_name,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |count(p_size) over(distribute by p_mfgr sort by p_name) as cd,
      |p_retailprice, sum(p_retailprice) over (distribute by p_mfgr sort by p_name
      |                                  rows between unbounded preceding and current row) as s1,
      |p_size, p_size - lag(p_size,1,p_size) over(distribute by p_mfgr sort by p_name) as deltaSz
      |from part
      |) sub1
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 8. testMixedCaseAlias",
    s"""
      |select p_mfgr, p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name, p_size desc) as R
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 9. testHavingWithWindowingNoGBY",
    s"""
      |select p_mfgr, p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |sum(p_retailprice) over (distribute by p_mfgr sort by p_name
      |                        rows between unbounded preceding and current row)  as s1
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 10. testHavingWithWindowingCondRankNoGBY",
    s"""
      |select p_mfgr, p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |sum(p_retailprice) over (distribute by p_mfgr sort by p_name
      |                        rows between unbounded preceding and current row) as s1
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 11. testFirstLast",
    s"""
      |select  p_mfgr,p_name, p_size,
      |sum(p_size) over (distribute by p_mfgr sort by p_name
      |rows between current row and current row) as s2,
      |first_value(p_size) over w1  as f,
      |last_value(p_size, false) over w1  as l
      |from part
      |window w1 as (distribute by p_mfgr sort by p_name rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 12. testFirstLastWithWhere",
    s"""
      |select  p_mfgr,p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |sum(p_size) over (distribute by p_mfgr sort by p_name
      |rows between current row and current row) as s2,
      |first_value(p_size) over w1 as f,
      |last_value(p_size, false) over w1 as l
      |from part
      |where p_mfgr = 'Manufacturer#3'
      |window w1 as (distribute by p_mfgr sort by p_name rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 13. testSumWindow",
    s"""
      |select  p_mfgr,p_name, p_size,
      |sum(p_size) over w1 as s1,
      |sum(p_size) over (distribute by p_mfgr  sort by p_name
      |rows between current row and current row)  as s2
      |from part
      |window w1 as (distribute by p_mfgr  sort by p_name rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 14. testNoSortClause",
    s"""
      |select  p_mfgr,p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr
      |from part
      |window w1 as (distribute by p_mfgr sort by p_name rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 15. testExpressions",
    s"""
      |select  p_mfgr,p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |cume_dist() over(distribute by p_mfgr sort by p_name) as cud,
      |percent_rank() over(distribute by p_mfgr sort by p_name) as pr,
      |ntile(3) over(distribute by p_mfgr sort by p_name) as nt,
      |count(p_size) over(distribute by p_mfgr sort by p_name) as ca,
      |avg(p_size) over(distribute by p_mfgr sort by p_name) as avg,
      |stddev(p_size) over(distribute by p_mfgr sort by p_name) as st,
      |first_value(p_size % 5) over(distribute by p_mfgr sort by p_name) as fv,
      |last_value(p_size) over(distribute by p_mfgr sort by p_name) as lv,
      |first_value(p_size) over w1  as fvW1
      |from part
      |window w1 as (distribute by p_mfgr sort by p_mfgr, p_name
      |             rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 16. testMultipleWindows",
    s"""
      |select  p_mfgr,p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |cume_dist() over(distribute by p_mfgr sort by p_name) as cud,
      |sum(p_size) over (distribute by p_mfgr sort by p_name
      |range between unbounded preceding and current row) as s1,
      |sum(p_size) over (distribute by p_mfgr sort by p_size
      |range between 5 preceding and current row) as s2,
      |first_value(p_size) over w1  as fv1
      |from part
      |window w1 as (distribute by p_mfgr sort by p_mfgr, p_name
      |             rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 17. testCountStar",
    s"""
      |select  p_mfgr,p_name, p_size,
      |count(*) over(distribute by p_mfgr sort by p_name ) as c,
      |count(p_size) over(distribute by p_mfgr sort by p_name) as ca,
      |first_value(p_size) over w1  as fvW1
      |from part
      |window w1 as (distribute by p_mfgr sort by p_mfgr, p_name
      |             rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 18. testUDAFs",
    s"""
      |select  p_mfgr,p_name, p_size,
      |sum(p_retailprice) over w1 as s,
      |min(p_retailprice) over w1 as mi,
      |max(p_retailprice) over w1 as ma,
      |avg(p_retailprice) over w1 as ag
      |from part
      |window w1 as (distribute by p_mfgr sort by p_mfgr, p_name
      |             rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 19. testUDAFsWithGBY",
    """
      |select  p_mfgr,p_name, p_size, p_retailprice,
      |sum(p_retailprice) over w1 as s,
      |min(p_retailprice) as mi ,
      |max(p_retailprice) as ma ,
      |avg(p_retailprice) over w1 as ag
      |from part
      |group by p_mfgr,p_name, p_size, p_retailprice
      |window w1 as (distribute by p_mfgr sort by p_mfgr, p_name
      |             rows between 2 preceding and 2 following);
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 20. testSTATs",
    """
      |select  p_mfgr,p_name, p_size,
      |stddev(p_retailprice) over w1 as sdev,
      |stddev_pop(p_retailprice) over w1 as sdev_pop,
      |collect_set(p_size) over w1 as uniq_size,
      |variance(p_retailprice) over w1 as var,
      |corr(p_size, p_retailprice) over w1 as cor,
      |covar_pop(p_size, p_retailprice) over w1 as covarp
      |from part
      |window w1 as (distribute by p_mfgr sort by p_mfgr, p_name
      |             rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 21. testDISTs",
    """
      |select  p_mfgr,p_name, p_size,
      |histogram_numeric(p_retailprice, 5) over w1 as hist,
      |percentile(p_partkey, 0.5) over w1 as per,
      |row_number() over(distribute by p_mfgr sort by p_name) as rn
      |from part
      |window w1 as (distribute by p_mfgr sort by p_mfgr, p_name
      |             rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 24. testLateralViews",
    """
      |select p_mfgr, p_name,
      |lv_col, p_size, sum(p_size) over w1   as s
      |from (select p_mfgr, p_name, p_size, array(1,2,3) arr from part) p
      |lateral view explode(arr) part_lv as lv_col
      |window w1 as (distribute by p_mfgr sort by p_size, lv_col
      |             rows between 2 preceding and current row)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 26. testGroupByHavingWithSWQAndAlias",
    """
      |select p_mfgr, p_name, p_size, min(p_retailprice) as mi,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |p_size, p_size - lag(p_size,1,p_size) over(distribute by p_mfgr sort by p_name) as deltaSz
      |from part
      |group by p_mfgr, p_name, p_size
      |having p_size > 0
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 27. testMultipleRangeWindows",
    """
      |select  p_mfgr,p_name, p_size,
      |sum(p_size) over (distribute by p_mfgr sort by p_size
      |range between 10 preceding and current row) as s2,
      |sum(p_size) over (distribute by p_mfgr sort by p_size
      |range between current row and 10 following )  as s1
      |from part
      |window w1 as (rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 28. testPartOrderInUDAFInvoke",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_size) over (partition by p_mfgr  order by p_name
      |rows between 2 preceding and 2 following) as s
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 29. testPartOrderInWdwDef",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_size) over w1 as s
      |from part
      |window w1 as (partition by p_mfgr  order by p_name
      |             rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 30. testDefaultPartitioningSpecRules",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_size) over w1 as s,
      |sum(p_size) over w2 as s2
      |from part
      |window w1 as (distribute by p_mfgr sort by p_name rows between 2 preceding and 2 following),
      |       w2 as (partition by p_mfgr order by p_name)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 31. testWindowCrossReference",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_size) over w1 as s1,
      |sum(p_size) over w2 as s2
      |from part
      |window w1 as (partition by p_mfgr order by p_name
      |             range between 2 preceding and 2 following),
      |       w2 as w1
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 32. testWindowInheritance",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_size) over w1 as s1,
      |sum(p_size) over w2 as s2
      |from part
      |window w1 as (partition by p_mfgr order by p_name
      |             range between 2 preceding and 2 following),
      |       w2 as (w1 rows between unbounded preceding and current row)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 33. testWindowForwardReference",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_size) over w1 as s1,
      |sum(p_size) over w2 as s2,
      |sum(p_size) over w3 as s3
      |from part
      |window w1 as (distribute by p_mfgr sort by p_name
      |             range between 2 preceding and 2 following),
      |       w2 as w3,
      |       w3 as (distribute by p_mfgr sort by p_name
      |             range between unbounded preceding and current row)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 34. testWindowDefinitionPropagation",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_size) over w1 as s1,
      |sum(p_size) over w2 as s2,
      |sum(p_size) over (w3 rows between 2 preceding and 2 following)  as s3
      |from part
      |window w1 as (distribute by p_mfgr sort by p_name
      |             range between 2 preceding and 2 following),
      |       w2 as w3,
      |       w3 as (distribute by p_mfgr sort by p_name
      |             range between unbounded preceding and current row)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 35. testDistinctWithWindowing",
    """
      |select DISTINCT p_mfgr, p_name, p_size,
      |sum(p_size) over w1 as s
      |from part
      |window w1 as (distribute by p_mfgr sort by p_name rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 36. testRankWithPartitioning",
    """
      |select p_mfgr, p_name, p_size,
      |rank() over (partition by p_mfgr order by p_name )  as r
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 37. testPartitioningVariousForms",
    """
      |select p_mfgr,
      |round(sum(p_retailprice) over (partition by p_mfgr order by p_mfgr),2) as s1,
      |min(p_retailprice) over (partition by p_mfgr) as s2,
      |max(p_retailprice) over (distribute by p_mfgr sort by p_mfgr) as s3,
      |round(avg(p_retailprice) over (distribute by p_mfgr),2) as s4,
      |count(p_retailprice) over (cluster by p_mfgr ) as s5
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 38. testPartitioningVariousForms2",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_retailprice) over (partition by p_mfgr, p_name order by p_mfgr, p_name
      |rows between unbounded preceding and current row) as s1,
      |min(p_retailprice) over (distribute by p_mfgr, p_name sort by p_mfgr, p_name
      |rows between unbounded preceding and current row) as s2,
      |max(p_retailprice) over (partition by p_mfgr, p_name order by p_name) as s3
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 39. testUDFOnOrderCols",
    """
      |select p_mfgr, p_type, substr(p_type, 2) as short_ptype,
      |rank() over (partition by p_mfgr order by substr(p_type, 2))  as r
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 40. testNoBetweenForRows",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_retailprice) over (distribute by p_mfgr sort by p_name rows unbounded preceding) as s1
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 41. testNoBetweenForRange",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_retailprice) over (distribute by p_mfgr sort by p_size range unbounded preceding) as s1
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 42. testUnboundedFollowingForRows",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_retailprice) over (distribute by p_mfgr sort by p_name
      |rows between current row and unbounded following) as s1
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 43. testUnboundedFollowingForRange",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_retailprice) over (distribute by p_mfgr sort by p_size
      |range between current row and unbounded following) as s1
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 44. testOverNoPartitionSingleAggregate",
    """
      |select p_name, p_retailprice,
      |round(avg(p_retailprice) over(),2)
      |from part
      |order by p_name
    """.stripMargin, reset = false)
}
