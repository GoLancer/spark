package org.apache.spark.sql.json

import org.apache.spark.sql.test.TestSQLContext

object TestJsonData {
  val jsonRecord1 =
    """{"a1":1, "a2":"2", "a3":{"b1":3.0, "b2":{"c1":null, "c2": false, """ +
      """"c3": [123, "c3string1", null, true, {"d1":"d1string1"}]}}, "a4":true}"""

  val jsonTextData =
    TestSQLContext.sparkContext.parallelize(
      """{"a":11, "b":12, "c":13}""" ::
      """{"b":22, "c":23, "d":24}""" ::
      """{"c":33, "e":{"e1":351, "e2":352}}""" ::
      """{"a":41, "b":42, "e":{"e2":452}}""" :: Nil)
}
