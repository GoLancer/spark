package org.apache.spark.sql.json

import org.apache.spark.sql.test.TestSQLContext

object TestJsonData {
  val jsonRecord1 =
    """{"a1":1, "a2":"2", "a3":{"b1":3.0, "b2":{"c1":null, "c2": false, """ +
      """"c3": [123, "c3string1", null, true, {"d1":"d1string1"}]}}, "a4":true}"""

  val jsonRecord2 =
    """{"a1":1, "a2":[1, "str", true], "a3":[{"b1":3.0}, {"b1":4.0}, {"b2": 5.0}]}""""


  val jsonTextData1 =
    TestSQLContext.sparkContext.parallelize(
      """{"a":11, "b":12, "c":13}""" ::
      """{"b":22, "c":23, "d":24}""" ::
      """{"c":33, "e":{"e1":351, "e2":352}}""" ::
      """{"a":41, "b":42, "e":{"e2":452}}""" :: Nil)

  val jsonTextData2 =
    TestSQLContext.sparkContext.parallelize(
      jsonRecord2 :: Nil)

  /**
   * Handling array type correctly.
   * For an array field, if both empty arrays and struct arrays exist,
   * we should choose struct arrays.
   * For an array field, if both empty arrays and primitive arrays exist,
   * we should choose primitive arrays.
   * For an array field, if both struct arrays and primitive arrays exist,
   * we do not handle this case
   */
  val jsonTextData3 =
    TestSQLContext.sparkContext.parallelize(
      """{"a1":{"b1":[1, "str", true], "b2":[{"c1":3.0}, {"c1":4.0}, {"c2": 5.0}]}}"""" ::
      """{"a1":{"b1":[], "b2":[]}}"""" ::
      """{"a1":{"b1":[], "b2":null}}"""" :: Nil)
}
