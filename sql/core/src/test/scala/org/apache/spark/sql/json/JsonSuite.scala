package org.apache.spark.sql.json

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.json.JsonTable._
import org.apache.spark.sql.test.TestSQLContext._
import com.fasterxml.jackson.core.JsonFactory

class JsonSuite extends QueryTest {
  import TestJsonData._
  TestJsonData

  /*
  test("Getting all keys of a JSON record") {
    val jsonFactory: JsonFactory = new JsonFactory()
    val allKeys = getAllKeys(jsonFactory, jsonRecord1)
    val schema = makeStruct(allKeys.toSeq.map(key => key.substring(1, key.length - 1).split("`.`").toSeq))
    println(schema)
  }
  */

  test("Schema inferring") {
    val (schema, logicalPlan) = JsonTable.inferSchemaWithJacksonStreaming2(jsonTextData3)
    // println(logicalPlan)
    JsonTable.printSchema(schema)
  }

  /*
  test("Schema inferring") {
    val logicalPlan = JsonTable.inferSchema(jsonTextData)
    println(logicalPlan)
    logicalPlan.registerAsTable("simpleJsonTable")
    val selectAll = sql("SELECT * FROM simpleJsonTable")
    selectAll.collect().foreach(println)

    val simplePredicate = sql("SELECT * FROM simpleJsonTable where a > 10")
    println(simplePredicate.queryExecution)
    println(simplePredicate.queryExecution.logical)
    println(simplePredicate)
    simplePredicate.collect().foreach(println)

    val simplePredicateInNestedField = sql("SELECT * FROM simpleJsonTable where e.e1 > 10")
    println(simplePredicateInNestedField.queryExecution)
    println(simplePredicateInNestedField.queryExecution.logical)
    println(simplePredicateInNestedField)
    simplePredicateInNestedField.collect().foreach(println)

  }
  */

}
