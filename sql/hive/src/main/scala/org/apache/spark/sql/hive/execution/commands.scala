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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.execution.{Command, LeafNode}
import org.apache.spark.sql.hive.HiveContext

/**
 * :: DeveloperApi ::
 * Analyzes the given table in the current database to generate statistics, which will be
 * used in query optimizations.
 *
 * Right now, it only supports Hive tables and it only updates the size of a Hive table
 * in the Hive metastore.
 */
@DeveloperApi
case class AnalyzeTable(tableName: String) extends LeafNode with Command {
  def hiveContext = sqlContext.asInstanceOf[HiveContext]

  def output = Seq.empty

  override protected lazy val sideEffectResult: Seq[Row] = {
    hiveContext.analyze(tableName)
    Seq.empty[Row]
  }
}

/**
 * :: DeveloperApi ::
 * Drops a table from the metastore and removes it if it is cached.
 */
@DeveloperApi
case class DropTable(tableName: String, ifExists: Boolean) extends LeafNode with Command {
  def hiveContext = sqlContext.asInstanceOf[HiveContext]

  def output = Seq.empty

  override protected lazy val sideEffectResult: Seq[Row] = {
    val ifExistsClause = if (ifExists) "IF EXISTS " else ""
    try {
      hiveContext.tryUncacheQuery(hiveContext.table(tableName))
    } catch {
      // This table's metadata is not in
      case _: org.apache.hadoop.hive.ql.metadata.InvalidTableException =>
      // Got an error during table lookup or uncache the query. We log the exception message.
      // Users should be able to drop such kinds of tables regardless if there is an exception.
      case e: Exception => log.warn(s"${e.getMessage}")
    }
    hiveContext.runSqlHive(s"DROP TABLE $ifExistsClause$tableName")
    hiveContext.catalog.unregisterTable(Seq(tableName))
    Seq.empty[Row]
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class AddJar(path: String) extends LeafNode with Command {
  def hiveContext = sqlContext.asInstanceOf[HiveContext]

  override def output = Seq.empty

  override protected lazy val sideEffectResult: Seq[Row] = {
    hiveContext.runSqlHive(s"ADD JAR $path")
    hiveContext.sparkContext.addJar(path)
    Seq.empty[Row]
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class AddFile(path: String) extends LeafNode with Command {
  def hiveContext = sqlContext.asInstanceOf[HiveContext]

  override def output = Seq.empty

  override protected lazy val sideEffectResult: Seq[Row] = {
    hiveContext.runSqlHive(s"ADD FILE $path")
    hiveContext.sparkContext.addFile(path)
    Seq.empty[Row]
  }
}
