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

package org.apache.spark.sql.execution

import java.io._
import java.nio.ByteBuffer

import org.apache.spark.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.types._
import org.apache.spark.util.MutablePair

import scala.reflect.ClassTag

/**
 * A serializer stream for rows with only primitive types.
 */
class SparkSqlSerializer2V3SerializationStream(
    keySchema: Array[DataType],
    valueSchema: Array[DataType],
    out: OutputStream) extends SerializationStream with Logging{

  val rowOut = new DataOutputStream(out)

  var first = true

  def writeObject[T: ClassTag](t: T): SerializationStream = {
    val pair = t.asInstanceOf[MutablePair[Row, Row]]
    val key = pair._1
    val value = pair._2

    if (first) {
      logInfo("key: " + key + " value: " + value)
      first = false
    }

      if (keySchema != null) { writeRow(key, keySchema) }
    if (valueSchema != null) { writeRow(value, valueSchema) }

    this
  }

  def writeRow(row: Row, schema: Array[DataType]): Unit = {
    var i = 0
    while (i < schema.length) {
      schema(i) match {
        case StringType =>
          val value = row.getString(i)
          if (value.length > 12) sys.error(s"$value's length is larger than 12.")
          rowOut.writeInt(value.length)
          rowOut.write(value.getBytes("utf-8"))
        case IntegerType =>
          rowOut.writeInt(row.getInt(i))
        case LongType =>
          rowOut.writeLong(row.getLong(i))
        case DoubleType =>
          rowOut.writeDouble(row.getDouble(i))
      }
      i += 1
    }
  }

  def flush(): Unit = {
    rowOut.flush()
  }

  def close(): Unit = {
    rowOut.close()
  }
}

/**
 * A deserializer stream for rows with only primitive types.
 */
class SparkSqlSerializer2V3DeserializationStream(
    keySchema: Array[DataType],
    valueSchema: Array[DataType],
    in: InputStream)
  extends DeserializationStream with Logging {

  val rowIn = new DataInputStream(new BufferedInputStream(in))

  val key = new SpecificMutableRow(keySchema)
  val value = if (valueSchema != null) new SpecificMutableRow(valueSchema) else null

  var first = true

  def readObject[T: ClassTag](): T = {
    if (keySchema != null) { readRow(key, keySchema) }
    if (valueSchema != null) { readRow(value, valueSchema) }

    if (first) {
      logInfo("key: " + key + " value: " + value)
      first = false
    }

    (key, value).asInstanceOf[T]
  }

  def readRow(row: SpecificMutableRow, schema: Array[DataType]): Unit = {
    var i = 0
    while (i < schema.length) {
      schema(i) match {
        case StringType =>
          val length = rowIn.readInt()
          if (length > 12) sys.error(s"The string's length is larger than 12.")
          val bytes = new Array[Byte](length)
          rowIn.readFully(bytes)
          row.setString(i, new String(bytes, "utf-8"))
        case IntegerType =>
          row.setInt(i, rowIn.readInt())
        case LongType =>
          row.setLong(i, rowIn.readLong())
        case DoubleType =>
          row.setDouble(i, rowIn.readDouble())
      }
      i += 1
    }
  }

  def close(): Unit = {
    rowIn.close()
  }
}

/**
 * A serializer for rows with only primitive types.
 */
class SparkSqlSerializer2V3Instance(
    keySchema: Array[DataType],
    valueSchema: Array[DataType])
  extends SerializerInstance {

  def serialize[T: ClassTag](t: T): ByteBuffer = ???

  def deserialize[T: ClassTag](bytes: ByteBuffer): T = ???

  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = ???

  def serializeStream(s: OutputStream): SerializationStream = {
    new SparkSqlSerializer2V3SerializationStream(keySchema, valueSchema, s)
  }

  def deserializeStream(s: InputStream): DeserializationStream = {
    new SparkSqlSerializer2V3DeserializationStream(keySchema, valueSchema, s)
  }
}

/**
 * A serializer only used by [[Exchange]] and it only deals with Rows containing simple types
 * (i.e. StringType, LongType, DoubleType, IntegerType).
 */
private[sql] class SparkSqlSerializer2V3(keySchema: Array[DataType], valueSchema: Array[DataType])
  extends org.apache.spark.serializer.Serializer
  with Logging
  with Serializable{

  def newInstance(): SerializerInstance = new SparkSqlSerializer2V3Instance(keySchema, valueSchema)
}