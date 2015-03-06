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

import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, MutableRow}

import scala.reflect.ClassTag

import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.sql.Row

/**
 * This is a hand coded serialization stream to just deal with a Row having a single string.*/
class SparkSqlSerializer2SerializationStream(out: OutputStream) extends SerializationStream {
  val rowOut = new DataOutputStream(out)

  def writeObject[T: ClassTag](t: T): SerializationStream = {
    val (key, value) = t.asInstanceOf[(Row, Row)]
    val keyString = key.getString(0)
    val valueString = value.getString(0)
    rowOut.writeInt(keyString.length)
    rowOut.write(keyString.getBytes("utf-8"))
    rowOut.writeInt(valueString.length)
    rowOut.write(valueString.getBytes("utf-8"))
    this
  }

  def flush(): Unit = {
    out.flush()
  }

  def close(): Unit = {
    out.close()
  }
}

class SparkSqlSerializer2DeserializationStream(
    in: InputStream,
    key: MutableRow,
    value: MutableRow) extends DeserializationStream {
  val rowIn = new DataInputStream(new BufferedInputStream(in))

  def readObject[T: ClassTag](): T = {
    val keyLength = rowIn.readInt()
    val keyBytes = new Array[Byte](keyLength)
    rowIn.read(keyBytes, 0, keyLength)
    key.setString(0, new String(keyBytes, "utf-8"))

    val valueLength = rowIn.readInt()
    val valueBytes = new Array[Byte](valueLength)
    rowIn.read(valueBytes, 0, valueLength)
    value.setString(0, new String(valueBytes, "utf-8"))

    (key, value).asInstanceOf[T]
  }

  def close(): Unit = {
    in.close()
  }
}

class SparkSqlSerializer2Instance extends SerializerInstance {
  def serialize[T: ClassTag](t: T): ByteBuffer = ???

  def deserialize[T: ClassTag](bytes: ByteBuffer): T = ???

  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = ???

  def serializeStream(s: OutputStream): SerializationStream = {
    new SparkSqlSerializer2SerializationStream(s)
  }

  def deserializeStream(s: InputStream): DeserializationStream = {
    val key = new GenericMutableRow(1)
    val value = new GenericMutableRow(1)
    new SparkSqlSerializer2DeserializationStream(s, key, value)
  }
}

/**
 * A serializer only used by [[Exchange]] and it only deals with Rows having a single string value.
 */
private[sql] class SparkSqlSerializer2
  extends org.apache.spark.serializer.Serializer
  with Logging
  with Serializable{

  def newInstance(): SerializerInstance = new SparkSqlSerializer2Instance
}