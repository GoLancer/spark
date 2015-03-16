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

import scala.reflect.ClassTag

import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}
import org.apache.spark.Logging

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.types._

/**
 * A serializer for rows with only primitive types.
 */
class SparkSqlSerializer2V2Instance(
    keySchema: Array[DataType],
    valueSchema: Array[DataType])
  extends SerializerInstance with Logging {

  def serialize[T: ClassTag](t: T): ByteBuffer = ???

  def deserialize[T: ClassTag](bytes: ByteBuffer): T = ???

  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = ???

  def serializeStream(s: OutputStream): SerializationStream = {
    val serializationStream =
      SparkSqlSerializer2V2.serializeStream(s, keySchema, valueSchema)

    logInfo(
      "SparkSqlSerializer2V2Instance's serializeStream is " +
      s"${serializationStream.getClass.getCanonicalName}")

    serializationStream
  }

  def deserializeStream(s: InputStream): DeserializationStream = {
    val deserializationStream =
      SparkSqlSerializer2V2.deserializeStream(s, keySchema, valueSchema)

    logInfo(
      "SparkSqlSerializer2V2Instance's serializeStream is " +
        s"${deserializationStream.getClass.getCanonicalName}")

    deserializationStream
  }
}

/**
 * A serializer only used by [[Exchange]] and it only deals with Rows containing simple types
 * (i.e. StringType, LongType, DoubleType, IntegerType).
 */
private[sql] class SparkSqlSerializer2V2(
    keySchema: Array[DataType],
    valueSchema: Array[DataType])
  extends org.apache.spark.serializer.Serializer
  with Logging
  with Serializable{

  def newInstance(): SerializerInstance =
    new SparkSqlSerializer2V2Instance(keySchema, valueSchema)
}

abstract class BDBSerializationStream(out: OutputStream) extends SerializationStream {
  val rowOut = new DataOutputStream(out)
  def flush(): Unit = {
    rowOut.flush()
  }

  def close(): Unit = {
    rowOut.close()
  }
}

class Q2SerializationStream(out: OutputStream) extends BDBSerializationStream(out) {
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    val pair = t.asInstanceOf[(Row, Row)]
    val key = pair._1
    val value = pair._2

    rowOut.writeUTF(key.getString(0))

    rowOut.writeUTF(value.getString(0))
    rowOut.writeDouble(value.getDouble(1))

    this
  }
}

class Q2SerializationStream_binary(out: OutputStream) extends BDBSerializationStream(out) {
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    val pair = t.asInstanceOf[(Row, Row)]
    val key = pair._1
    val value = pair._2

    var tmp = key.get(0).asInstanceOf[Array[Byte]]
    rowOut.writeInt(tmp.length)
    rowOut.write(tmp)

    tmp = value.get(0).asInstanceOf[Array[Byte]]
    rowOut.writeInt(tmp.length)
    rowOut.write(tmp)
    rowOut.writeDouble(value.getDouble(1))

    this
  }
}

class Q31SerializationStream(out: OutputStream) extends BDBSerializationStream(out) {
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    val pair = t.asInstanceOf[(Row, Row)]
    val key = pair._1
    val value = pair._2

    rowOut.writeUTF(key.getString(0))

    rowOut.writeUTF(value.getString(0))
    rowOut.writeDouble(value.getDouble(1))
    rowOut.writeUTF(value.getString(2))

    this
  }
}

class Q31SerializationStream_alter(out: OutputStream) extends BDBSerializationStream(out) {
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    val pair = t.asInstanceOf[(Row, Row)]
    val key = pair._1
    val value = pair._2

    rowOut.writeUTF(key.getString(0))

    rowOut.writeUTF(value.getString(0))
    rowOut.writeUTF(value.getString(1))
    rowOut.writeDouble(value.getDouble(2))

    this
  }
}

class Q32SerializationStream(out: OutputStream) extends BDBSerializationStream(out) {
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    val pair = t.asInstanceOf[(Row, Row)]
    val key = pair._1
    val value = pair._2

    rowOut.writeUTF(key.getString(0))

    rowOut.writeInt(value.getInt(0))
    rowOut.writeUTF(value.getString(1))

    this
  }
}

class Q32SerializationStream_alter(out: OutputStream) extends BDBSerializationStream(out) {
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    val pair = t.asInstanceOf[(Row, Row)]
    val key = pair._1
    val value = pair._2

    rowOut.writeUTF(key.getString(0))

    rowOut.writeUTF(value.getString(0))
    rowOut.writeInt(value.getInt(1))

    this
  }
}

class Q33SerializationStream(out: OutputStream) extends BDBSerializationStream(out) {
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    val pair = t.asInstanceOf[(Row, Row)]
    val key = pair._1
    val value = pair._2

    rowOut.writeUTF(key.getString(0))

    rowOut.writeUTF(value.getString(0))
    rowOut.writeLong(value.getLong(1))
    rowOut.writeLong(value.getLong(2))
    rowOut.writeDouble(value.getDouble(3))

    this
  }
}

class Q33SerializationStream_alter(out: OutputStream) extends BDBSerializationStream(out) {
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    val pair = t.asInstanceOf[(Row, Row)]
    val key = pair._1
    val value = pair._2

    rowOut.writeUTF(key.getString(0))

    rowOut.writeUTF(value.getString(0))
    rowOut.writeDouble(value.getDouble(1))
    rowOut.writeLong(value.getLong(2))
    rowOut.writeLong(value.getLong(3))

    this
  }
}

class Q34SerializationStream(out: OutputStream) extends BDBSerializationStream(out) {
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    val pair = t.asInstanceOf[(Row, Row)]
    val key = pair._1

    rowOut.writeUTF(key.getString(0))
    rowOut.writeDouble(key.getDouble(1))
    rowOut.writeDouble(key.getDouble(2))

    this
  }
}

abstract class BDBDeserializationStream(in: InputStream) extends DeserializationStream {
  val rowIn = new DataInputStream(new BufferedInputStream(in))

  def close(): Unit = {
    rowIn.close()
  }
}

class Q2DeserializationStream(
    in: InputStream,
    keySchema: Array[DataType],
    valueSchema: Array[DataType]) extends BDBDeserializationStream(in) {

  val key = new SpecificMutableRow(keySchema)
  val value = new SpecificMutableRow(valueSchema)

  def readObject[T: ClassTag](): T = {
    key.setString(0, rowIn.readUTF())

    value.setString(0, rowIn.readUTF())
    value.setDouble(1, rowIn.readDouble())

    (key, value).asInstanceOf[T]
  }
}

class Q2DeserializationStream_binary(
   in: InputStream,
   keySchema: Array[DataType],
   valueSchema: Array[DataType]) extends BDBDeserializationStream(in) {

  val key = new SpecificMutableRow(keySchema)
  val value = new SpecificMutableRow(valueSchema)

  def readObject[T: ClassTag](): T = {
    val keyBytes = new Array[Byte](rowIn.readInt())
    rowIn.readFully(keyBytes)
    key.update(0, keyBytes)

    val valueBytes = new Array[Byte](rowIn.readInt())
    rowIn.readFully(valueBytes)
    value.update(0, valueBytes)
    value.setDouble(1, rowIn.readDouble())

    (key, value).asInstanceOf[T]
  }
}

class Q31DeserializationStream(
    in: InputStream,
    keySchema: Array[DataType],
    valueSchema: Array[DataType]) extends BDBDeserializationStream(in) {

  val key = new SpecificMutableRow(keySchema)
  val value = new SpecificMutableRow(valueSchema)

  def readObject[T: ClassTag](): T = {
    key.setString(0, rowIn.readUTF())

    value.setString(0, rowIn.readUTF())
    value.setDouble(1, rowIn.readDouble())
    value.setString(2, rowIn.readUTF())

    (key, value).asInstanceOf[T]
  }
}

class Q31DeserializationStream_alter(
    in: InputStream,
    keySchema: Array[DataType],
    valueSchema: Array[DataType]) extends BDBDeserializationStream(in) {

  val key = new SpecificMutableRow(keySchema)
  val value = new SpecificMutableRow(valueSchema)

  def readObject[T: ClassTag](): T = {
    key.setString(0, rowIn.readUTF())

    value.setString(0, rowIn.readUTF())
    value.setString(1, rowIn.readUTF())
    value.setDouble(2, rowIn.readDouble())

    (key, value).asInstanceOf[T]
  }
}

class Q32DeserializationStream(
    in: InputStream,
    keySchema: Array[DataType],
    valueSchema: Array[DataType]) extends BDBDeserializationStream(in) {
  val key = new SpecificMutableRow(keySchema)
  val value = new SpecificMutableRow(valueSchema)

  def readObject[T: ClassTag](): T = {
    key.setString(0, rowIn.readUTF())

    value.setInt(0, rowIn.readInt())
    value.setString(1, rowIn.readUTF())

    (key, value).asInstanceOf[T]
  }
}

class Q32DeserializationStream_alter(
    in: InputStream,
    keySchema: Array[DataType],
    valueSchema: Array[DataType]) extends BDBDeserializationStream(in) {
  val key = new SpecificMutableRow(keySchema)
  val value = new SpecificMutableRow(valueSchema)

  def readObject[T: ClassTag](): T = {
    key.setString(0, rowIn.readUTF())

    value.setString(0, rowIn.readUTF())
    value.setInt(1, rowIn.readInt())

    (key, value).asInstanceOf[T]
  }
}

class Q33DeserializationStream(
    in: InputStream,
    keySchema: Array[DataType],
    valueSchema: Array[DataType]) extends BDBDeserializationStream(in) {

  val key = new SpecificMutableRow(keySchema)
  val value = new SpecificMutableRow(valueSchema)

  def readObject[T: ClassTag](): T = {
    key.setString(0, rowIn.readUTF())

    value.setString(0, rowIn.readUTF())
    value.setLong(1, rowIn.readLong())
    value.setLong(2, rowIn.readLong())
    value.setDouble(3, rowIn.readDouble())

    (key, value).asInstanceOf[T]
  }
}

class Q33DeserializationStream_alter(
    in: InputStream,
    keySchema: Array[DataType],
    valueSchema: Array[DataType]) extends BDBDeserializationStream(in) {

  val key = new SpecificMutableRow(keySchema)
  val value = new SpecificMutableRow(valueSchema)

  def readObject[T: ClassTag](): T = {
    key.setString(0, rowIn.readUTF())

    value.setString(0, rowIn.readUTF())
    value.setDouble(1, rowIn.readDouble())
    value.setLong(2, rowIn.readLong())
    value.setLong(3, rowIn.readLong())

    (key, value).asInstanceOf[T]
  }
}

class Q34DeserializationStream(
    in: InputStream,
    keySchema: Array[DataType],
    valueSchema: Array[DataType]) extends BDBDeserializationStream(in) {

  val key = new SpecificMutableRow(keySchema)

  def readObject[T: ClassTag](): T = {
    key.setString(0, rowIn.readUTF())
    key.setDouble(1, rowIn.readDouble())
    key.setDouble(2, rowIn.readDouble())

    (key, null).asInstanceOf[T]
  }
}

object SparkSqlSerializer2V2 {
  def serializeStream(
      s: OutputStream,
      keySchema: Array[DataType],
      valueSchema: Array[DataType]): SerializationStream = {

    (keySchema.toSeq, valueSchema.toSeq) match {
      case (Seq(StringType), Seq(StringType, DoubleType)) =>
        new Q2SerializationStream(s)

      case (Seq(BinaryType), Seq(BinaryType, DoubleType)) =>
        new Q2SerializationStream_binary(s)

      case (Seq(StringType), Seq(StringType, DoubleType, StringType)) =>
        new Q31SerializationStream(s)

      case (Seq(StringType), Seq(StringType, StringType, DoubleType)) =>
        new Q31SerializationStream_alter(s)

      case (Seq(StringType), Seq(IntegerType, StringType)) =>
        new Q32SerializationStream(s)

      case (Seq(StringType), Seq(StringType, IntegerType)) =>
        new Q32SerializationStream_alter(s)

      case (Seq(StringType), Seq(StringType, LongType, LongType, DoubleType)) =>
        new Q33SerializationStream(s)

      case (Seq(StringType), Seq(StringType, DoubleType, LongType, LongType)) =>
        new Q33SerializationStream_alter(s)

      case (Seq(StringType, DoubleType, DoubleType), null) =>
        new Q34SerializationStream(s)

      case _ => sys.error("key or value schema is not supported. keySchema is " +
        keySchema.toSeq + " and valueSchema is" + valueSchema.toSeq)
    }
  }

  def deserializeStream(
      s: InputStream,
      keySchema: Array[DataType],
      valueSchema: Array[DataType]): DeserializationStream = {

    (keySchema.toSeq, valueSchema.toSeq) match {
      case (Seq(StringType), Seq(StringType, DoubleType)) =>
        new Q2DeserializationStream(s, keySchema, valueSchema)

      case (Seq(BinaryType), Seq(BinaryType, DoubleType)) =>
        new Q2DeserializationStream_binary(s, keySchema, valueSchema)

      case (Seq(StringType), Seq(StringType, DoubleType, StringType)) =>
        new Q31DeserializationStream(s, keySchema, valueSchema)

      case (Seq(StringType), Seq(StringType, StringType, DoubleType)) =>
        new Q31DeserializationStream_alter(s, keySchema, valueSchema)

      case (Seq(StringType), Seq(IntegerType, StringType)) =>
        new Q32DeserializationStream(s, keySchema, valueSchema)

      case (Seq(StringType), Seq(StringType, IntegerType)) =>
        new Q32DeserializationStream_alter(s, keySchema, valueSchema)

      case (Seq(StringType), Seq(StringType, LongType, LongType, DoubleType)) =>
        new Q33DeserializationStream(s, keySchema, valueSchema)

      case (Seq(StringType), Seq(StringType, DoubleType, LongType, LongType)) =>
        new Q33DeserializationStream_alter(s, keySchema, valueSchema)

      case (Seq(StringType, DoubleType, DoubleType), null) =>
        new Q34DeserializationStream(s, keySchema, valueSchema)

      case _ => sys.error("key or value schema is not supported. keySchema is " +
        keySchema.toSeq + " and valueSchema is" + valueSchema.toSeq)
    }
  }
}