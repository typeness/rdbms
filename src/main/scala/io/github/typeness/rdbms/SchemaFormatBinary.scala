package io.github.typeness.rdbms

import upickle.default._


object SchemaFormatBinary extends SchemaFormat[Array[Byte]] {

  override def serialize(schema: Schema): Array[Byte] = writeBinary(schema)

  override def deserialize(t: Array[Byte]): Either[SQLError, Schema] = Right(readBinary[Schema](t))
}
