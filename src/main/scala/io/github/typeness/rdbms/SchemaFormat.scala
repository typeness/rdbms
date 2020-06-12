package io.github.typeness.rdbms

trait SchemaFormat[T] {
  def serialize(schema: Schema): T
  def deserialize(t: T): Either[SQLError, Schema]
}
