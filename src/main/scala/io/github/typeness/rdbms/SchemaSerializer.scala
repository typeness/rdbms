package io.github.typeness.rdbms

trait SchemaSerializer[T] {
  def serialize(schema: Schema): List[T] = schema.relations.values.map(serialize).toList
  def serialize(relation: Relation): T
}
