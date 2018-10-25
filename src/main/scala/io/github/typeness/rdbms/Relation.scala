package io.github.typeness.rdbms

sealed trait Property
case object Unique extends Property
case object NotNULL extends Property
case object PrimaryKey extends Property
case class Default(value: Value) extends Property

case class Value(value: Literal)

object Relation {
  type Row = List[BodyAttribute]
  type Header = List[HeadingAttribute]
}

import Relation._

case class Relation(name: String, primaryKey: List[String],
                    heading: List[HeadingAttribute], body: List[Row])

