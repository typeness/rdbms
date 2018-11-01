package io.github.typeness.rdbms

sealed trait Property
case object Unique extends Property
case object NotNULL extends Property
case object PrimaryKey extends Property
case class Default(value: Literal) extends Property
case class Identity(name: String, current: Int, step: Int)


case class Schema(relations: List[Relation]) {
  def getRelation(name: String): Either[SchemaDoesNotExists, Relation] =
    relations.find(_.name == name) match {
      case Some(value) => Right(value)
      case _ => Left(SchemaDoesNotExists(name))
    }
}



object Relation {
  type Row = List[BodyAttribute]
  type Header = List[HeadingAttribute]
}



object Row {
  import Relation._
  def select(name: String, row: Row): Option[BodyAttribute] = row.find(_.name == name)
}

import Relation._

case class Relation(name: String, primaryKey: List[String], identity: Option[Identity],
                    heading: List[HeadingAttribute], body: List[Row])

