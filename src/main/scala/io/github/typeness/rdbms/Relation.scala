package io.github.typeness.rdbms

import cats.syntax.either._

case class Identity(name: String, current: Int, step: Int)

case class Schema(relations: List[Relation]) {
  def getRelation(name: String): Either[SchemaDoesNotExists, Relation] =
    relations.find(_.name == name) match {
      case Some(value) => Right(value)
      case _           => Left(SchemaDoesNotExists(name))
    }
}

object Relation {
  type Header = List[HeadingAttribute]
}

case class Row(attributes: List[BodyAttribute]) {
  def projectOption(name: String): Option[BodyAttribute] =
    attributes.find(_.name == name)
  def projectEither(name: String): Either[MissingColumnName, BodyAttribute] =
    Either.fromOption(projectOption(name), MissingColumnName(name))
  def projectList(names: List[String]): Row =
    Row(attributes.filter(attrib => names.contains(attrib.name)))
  def getNames: List[String] = attributes.map(_.name)
  def getValues: List[Literal] = attributes.map(_.literal)
  def map(f: BodyAttribute => BodyAttribute): Row = Row(attributes.map(f))
  def filter(f: BodyAttribute => Boolean): Row = Row(attributes.filter(f))

}

object Row {
  def apply(attributes: BodyAttribute*): Row = this(attributes.toList)
}

case class Relation(name: String,
                    primaryKey: List[String],
                    identity: Option[Identity],
                    heading: List[HeadingAttribute],
                    body: List[Row]) {

}
