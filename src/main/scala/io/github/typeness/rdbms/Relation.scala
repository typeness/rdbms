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
  type Header = List[HeadingAttribute]
}

case class Row(attributes: List[BodyAttribute]) {
  def select(name: String): Option[BodyAttribute] = attributes.find(_.name == name)
  def getNames: List[String] = attributes.map(_.name)
  def getValues: List[Literal] = attributes.map(_.literal)
  def map(f: BodyAttribute => BodyAttribute): Row = Row(attributes.map(f))
  def filter(f: BodyAttribute => Boolean): Row = Row(attributes.filter(f))

}

object Row {
  def apply(attributes: BodyAttribute*): Row = this(attributes.toList)
}

case class Relation(name: String, primaryKey: List[String], identity: Option[Identity],
                    heading: List[HeadingAttribute], body: List[Row])

