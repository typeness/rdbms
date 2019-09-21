package io.github.typeness.rdbms

import cats.syntax.either._

case class Identity(name: String, current: Int, step: Int)

object Schema {
  def apply(relations: List[Relation]): Schema =
    Schema(relations.map(relation => (relation.name, relation)).toMap)
}

case class Schema(relations: Map[String, Relation]) {
  def getRelation(name: Option[String]): Either[RelationDoesNotExists, Relation] =
    name match {
      case Some(value) => getRelation(value)
      case None => Right(Relation.empty)
    }

  def getRelation(name: String): Either[RelationDoesNotExists, Relation] =
    Either.fromOption(relations.get(name), RelationDoesNotExists(name))

  def update(relation: Relation): Schema =
    Schema(relations.updated(relation.name, relation))
}

object Relation {
  type Header = List[HeadingAttribute]
  val empty = Relation("", Nil, None, Nil, List(Row()), Nil)
}

case class Row(attributes: List[BodyAttribute]) {
  def projectOption(name: String): Option[BodyAttribute] =
    attributes.find(_.name == name)
  def projectEither(name: String): Either[MissingColumnName, BodyAttribute] =
    Either.fromOption(projectOption(name), MissingColumnName(name))
  def projectList(names: List[String]): Row =
    Row(attributes.filter(attrib => names.contains(attrib.name)))
  lazy val getNames: List[String] = attributes.map(_.name)
  lazy val getValues: List[Literal] = attributes.map(_.literal)
  def map(f: BodyAttribute => BodyAttribute): Row = Row(attributes.map(f))
  def filter(f: BodyAttribute => Boolean): Row = Row(attributes.filter(f))

}

object Row {
  def apply(attributes: BodyAttribute*): Row = this(attributes.toList)
}

case class Relation(name: String,
                    primaryKeys: List[String],
                    identity: Option[Identity],
                    heading: Relation.Header,
                    body: List[Row],
                    relationConstraints: List[RelationConstraint]) {

  lazy val getPrimaryKeys: List[HeadingAttribute] =
    heading.filter(_.constraints.exists {
      case _: PrimaryKey => true
      case _             => false
    })
  lazy val getForeignKeys: List[(String, ForeignKey)] =
    heading.collect {
      case attribute: Attribute
          if attribute.constraints.collect { case key: ForeignKey => key }.nonEmpty =>
        (attribute.name, attribute.constraints.collect { case key: ForeignKey => key }.head)
    }
}
