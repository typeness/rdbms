package io.github.typeness.rdbms

import cats.syntax.either._

case class Identity(name: AttributeName, current: Int, step: Int)

object Schema {
  def apply(relations: Relation*): Schema =
    Schema(relations.map(relation => (relation.name, relation)).toMap)
}

case class Schema(relations: Map[RelationName, Relation]) {
  def getRelation(name: Option[RelationName]): Either[RelationDoesNotExists, Relation] =
    name match {
      case Some(value) => getRelation(value)
      case None => Right(Relation.empty)
    }

  def getRelation(name: RelationName): Either[RelationDoesNotExists, Relation] =
    Either.fromOption(relations.get(name), RelationDoesNotExists(name))

  def update(relation: Relation): Schema =
    Schema(relations.updated(relation.name, relation))
}

object Relation {
  type Header = List[HeadingAttribute]
  val empty: Relation = Relation(RelationName(""), Nil, None, Nil, List(Row()), Nil)
}

case class Row(attributes: List[BodyAttribute]) {
  def projectOption(name: AttributeName): Option[BodyAttribute] =
    attributes.find(_.name == name)
  def projectEither(name: AttributeName): Either[MissingColumnName, BodyAttribute] =
    Either.fromOption(projectOption(name), MissingColumnName(name))
  def projectList(names: List[AttributeName]): Row =
    Row(attributes.filter(attrib => names.has(attrib.name)))
  lazy val getNames: List[AttributeName] = attributes.map(_.name)
  lazy val getValues: List[Literal] = attributes.map(_.literal)
  def map(f: BodyAttribute => BodyAttribute): Row = Row(attributes.map(f))
  def filter(f: BodyAttribute => Boolean): Row = Row(attributes.filter(f))

}

case class RelationName(value: String) extends AnyVal

object Row {
  def apply(attributes: BodyAttribute*): Row = this(attributes.toList)
}

case class Relation(name: RelationName,
                    primaryKeys: List[AttributeName],
                    identity: Option[Identity],
                    heading: Relation.Header,
                    body: List[Row],
                    relationConstraints: List[RelationConstraint]) {

  lazy val getPrimaryKeys: List[HeadingAttribute] =
    heading.filter(_.constraints.exists {
      case _: PrimaryKey => true
      case _             => false
    })
  lazy val getForeignKeys: List[(AttributeName, ForeignKey)] =
    heading.collect {
      case attribute: HeadingAttribute
          if attribute.constraints.collect { case key: ForeignKey => key }.nonEmpty =>
        (attribute.name, attribute.constraints.collect { case key: ForeignKey => key }.head)
    }
}
