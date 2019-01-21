package io.github.typeness.rdbms

import cats.syntax.either._

case class Identity(name: String, current: Int, step: Int)

object Schema {
  def apply(relations: List[Relation]): Schema =
    Schema(relations.map(relation => (relation.name, relation)).toMap)
}

case class Schema(relations: Map[String, Relation]) {
  def getRelation(name: String): Either[RelationDoesNotExists, Relation] =
    Either.fromOption(relations.get(name), RelationDoesNotExists(name))

  def update(relation: Relation): Schema =
    Schema(relations.updated(relation.name, relation))
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
                    heading: Relation.Header,
                    body: List[Row]) {

  def getPrimaryKeys: List[HeadingAttribute] =
    heading.filter(_.constraints.exists {
      case _: PrimaryKey.type => true
      case _                  => false
    })
  def getForeignKeys: List[(String, ForeignKey)] =
    heading.collect {
      case attribute: Attribute
          if attribute.constraints.collect { case key: ForeignKey => key }.nonEmpty =>
        (attribute.name, attribute.constraints.collect { case key: ForeignKey => key }.head)
    }
}
