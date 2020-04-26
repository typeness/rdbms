package io.github.typeness.rdbms

import Relation._

sealed trait SQL

sealed trait Manipulation extends SQL
// Data Manipulation Language
sealed trait Insert extends Manipulation {
  def to: RelationName
}
case class NamedInsert(to: RelationName, rows: List[Row]) extends Insert
case class AnonymousInsert(to: RelationName, rows: List[List[Literal]]) extends Insert
case class Delete(name: RelationName, condition: Option[Bool]) extends Manipulation
case class Update(name: RelationName, updated: Row, condition: Option[Bool]) extends Manipulation

// Data Definition Language
sealed trait Definition extends SQL
case class Create(name: RelationName,
                  attributes: Header,
                  relationConstraints: List[RelationConstraint],
                  identity: Option[Identity])
    extends Definition

sealed trait AlterTable extends Definition {
  def relation: RelationName
}
case class AlterAddColumn(relation: RelationName, headingAttribute: HeadingAttribute) extends AlterTable
case class AlterDropColumn(relation: RelationName, column: String) extends AlterTable
case class AlterAddConstraint(relation: RelationName, constraint: RelationConstraint) extends AlterTable
case class AlterDropConstraint(relation: RelationName, constraint: String) extends AlterTable

case class DropTable(name: RelationName) extends Definition

// Data Access Language
sealed trait Control extends SQL
case object Grant extends Control

// Data Query Language
sealed trait Query extends SQL

case class Select(
    projection: List[Projection],
    from: Option[RelationName],
    joins: List[Join],
    where: Option[Bool],
    groupBy: List[String],
    having: Option[Bool],
    order: List[Order],
    distinct: Boolean = false,
    alias: Option[RelationName] = None
) extends Query {
  lazy val getAggregates: List[Aggregate] =
    projection
      .collect {
        case agg: Aggregate => agg
        case Alias(agg: Aggregate, _) => agg
      }
}

case class Union(left: Query, right: Query) extends Query
case class UnionAll(left: Query, right: Query) extends Query
case class Intersect(left: Query, right: Query) extends Query
case class Except(left: Query, right: Query) extends Query

sealed trait Order {
  def name: String
}
case class Ascending(name: String) extends Order
case class Descending(name: String) extends Order

sealed trait Bool {
  def show: String
}
case class Equals(left: Projection, right: Projection) extends Bool {
  override def show: String = s"${left.show}==${right.show}"
}
case class GreaterOrEquals(left: Projection, right: Projection) extends Bool {
  override def show: String = s"${left.show}>=${right.show}"
}
case class Greater(left: Projection, right: Projection) extends Bool {
  override def show: String = s"${left.show}>${right.show}"
}
case class LessOrEquals(left: Projection, right: Projection) extends Bool {
  override def show: String = s"${left.show}<=${right.show}"
}
case class Less(left: Projection, right: Projection) extends Bool {
  override def show: String = s"${left.show}<${right.show}"
}
case class IsNULL(name: String) extends Bool {
  override def show: String = "IS NULL"
}
case class IsNotNULL(name: String) extends Bool {
  override def show: String = "IS NOT NULL"
}
case class Between(name: String, lhs: Projection, rhs: Projection) extends Bool {
  override def show: String = s"BETWEEN ${lhs.show} AND ${rhs.show}"
}
case class And(lhs: Bool, rhs: Bool) extends Bool {
  override def show: String = s"${lhs.show} AND ${rhs.show}"
}
case class Or(lhs: Bool, rhs: Bool) extends Bool {
  override def show: String = s"${lhs.show} OR ${rhs.show}"
}
case class Like(name: String, text: String) extends Bool {
  override def show: String = s"LIKE $text"
}
case class Not(value: Bool) extends Bool {
  override def show: String = s"NOT ${value.show}"
}

case class Where(condition: Bool)

sealed trait Join {
  def name: RelationName
}
case class CrossJoin(name: RelationName) extends Join
case class InnerJoin(name: RelationName, on: Bool) extends Join
case class LeftOuterJoin(name: RelationName, on: Bool) extends Join
case class RightOuterJoin(name: RelationName, on: Bool) extends Join
case class FullOuterJoin(name: RelationName, on: Bool) extends Join
