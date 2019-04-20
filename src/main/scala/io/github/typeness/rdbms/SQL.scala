package io.github.typeness.rdbms

import Relation._

sealed trait SQL

sealed trait Manipulation extends SQL
// Data Manipulation Language
sealed trait Insert extends Manipulation {
  def to: String
}
case class NamedInsert(to: String, rows: List[Row]) extends Insert
case class AnonymousInsert(to: String, rows: List[List[Literal]]) extends Insert
case class Delete(name: String, condition: Option[Bool]) extends Manipulation
case class Update(name: String, updated: Row, condition: Option[Bool]) extends Manipulation

// Data Definition Language
sealed trait Definition extends SQL
case class Create(name: String,
                  attributes: Header,
                  relationConstraints: List[RelationConstraint],
                  identity: Option[Identity])
    extends Definition

sealed trait AlterTable extends Definition {
  def relation: String
}
case class AlterAddColumn(relation: String, headingAttribute: HeadingAttribute) extends AlterTable
case class AlterDropColumn(relation: String, column: String) extends AlterTable
case class AlterAddConstraint(relation: String, constraint: RelationConstraint) extends AlterTable
case class AlterDropConstraint(relation: String, constraint: String) extends AlterTable

case class DropTable(name: String) extends Definition

// Data Access Language
sealed trait Control extends SQL
case object Grant extends Control

// Data Query Language
sealed trait Query extends SQL

case class Select(
    projection: List[Projection],
    from: Option[String],
    joins: List[Join],
    where: Option[Bool],
    groupBy: List[String],
    having: Option[Bool],
    order: List[Order],
    distinct: Boolean = false,
    alias: Option[String] = None
) extends Query {
  lazy val getAggregates: List[Aggregate] =
    projection
      .collect {
        case agg: Aggregate => agg
        case Alias(agg: Aggregate, _) => agg
      }
      .asInstanceOf[List[Aggregate]]
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

sealed trait Bool
case class Equals(left: Projection, right: Projection) extends Bool
case class GreaterOrEquals(left: Projection, right: Projection) extends Bool
case class Greater(left: Projection, right: Projection) extends Bool
case class LessOrEquals(left: Projection, right: Projection) extends Bool
case class Less(left: Projection, right: Projection) extends Bool
case class IsNULL(name: String) extends Bool
case class IsNotNULL(name: String) extends Bool
case class Between(name: String, lhs: Projection, rhs: Projection) extends Bool
case class And(lhs: Bool, rhs: Bool) extends Bool
case class Or(lhs: Bool, rhs: Bool) extends Bool
case class Like(name: String, text: String) extends Bool
case class Not(value: Bool) extends Bool

case class Where(condition: Bool)

sealed trait Join {
  def name: String
}
case class CrossJoin(name: String) extends Join
case class InnerJoin(name: String, on: Bool) extends Join
case class LeftOuterJoin(name: String, on: Bool) extends Join
case class RightOuterJoin(name: String, on: Bool) extends Join
case class FullOuterJoin(name: String, on: Bool) extends Join
