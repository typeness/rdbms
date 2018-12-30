package io.github.typeness.rdbms

import Relation._

sealed trait SQL

sealed trait Manipulation extends SQL

// Data Manipulation Language
sealed trait Insert extends SQL
case class NamedInsert(name: String, row: Row) extends Insert
case class AnonymousInsert(to: String, values: List[Literal]) extends Insert
case class Delete(name: String, condition: Option[Bool]) extends SQL
case class Update(name: String, updated: Row, condition: Option[Bool]) extends SQL

// Data Definition Language
sealed trait Definition extends SQL
case class Create(name: String,
                  attributes: Header,
                  primaryKeys: List[String],
                  identity: Option[Identity])
    extends Definition
case class AlterAdd(name: String) extends Definition
case class AlterDrop(name: String) extends Definition
case class Drop(name: String) extends Definition

// Data Access Language
sealed trait Control extends SQL
case object Grant extends Control

// Data Query Language
sealed trait Query extends SQL

case class Select(
    projection: List[Expression],
    from: String,
    joins: List[Join],
    where: Option[Bool],
    groupBy: List[String],
    having: Option[Bool],
    order: List[Order],
    distinct: Boolean = false
) extends Query {
  def getAggregates: List[Aggregate] =
    projection
      .filter {
        case d: Aggregate => true
        case _            => false
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
case class Equals(name: String, value: Expression) extends Bool
case class GreaterOrEquals(name: String, value: Expression) extends Bool
case class Greater(name: String, value: Expression) extends Bool
case class LessOrEquals(name: String, value: Expression) extends Bool
case class Less(name: String, value: Expression) extends Bool
case class IsNULL(name: String) extends Bool
case class Between(name: String, lhs: Expression, rhs: Expression) extends Bool
case class And(lhs: Bool, rhs: Bool) extends Bool
case class Or(lhs: Bool, rhs: Bool) extends Bool

case class Where(condition: Bool)

sealed trait Join {
  def name: String
}
case class CrossJoin(name: String) extends Join
case class InnerJoin(name: String, on: Bool) extends Join
case class LeftOuterJoin(name: String, on: Bool) extends Join
case class RightOuterJoin(name: String, on: Bool) extends Join
case class FullOuterJoin(name: String, on: Bool) extends Join
