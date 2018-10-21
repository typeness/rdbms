package io.github.typeness.rdbms

import Relation._

sealed trait SQL

sealed trait Manipulation extends SQL
sealed trait Insert extends SQL
case class NamedInsert(name: String, row: Row) extends Insert
case class AnonymousInsert(to: String, values: List[Value]) extends Insert

sealed trait Definition extends SQL
case class Create(name: String, attributes: Header, primaryKeys: List[String]) extends Definition

sealed trait Control extends SQL
case object Grant extends Control

case class Select(names: List[String], from: String, condition: Option[Bool], order: Option[Order]) extends SQL


sealed trait Order
case object Ascending extends Order
case object Descending extends Order

sealed trait Bool
case class Equals(name: String, value: Value) extends Bool
case class GreaterOrEquals(name: String, value: Value) extends Bool
case class LessOrEquals(name: String, value: Value) extends Bool
case class IsNULL(name: String) extends Bool
case class Between(name: String, lhs: Value, rhs: Value) extends Bool
case class And(lhs: Bool, rhs: Bool) extends Bool
case class Or(lhs: Bool, rhs: Bool) extends Bool
case class Where(condition: Bool)