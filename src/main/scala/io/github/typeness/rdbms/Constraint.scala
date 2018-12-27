package io.github.typeness.rdbms

sealed trait Constraint
case object Unique extends Constraint
case object NotNULL extends Constraint
case object NULL extends Constraint
case object PrimaryKey extends Constraint
case class Default(value: Literal) extends Constraint
case class Check(bool: Bool) extends Constraint
case class ForeignKey(name: String, schema: String) extends Constraint
