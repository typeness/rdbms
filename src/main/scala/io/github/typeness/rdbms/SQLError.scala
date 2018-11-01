package io.github.typeness.rdbms

sealed trait SQLError
case object MultiplePrimaryKeys extends SQLError
case class MultipleColumnNames(name: String) extends SQLError
case class ColumnDoesNotExists(name: String) extends SQLError
case class MissingColumnName(name: String) extends SQLError
case object WrongNumberOfAttributes extends SQLError
case class IdentityViolation(name: String) extends SQLError
case class SchemaDoesNotExists(name: String) extends SQLError
