package io.github.typeness.rdbms

sealed trait Expression

case class Var(name: String) extends Expression

sealed trait Literal extends Expression {
  def typeOf: AnyType
}

case class IntegerLiteral(value: Int) extends Literal {
  override def typeOf: AnyType = IntegerType
}

case class StringLiteral(value: String) extends Literal {
  override def typeOf: AnyType = StringType
}

case class Date(value: String) extends Literal {
  override def typeOf: AnyType = DateType
}

case object NULLLiteral extends Literal {
  override def typeOf: AnyType = NullType
}