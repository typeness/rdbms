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
  override def typeOf: AnyType = NVarCharType(1)
}

case class DateLiteral(value: String) extends Literal {
  override def typeOf: AnyType = DateType
}

case object NULLLiteral extends Literal {
  override def typeOf: AnyType = NullType
}

object Literal {

  def compare[A >: Literal](lhs: Literal, rhs: Literal): Either[SQLError, Int] = {
    if (lhs.typeOf != rhs.typeOf) Left(TypeMismatch(lhs.typeOf, rhs.typeOf))
    // find a better way to do this?
    else Right(compareUnsafe(lhs, rhs))
  }

  def compareUnsafe(lhs: Literal, rhs: Literal): Int = {
    lhs.typeOf match {
      case IntegerType =>
        comparison(lhs.asInstanceOf[IntegerLiteral], rhs.asInstanceOf[IntegerLiteral])
      case DateType =>
        comparison(lhs.asInstanceOf[StringLiteral], rhs.asInstanceOf[StringLiteral])
      case NVarCharType(_) =>
        comparison(lhs.asInstanceOf[StringLiteral], rhs.asInstanceOf[StringLiteral])
      case NullType => -1
      case MoneyType =>
        comparison(lhs.asInstanceOf[IntegerLiteral], rhs.asInstanceOf[IntegerLiteral])
      case BitType =>
        comparison(lhs.asInstanceOf[IntegerLiteral], rhs.asInstanceOf[IntegerLiteral])
    }
  }
  def reverseCompare(lhs: Literal, rhs: Literal): Either[SQLError, Int] = compare(lhs, rhs).map(-_)

  def comparison(lhs: IntegerLiteral, rhs: IntegerLiteral): Int =
    implicitly[Ordering[Int]].compare(lhs.value, rhs.value)
  def comparison(lhs: StringLiteral, rhs: StringLiteral): Int =
    implicitly[Ordering[String]].compare(lhs.value, rhs.value)
  def comparison(lhs: DateLiteral, rhs: DateLiteral): Int =
    implicitly[Ordering[String]].compare(lhs.value, rhs.value)
  def comparison(lhs: NULLLiteral.type, rhs: NULLLiteral.type): Int = -1
}

