package io.github.typeness.rdbms

import upickle.default._

sealed trait Projection {
  def show: String
  def toAttributeName: AttributeName
}

object Projection {
  implicit val projectionReadWriter: ReadWriter[Projection] = ReadWriter.merge(
    macroRW[Var],
    macroRW[Accessor],
    macroRW[Alias],
    Literal.literalReadWriter
  )
}

case class Var(name: AttributeName) extends Projection {
  override def show: String = name.value
  override def toAttributeName: AttributeName = name
}

case class Accessor(prefix: RelationName, name: AttributeName) extends Projection {
  override def show: String = str"${prefix.value}.${name.value}"
  def toAttributeName: AttributeName = AttributeName(str"${prefix.value}.${name.value}")
}

case class Alias(original: Projection, alias: AttributeName) extends Projection {
  override def show: String = alias.value
  override def toAttributeName: AttributeName = alias
}

sealed trait Literal extends Projection {
  def typeOf: AnyType
  def show: String
}

sealed trait NumericLiteral extends Literal

object NumericLiteral {
  implicit val numericLiteral: ReadWriter[NumericLiteral] = ReadWriter.merge(
    macroRW[IntegerLiteral],
    macroRW[RealLiteral],
  )
}

case class IntegerLiteral(value: Int) extends NumericLiteral {
  override def typeOf: AnyType = IntegerType
  override def show: String = value.toString
  override def toAttributeName: AttributeName = AttributeName(value.toString)
}

case class RealLiteral(value: Double) extends NumericLiteral {
  override def typeOf: AnyType = RealType
  override def show: String = value.toString
  override def toAttributeName: AttributeName = AttributeName(value.toString)
}

case class StringLiteral(value: String) extends Literal {
  override def typeOf: AnyType = NVarCharType(1)
  override def show: String = str"'$value'"
  override def toAttributeName: AttributeName = AttributeName(str"'$value'")
}

case class DateLiteral(value: String) extends Literal {
  override def typeOf: AnyType = DateType
  override def show: String = str"'$value'"
  override def toAttributeName: AttributeName = AttributeName(str"'$value'")
}

case class MoneyLiteral(value: Int) extends Literal {
  override def typeOf: AnyType = MoneyType
  override def show: String = value.toString
  override def toAttributeName: AttributeName = AttributeName(value.toString)
}

case object NULLLiteral extends Literal {
  override def typeOf: AnyType = NullType
  override def show: String = "NULL"
  override def toAttributeName: AttributeName = AttributeName("NULL")
}

object Literal {

  implicit val literalReadWriter: ReadWriter[Literal] = ReadWriter.merge(
    NumericLiteral.numericLiteral,
    macroRW[StringLiteral],
    macroRW[DateLiteral],
    macroRW[MoneyLiteral],
    macroRW[NULLLiteral.type],
  )

  def compare[A >: Literal](lhs: Literal, rhs: Literal): Either[SQLError, Int] = {
    if (lhs.typeOf != rhs.typeOf) Left(TypeMismatch(lhs.typeOf, rhs.typeOf, lhs))
    // find a better way to do this?
    else Right(compareUnsafe(lhs, rhs))
  }

  def compareUnsafe(lhs: Literal, rhs: Literal): Int = {
    lhs.typeOf match {
      case IntegerType =>
        comparison(lhs.asInstanceOf[IntegerLiteral], rhs.asInstanceOf[IntegerLiteral])
      case RealType =>
        comparison(lhs.asInstanceOf[RealLiteral], rhs.asInstanceOf[RealLiteral])
      case DateType =>
        comparison(StringLiteral(lhs.asInstanceOf[DateLiteral].value),
                   StringLiteral(rhs.asInstanceOf[DateLiteral].value))
      case NVarCharType(_) =>
        comparison(lhs.asInstanceOf[StringLiteral], rhs.asInstanceOf[StringLiteral])
      case NullType => -1
      case MoneyType =>
        comparison(IntegerLiteral(lhs.asInstanceOf[MoneyLiteral].value),
                   IntegerLiteral(rhs.asInstanceOf[MoneyLiteral].value))
      case BitType =>
        comparison(lhs.asInstanceOf[IntegerLiteral], rhs.asInstanceOf[IntegerLiteral])
      case CharType(_)       => ???
      case DecimalType(_, _) => ???
      case TinyIntType       => ???
      case ImageType         => ???
      case NTextType         => ???
    }
  }
  def reverseCompare(lhs: Literal, rhs: Literal): Either[SQLError, Int] = compare(lhs, rhs).map(-_)

  def comparison(lhs: IntegerLiteral, rhs: IntegerLiteral): Int =
    implicitly[Ordering[Int]].compare(lhs.value, rhs.value)
  def comparison(lhs: StringLiteral, rhs: StringLiteral): Int =
    implicitly[Ordering[String]].compare(lhs.value, rhs.value)
  def comparison(lhs: DateLiteral, rhs: DateLiteral): Int =
    implicitly[Ordering[String]].compare(lhs.value, rhs.value)
  def comparison(lhs: RealLiteral, rhs: RealLiteral): Int =
    Ordering.Double.TotalOrdering.compare(lhs.value, rhs.value)
}

sealed trait Aggregate extends Projection {
  def eval(literals: List[Literal]): Either[TypeMismatch, Literal]
  def argument: String
}
case class Sum(argument: String) extends Aggregate {
  override def eval(literals: List[Literal]): Either[TypeMismatch, NumericLiteral] = {
    val (nonNumerics, numerics) = literals.partition {
      case _: IntegerLiteral => false
      case _: RealLiteral    => false
      case _                 => true
    }
    val ints = numerics.collect {
      case d: IntegerLiteral => d
    }
    val reals = numerics.collect {
      case d: RealLiteral => d
    }
    if (nonNumerics.nonEmpty)
      Left(TypeMismatch(nonNumerics.head.typeOf, IntegerType, nonNumerics.head))
    else if (ints.nonEmpty && reals.nonEmpty) Left(TypeMismatch(RealType, IntegerType, ints.head))
    else if (ints.nonEmpty) Right(IntegerLiteral(ints.map(_.value).sum))
    else Right(RealLiteral(reals.map(_.value).sum))
  }

  override def show: String = str"Sum($argument)"

  override def toAttributeName: AttributeName = AttributeName(str"Sum($argument)")
}
case class Avg(argument: String) extends Aggregate {
  override def eval(literals: List[Literal]): Either[TypeMismatch, RealLiteral] = {
    for {
      sum <- Sum(argument).eval(literals)
      count <- Count(argument).eval(literals)
    } yield
      sum match {
        case IntegerLiteral(value) => RealLiteral(value.toDouble / count.value)
        case RealLiteral(value)    => RealLiteral(value / count.value)
      }
  }
  override def show: String = str"Avg($argument)"

  override def toAttributeName: AttributeName = AttributeName(str"Avg($argument)")
}
case class Count(argument: String) extends Aggregate {
  override def eval(literals: List[Literal]): Either[TypeMismatch, IntegerLiteral] =
    Right(IntegerLiteral(literals.size))
  override def show: String = str"Count($argument)"

  override def toAttributeName: AttributeName = AttributeName(str"Count($argument)")
}
case class Max(argument: String) extends Aggregate {
  override def eval(literals: List[Literal]): Either[TypeMismatch, Literal] =
    Right(
      literals
        .sortWith {
          case (left, right) => Literal.compareUnsafe(left, right) > 0
        }
        .headOption
        .getOrElse(NULLLiteral))
  override def show: String = str"Max($argument)"

  override def toAttributeName: AttributeName = AttributeName(str"Max($argument)")
}
case class Min(argument: String) extends Aggregate {
  override def eval(literals: List[Literal]): Either[TypeMismatch, Literal] =
    Right(
      literals
        .sortWith {
          case (left, right) => Literal.compareUnsafe(left, right) < 0
        }
        .headOption
        .getOrElse(NULLLiteral))
  override def show: String = str"Min($argument)"

  override def toAttributeName: AttributeName = AttributeName(str"Min($argument)")
}

case class Plus(left: Projection, right: Projection) extends Projection {
  val calc: (Double, Double) => Double = (a, b) => a + b
  def show: String = str"${left.show}+${right.show}"

  override def toAttributeName: AttributeName = AttributeName(str"${left.show}+${right.show}")
}

case class Minus(left: Projection, right: Projection) extends Projection {
  val calc: (Double, Double) => Double = (a, b) => a - b
  def show: String = str"${left.show}-${right.show}"

  override def toAttributeName: AttributeName = AttributeName(str"${left.show}-${right.show}")
}

case class Multiplication(left: Projection, right: Projection) extends Projection {
  val calc: (Double, Double) => Double = (a, b) => a * b
  def show: String = str"${left.show}*${right.show}"

  override def toAttributeName: AttributeName = AttributeName(str"${left.show}*${right.show}")
}
