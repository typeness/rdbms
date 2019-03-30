package io.github.typeness.rdbms

sealed trait Projection {
  def show: String
}

case class Var(name: String) extends Projection {
  override def show: String = name
}

case class Accessor(prefix: String, name: String) extends Projection {
  override def show: String = s"$prefix.$name"
}

case class Alias(original: Projection, alias: String) extends Projection {
  override def show: String = alias
}

sealed trait Literal extends Projection {
  def typeOf: AnyType
  def show: String
}

sealed trait NumericLiteral extends Literal

case class IntegerLiteral(value: Int) extends NumericLiteral {
  override def typeOf: AnyType = IntegerType
  override def show: String = value.toString
}

case class RealLiteral(value: Double) extends NumericLiteral {
  override def typeOf: AnyType = RealType
  override def show: String = value.toString
}

case class StringLiteral(value: String) extends Literal {
  override def typeOf: AnyType = NVarCharType(1)
  override def show: String = value
}

case class DateLiteral(value: String) extends Literal {
  override def typeOf: AnyType = DateType
  override def show: String = value
}

case class MoneyLiteral(value: Int) extends Literal {
  override def typeOf: AnyType = MoneyType
  override def show: String = value.toString
}

case object NULLLiteral extends Literal {
  override def typeOf: AnyType = NullType
  override def show: String = "NULL"
}

object Literal {

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
  def comparison(lhs: NULLLiteral.type, rhs: NULLLiteral.type): Int = -1
  def comparison(lhs: RealLiteral, rhs: RealLiteral): Int =
    implicitly[Ordering[Double]].compare(lhs.value, rhs.value)
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

  override def show: String = s"Sum($argument)"
}
case class Avg(argument: String) extends Aggregate {
  override def eval(literals: List[Literal]): Either[TypeMismatch, RealLiteral] = {
    for {
      sum <- Sum(argument).eval(literals)
      count <- Count(argument).eval(literals)
    } yield
      sum match {
        case IntegerLiteral(value) => RealLiteral(value / count.value)
        case RealLiteral(value)    => RealLiteral(value / count.value)
      }
  }
  override def show: String = s"Avg($argument)"
}
case class Count(argument: String) extends Aggregate {
  override def eval(literals: List[Literal]): Either[TypeMismatch, IntegerLiteral] =
    Right(IntegerLiteral(literals.size))
  override def show: String = s"Count($argument)"
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
  override def show: String = s"Max($argument)"
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
  override def show: String = s"Min($argument)"
}

case class Plus(left: Projection, right: Projection) extends Projection {
  val calc: (Double, Double) => Double = (a, b) => a + b
  def show: String = s"${left.show}+${right.show}"
}

case class Minus(left: Projection, right: Projection) extends Projection {
  val calc: (Double, Double) => Double = (a, b) => a - b
  def show: String = s"${left.show}-${right.show}"
}

case class Multiplication(left: Projection, right: Projection) extends Projection {
  val calc: (Double, Double) => Double = (a, b) => a * b
  def show: String = s"${left.show}*${right.show}"
}
