package io.github.typeness.rdbms

sealed trait Projection

case class Var(name: String) extends Projection

sealed trait Literal extends Projection {
  def typeOf: AnyType
  def show: String
}

case class IntegerLiteral(value: Int) extends Literal {
  override def typeOf: AnyType = IntegerType
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

sealed trait Aggregate extends Projection {
  def eval(literals: List[Literal]): Either[TypeMismatch, Literal]
  def argument: String
}
case class Sum(argument: String) extends Aggregate {
  override def eval(literals: List[Literal]): Either[TypeMismatch, IntegerLiteral] = {
    val (nonInts, ints) = literals.partition {
      case _: IntegerLiteral => false
    }
    if (nonInts.nonEmpty) Left(TypeMismatch(nonInts.head.typeOf, IntegerType, nonInts.head))
    // find a way to avoid using isInstanceOf
    else Right(IntegerLiteral(ints.asInstanceOf[List[IntegerLiteral]].map(_.value).sum))
  }
}
case class Avg(argument: String) extends Aggregate {
  override def eval(literals: List[Literal]): Either[TypeMismatch, IntegerLiteral] = {
    for {
      sum <- Sum(argument).eval(literals)
      count <- Count(argument).eval(literals)
    } yield IntegerLiteral(sum.value / count.value)
  }
}
case class Count(argument: String) extends Aggregate {
  override def eval(literals: List[Literal]): Either[TypeMismatch, IntegerLiteral] =
    Right(IntegerLiteral(literals.size))
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
}
