package io.github.typeness.rdbms

import io.github.typeness.rdbms.BoolInterpreter.getLiteral

object ArithmeticInterpreter {

  private def getNumericLiteral(projection: Projection,
                                row: Row): Either[SQLError, NumericLiteral] =
    getLiteral(projection, row).flatMap {
      case literal: NumericLiteral => Right(literal)
      case str: StringLiteral      => Left(TypeMismatch(IntegerType, NVarCharType(32), str))
      case date: DateLiteral       => Left(TypeMismatch(IntegerType, DateType, date))
      case money: MoneyLiteral     => Left(TypeMismatch(IntegerType, MoneyType, money))
      case NULLLiteral             => Left(TypeMismatch(IntegerType, NullType, NULLLiteral))
    }

  def calculate(left: Projection,
                right: Projection,
                row: Row,
                calc: (Double, Double) => Double): Either[SQLError, NumericLiteral] =
    for {
      leftResult <- getNumericLiteral(left, row)
      rightResult <- getNumericLiteral(right, row)
      result <- (leftResult, rightResult) match {
        case (RealLiteral(l), RealLiteral(r)) => Right(RealLiteral(calc(l, r)))
        case (IntegerLiteral(l), IntegerLiteral(r)) =>
          Right(IntegerLiteral(calc(l.toDouble, r.toDouble).toInt))
        case (IntegerLiteral(l), RealLiteral(r)) => Right(RealLiteral(calc(l.toDouble, r)))
        case (RealLiteral(l), IntegerLiteral(r)) => Right(RealLiteral(calc(l.toDouble, r.toDouble)))
      }
    } yield result

}
