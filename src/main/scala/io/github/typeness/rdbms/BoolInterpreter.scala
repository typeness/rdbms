package io.github.typeness.rdbms

import cats.syntax.either._
import cats.instances.list._
import cats.instances.either._
import cats.syntax.traverse._
import io.github.typeness.rdbms.SQLError.EitherSQLError

object BoolInterpreter {
  def eval(expression: Bool, rows: List[Row]): Either[SQLError, List[Row]] =
    expression match {
      case Equals(name, rhs)          => filter(Var(name), rhs, x => x == 0, rows)
      case Greater(name, rhs)         => filter(Var(name), rhs, x => x > 0, rows)
      case GreaterOrEquals(name, rhs) => filter(Var(name), rhs, x => x >= 0, rows)
      case Less(name, rhs)            => filter(Var(name), rhs, x => x < 0, rows)
      case LessOrEquals(name, rhs)    => filter(Var(name), rhs, x => x <= 0, rows)
      case IsNULL(name) =>
        def isNull(attribute: BodyAttribute) = attribute match {
          case BodyAttribute(_, NULLLiteral) => true
          case _                             => false
        }
        val filtered: List[Either[SQLError, Option[Row]]] = rows.map { row =>
          val attribute = row.select(name)
          Either.fromOption(attribute, ColumnDoesNotExists(name)).map(isNull).map {
            case true  => Some(row)
            case false => None
          }
        }
        filtered.sequence[EitherSQLError, Option[Row]].map(_.flatten)
      case Between(name, lhs, rhs) =>
        val greaterOrEquals = GreaterOrEquals(name, lhs)
        val lessOrEquals = LessOrEquals(name, rhs)
        eval(And(greaterOrEquals, lessOrEquals), rows)
      case And(lhs, rhs) =>
        for {
          left <- eval(lhs, rows)
          intersection <- eval(rhs, left)
        } yield intersection
      case Or(lhs, rhs) =>
        for {
          left <- eval(lhs, rows)
          right <- eval(rhs, rows)
        } yield left.toSet.union(right.toSet).toList
    }

  def filter(left: Expression,
             right: Expression,
             condition: Int => Boolean,
             rows: List[Row]): Either[SQLError, List[Row]] = {
    val filtered: List[Either[SQLError, Option[Row]]] = rows.map { row =>
      for {
        lhs <- getLiteral(left, row)
        rhs <- getLiteral(right, row)
        conditionResult = Literal.compare(lhs, rhs).map(condition).contains(true)
      } yield if (conditionResult) Some(row) else None
    }
    filtered.sequence[EitherSQLError, Option[Row]].map(_.flatten)
  }

  private def getLiteral(expression: Expression, row: Row): Either[ColumnDoesNotExists, Literal] =
    expression match {
      case Var(name) =>
        Either.fromOption(row.select(name), ColumnDoesNotExists(name)).map(_.literal)
      case literal: Literal =>
        Right(literal)
    }

}
