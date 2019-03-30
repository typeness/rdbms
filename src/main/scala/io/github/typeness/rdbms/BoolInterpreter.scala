package io.github.typeness.rdbms

import cats.instances.list._
import cats.instances.either._
import cats.syntax.traverse._

object BoolInterpreter {
  def eval(expression: Bool, rows: List[Row]): Either[SQLError, List[Row]] =
    expression match {
      case Not(value) =>
        val complement = eval(value, rows)
        complement.map(rows.diff(_))
      case Equals(lhs, rhs)          => filter(lhs, rhs, x => x == 0, rows)
      case Greater(lhs, rhs)         => filter(lhs, rhs, x => x > 0, rows)
      case GreaterOrEquals(lhs, rhs) => filter(lhs, rhs, x => x >= 0, rows)
      case Less(lhs, rhs)            => filter(lhs, rhs, x => x < 0, rows)
      case LessOrEquals(lhs, rhs)    => filter(lhs, rhs, x => x <= 0, rows)
      case IsNotNULL(name)           => eval(Not(IsNULL(name)), rows)
      case IsNULL(name) =>
        def isNull(attribute: BodyAttribute) = attribute match {
          case BodyAttribute(_, NULLLiteral) => true
          case _                             => false
        }
        val filtered = rows.traverse { row =>
          val attribute = row.projectEither(name)
          attribute.map(isNull).map {
            case true  => Some(row)
            case false => None
          }
        }
        filtered.map(_.flatten)
      case Between(name, lhs, rhs) =>
        val greaterOrEquals = GreaterOrEquals(Var(name), lhs)
        val lessOrEquals = LessOrEquals(Var(name), rhs)
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
      case Like(id, text) =>
        val regex = sqlLikeToRegex(text)
        def rowMatches(row: Row): Either[SQLError, Option[Row]] =
          row.projectEither(id).flatMap { attribute =>
            attribute.literal match {
              case StringLiteral(value) =>
                if (value.toLowerCase.matches(regex.toLowerCase)) Right(Some(row))
                else Right(None)
              case lit =>
                Left(TypeMismatch(lit.typeOf, NVarCharType(256), lit))
            }
          }
        rows
          .traverse(rowMatches)
          .map(_.flatten)
    }

  private def sqlLikeToRegex(like: String): String = like.flatMap {
    case '_' => '.' :: Nil
    case '%' => '.' :: '*' :: Nil
    case a   => a :: Nil
  }

  private def filter(left: Projection,
                     right: Projection,
                     condition: Int => Boolean,
                     rows: List[Row]): Either[SQLError, List[Row]] = {
    val filtered: List[Either[SQLError, Option[Row]]] = rows.map { row =>
      for {
        lhs <- getLiteral(left, row)
        rhs <- getLiteral(right, row)
        conditionResult = Literal.compare(lhs, rhs).map(condition).contains(true)
      } yield if (conditionResult) Some(row) else None
    }
    filtered.sequence.map(_.flatten)
  }

  def getLiteral(expression: Projection, row: Row): Either[SQLError, Literal] =
    expression match {
      case Alias(proj, _)  =>
        getLiteral(proj, row)
      case agg: Aggregate =>
        agg.eval(row.getValues)
      case Var(name) =>
        row.projectEither(name).map(_.literal)
      case literal: Literal =>
        Right(literal)
      case mult: Multiplication =>
        ArithmeticInterpreter.calculate(mult.left, mult.right, row, (a, b) => a * b)
      case plus: Plus =>
        ArithmeticInterpreter.calculate(plus.left, plus.right, row, (a, b) => a + b)
      case minus: Minus =>
        ArithmeticInterpreter.calculate(minus.left, minus.right, row, (a, b) => a - b)
    }

}
