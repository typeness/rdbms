package io.github.typeness.rdbms

import Relation._

object BoolInterpreter {
  def eval(expression: Bool, rows: List[Row]): Either[SQLError, List[Row]] = expression match {
    case Equals(name, rhs) => Right(
      rows.filter { row =>
        Row.select(name, row).exists {
          attribute =>
            rhs match {
              case literal: Literal =>
                attribute.literal == literal
              case Var(name2) =>
                val attribute2 = Row.select(name2, row)
                attribute2.exists(_.literal == attribute.literal)
            }
        }
      }
    )
    case GreaterOrEquals(name, value) => ???
    case LessOrEquals(name, value) => ???
    case IsNULL(name) => Right(rows.filter { row =>
      val attribute = Row.select(name, row)
      attribute match {
        case Some(BodyAttribute(_, NULLLiteral)) => true
        case _ => false
      }
    })
    case Between(name, lhs, rhs) => ???
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
}
