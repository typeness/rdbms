package io.github.typeness.rdbms

import Relation._

object BoolInterpreter {
  def eval(expression: Bool, rows: List[Row]): Either[SQLError, List[Row]] = expression match {
    case Equals(name, value) => Right(
      rows.filter { row =>
        val attribute = row.find(_.name == name)
        attribute match {
          case Some(BodyAttribute(_, `value`)) => true
          case _ => false
        }
      }
    )
    case GreaterOrEquals(name, value) => ???
    case LessOrEquals(name, value) => ???
    case IsNULL(name) => Right(rows.filter { row =>
      val attribute = row.find(_.name == name)
      attribute match {
        case Some(BodyAttribute(_, Value(NULLLiteral))) => true
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
