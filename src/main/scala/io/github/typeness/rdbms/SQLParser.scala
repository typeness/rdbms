package io.github.typeness.rdbms

import fastparse._
import NoWhitespace._

object SQLParser {

  def parse(source: String): Parsed[SQL] = fastparse.parse(source, sql(_))

  private def sql[_: P]: P[SQL] =
    P((insert | delete | update | query | create) ~ End)

  private def query[_: P]: P[Query] =
    P(
      select ~ (space ~ (IgnoreCase("UNION") | IgnoreCase("INTERSECT") |
        IgnoreCase("EXCEPT") | IgnoreCase("UNION ALL")).! ~ space ~ select).rep).map {
      case (left, Nil) =>
        left
      case (left, queries) =>
        queries.foldLeft(left: Query) {
          case (lhs, ("UNION", rhs))        => Union(lhs, rhs)
          case (lhs, ("INTERSECT", rhs))    => Intersect(lhs, rhs)
          case (lhs, ("UNION ALL", rhs))    => UnionAll(lhs, rhs)
          case (lhs, (_ /*"EXCEPT"*/, rhs)) => Except(lhs, rhs)
        }
    }

  private def insert[_: P] =
    P(IgnoreCase("INSERT INTO") ~ space ~ id ~ space ~ ids.? ~ IgnoreCase("VALUES") ~ space ~ row ~ End)
      .map {
        case (name, None, row) =>
          AnonymousInsert(name, row)
        case (name, Some(ids), row) =>
////        if (ids.size != row.size)
////          Fail("Number of identifiers do not match number of attributes")
////        else
          NamedInsert(name, Row(ids.zip(row).map(attr => BodyAttribute(attr._1, attr._2))))
      }

  private def delete[_: P]: P[Delete] =
    P(IgnoreCase("DELETE FROM") ~ space ~ id ~ space ~ where).map {
      case (name, bool) => Delete(name, Some(bool))
    }

  private def select[_: P]: P[Select] =
    P(
      IgnoreCase("SELECT") ~ space ~ IgnoreCase("DISTINCT").!.? ~ space ~ selectList ~ space ~
        IgnoreCase("FROM") ~ space ~ id ~ space ~ join ~ (space ~ where).? ~
        (space ~ groupBy).? ~ space ~ order.?
    ).map {
      case (distinct, proj, name, join, cond, group, order) =>
        Select(proj,
               name,
               join,
               cond,
               group.getOrElse(Nil),
               order.getOrElse(Nil),
               distinct.isDefined)
    }

  private def create[_: P]: P[Create] =
    P(IgnoreCase("CREATE TABLE") ~ space ~ id ~ "(" ~ headingAttribute.rep(sep = ", ") ~ ")").map {
      case (name, attributes) => Create(name, attributes.toList, Nil, None)
    }

  private def headingAttribute[_: P]: P[HeadingAttribute] =
    P(id ~ space ~ typeOfAttribute ~ (space ~ constraint).rep).map {
      case (name, typeOf, properties) => HeadingAttribute(name, typeOf, properties.toList)
    }

  private def order[_: P]: P[List[Order]] =
    P(
      IgnoreCase("ORDER BY") ~ space ~
        (id ~ space ~ (IgnoreCase("ASC") | IgnoreCase("DESC")).!).rep(min = 1, sep = ", ")
    ).map { x =>
      x.map {
        case (name, "ASC") => Ascending(name)
        case (name, _)     => Descending(name)
      }.toList
    }

  private def where[_: P]: P[Bool] =
    P(IgnoreCase("WHERE") ~ space ~ or)

  private def groupBy[_: P]: P[List[String]] =
    P(IgnoreCase("GROUP BY") ~ space ~ id.rep(min = 1, sep = ", ").map(_.toList))

  private def join[_: P]: P[List[Join]] =
    P(crossJoin | innerJoin | leftOuterJoin | rightOuterJoin | fullOuterJoin)
      .rep(sep = " ")
      .map(_.toList)

  private def crossJoin[_: P]: P[CrossJoin] =
    P(IgnoreCase("CROSS JOIN") ~ space ~ id).map(CrossJoin)

  private def innerJoin[_: P]: P[InnerJoin] =
    P(
      (IgnoreCase("INNER") ~ space).? ~ IgnoreCase("JOIN") ~ space ~ id ~ space
        ~ IgnoreCase("ON") ~ space ~ or)
      .map {
        case (name, on) => InnerJoin(name, on)
      }

  private def leftOuterJoin[_: P]: P[LeftOuterJoin] =
    P(IgnoreCase("LEFT OUTER JOIN") ~ space ~ id ~ space ~ IgnoreCase("ON") ~ space ~ or).map {
      case (name, on) => LeftOuterJoin(name, on)
    }

  private def rightOuterJoin[_: P]: P[RightOuterJoin] =
    P(IgnoreCase("RIGHT OUTER JOIN") ~ space ~ id ~ space ~ IgnoreCase("ON") ~ space ~ or).map {
      case (name, on) => RightOuterJoin(name, on)
    }

  private def fullOuterJoin[_: P]: P[FullOuterJoin] =
    P(IgnoreCase("FULL OUTER JOIN") ~ space ~ id ~ space ~ IgnoreCase("ON") ~ space ~ or).map {
      case (name, on) => FullOuterJoin(name, on)
    }

  private def selectList[_: P]: P[List[Expression]] =
    P(
      "*".!.map(_ => List.empty) |
        (aggregate | id.map(Var) | literal).rep(1, sep = ", ").map(_.toList)
    )

  private def ids[_: P]: P[List[String]] =
    P("(" ~ id.rep(1, sep = ", ").map(_.toList) ~ ") ")

  private def row[_: P]: P[List[Literal]] =
    P("(" ~ literal.rep(1, sep = ", ").map(_.toList) ~ ")")

  private def literal[_: P]: P[Literal] =
    P(integer | `null` | date | string)

  private def string[_: P]: P[StringLiteral] =
    P("'" ~ CharIn("a-zA-Z").rep.! ~ "'").map(StringLiteral)

  private def integer[_: P]: P[IntegerLiteral] =
    P(CharPred(c => '0' <= c && c <= '9').rep(1).!).map(x => IntegerLiteral(x.toInt))

  private def date[_: P]: P[DateLiteral] =
    P(
      "'" ~ CharIn("0-9").rep(exactly = 4).!
        ~ "-" ~ CharIn("0-9").rep(exactly = 2).! ~ "-" ~ CharIn("0-9").rep(exactly = 2).! ~ "'"
    ).!.map(DateLiteral)

  private def id[_: P]: P[String] =
    P(
      aggregate.! |
      CharIn("a-zA-Z").rep(1).! |
      ("[" ~ CharIn("a-zA-Z ").rep(1) ~ "]").!
    )

  private def `null`[_: P]: P[NULLLiteral.type] =
    P(IgnoreCase("NULL")).map(_ => NULLLiteral)

  private def space[_: P]: P[Unit] = P(CharsWhileIn(" \r\n", 0))

  private def expression[_: P]: P[Expression] =
    P(id.map(Var) | literal)

  private def booleanOperator[_: P]: P[Bool] =
    P(equals | greaterOrEquals | lessOrEquals | less | greater | isNull | between)

  private def equals[_: P]: P[Equals] =
    P(id ~ space ~ "=" ~ space ~ expression).map {
      case (name, value) => Equals(name, value)
    }

  private def greaterOrEquals[_: P]: P[GreaterOrEquals] =
    P(id ~ space ~ ">=" ~ space ~ expression).map {
      case (name, value) => GreaterOrEquals(name, value)
    }

  private def lessOrEquals[_: P]: P[LessOrEquals] =
    P(id ~ space ~ "<=" ~ space ~ expression).map {
      case (name, value) => LessOrEquals(name, value)
    }

  private def less[_: P]: P[Less] =
    P(id ~ space ~ "<" ~ space ~ expression).map {
      case (name, value) => Less(name, value)
    }

  private def greater[_: P]: P[Greater] =
    P(id ~ space ~ ">" ~ space ~ expression).map {
      case (name, value) => Greater(name, value)
    }

  private def isNull[_: P]: P[IsNULL] =
    P(id ~ space ~ IgnoreCase("IS NULL")).map(IsNULL)

  private def between[_: P]: P[Between] =
    P(id ~ space ~ IgnoreCase("BETWEEN") ~ space ~ expression ~ space ~ IgnoreCase("AND") ~ space ~ expression)
      .map {
        case (name, value1, value2) => Between(name, value1, value2)
      }

  private def and[_: P]: P[Bool] =
    P(booleanOperator ~ (space ~ IgnoreCase("AND") ~ space ~ booleanOperator).rep).map {
      case (a, Nil) => a
      case (a, ops) => And(a, ops.reduceLeft(And))
    }

  private def or[_: P]: P[Bool] =
    P(and ~ (space ~ IgnoreCase("OR") ~ space ~ and).rep).map {
      case (a, Nil) => a
      case (a, ops) => Or(a, ops.reduceLeft(Or))
    }

  private def update[_: P]: P[Update] =
    P(
      IgnoreCase("UPDATE") ~ space ~ id ~ space ~
        IgnoreCase("SET") ~ space ~ updateList ~ (space ~ where).?).map {
      case (name, row, condition) => Update(name, row, condition)
    }

  private def updateList[_: P]: P[Row] =
    P((id ~ space ~ "=" ~ space ~ literal).rep(1, sep = ", ").map { x =>
      x.map {
        case (name, lit) => BodyAttribute(name, lit)
      }
    }).map(_.toList).map(new Row(_))

  private def typeOfAttribute[_: P]: P[AnyType] =
    P(
      IgnoreCase("INT").!.map(_ => IntegerType) |
        IgnoreCase("DATE").!.map(_ => DateType) |
        (IgnoreCase("NVARCHAR") ~ "(" ~ integer ~ ")").map(int => NVarCharType(int.value)) |
        IgnoreCase("MONEY").!.map(_ => MoneyType) |
        IgnoreCase("BIT").!.map(_ => BitType) |
        (IgnoreCase("CHAR") ~ "(" ~ integer ~ ")").map(int => CharType(int.value)) |
        (IgnoreCase("DECIMAL") ~ "(" ~ integer ~ "," ~ space ~ integer ~ ")").map {
          case (prec, scale) => DecimalType(prec.value, scale.value)
        } |
        IgnoreCase("TINYINT").!.map(_ => TinyIntType)
    )

  private def constraint[_: P]: P[Constraint] =
    P(
      IgnoreCase("UNIQUE").map(_ => Unique) |
        IgnoreCase("NOT NULL").map(_ => NotNULL) |
        IgnoreCase("NULL").map(_ => NULL) |
        IgnoreCase("PRIMARY KEY").map(_ => PrimaryKey) |
        (IgnoreCase("DEFAULT") ~ space ~ literal).map(Default) |
        (IgnoreCase("CHECK") ~ space ~ or).map(Check) |
        (IgnoreCase("FOREIGN KEY REFERENCES") ~ space ~ id ~ "(" ~ id ~ ")").map {
          case (name, schema) => ForeignKey(name, schema)
        }
    )

  private def aggregate[_: P]: P[Aggregate] =
    P(
      (IgnoreCase("SUM") ~ "(" ~ id ~ ")").map(Sum) |
        (IgnoreCase("AVG") ~ "(" ~ id ~ ")").map(Avg) |
        (IgnoreCase("COUNT") ~ "(" ~ id ~ ")").map(Count) |
        (IgnoreCase("MAX") ~ "(" ~ id ~ ")").map(Max) |
        (IgnoreCase("MIN") ~ "(" ~ id ~ ")").map(Min)
    )
}
