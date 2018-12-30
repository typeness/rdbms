package io.github.typeness.rdbms

import cats.instances.list._
import cats.instances.either._
import cats.syntax.traverse._
import cats.syntax.either._
import SQLError.EitherSQLError

object QueryBuilder extends BuilderUtils {

  private case class JoinWithRelation(join: Join, relation: Relation)

  def makeQuery(query: Query, schema: Schema): Either[SQLError, List[Row]] = query match {
    case sel: Select          => select(sel, schema)
    case union: Union         => unionSelect(union, schema)
    case unionAll: UnionAll   => unionAllSelect(unionAll, schema)
    case intersect: Intersect => intersectSelect(intersect, schema)
    case except: Except       => exceptSelect(except, schema)
  }

  private def unionSelect(union: Union, schema: Schema): Either[SQLError, List[Row]] = {
    val leftEither = makeQuery(union.left, schema)
    val rightEither = makeQuery(union.right, schema)
    for {
      left <- leftEither
      right <- rightEither
    } yield left.toSet.union(right.toSet).toList
  }

  private def unionAllSelect(unionAll: UnionAll, schema: Schema): Either[SQLError, List[Row]] = {
    val leftEither = makeQuery(unionAll.left, schema)
    val rightEither = makeQuery(unionAll.right, schema)
    for {
      left <- leftEither
      right <- rightEither
    } yield left ::: right
  }

  private def intersectSelect(intersect: Intersect, schema: Schema): Either[SQLError, List[Row]] = {
    val leftEither = makeQuery(intersect.left, schema)
    val rightEither = makeQuery(intersect.right, schema)
    for {
      left <- leftEither
      right <- rightEither
    } yield left.toSet.intersect(right.toSet).toList
  }

  private def exceptSelect(except: Except, schema: Schema): Either[SQLError, List[Row]] = {
    val leftEither = makeQuery(except.left, schema)
    val rightEither = makeQuery(except.right, schema)

    for {
      left <- leftEither
      right <- rightEither
    } yield left.toSet.diff(right.toSet).toList
  }

  private def select(query: Select, schema: Schema): Either[SQLError, List[Row]] =
    for {
      relation <- schema.getRelation(query.from)
      //    _ <- checkUndefinedNames(query.projection, relation.heading.map(_.name))
      joined <- makeJoins(relation, query.joins, schema)
      filteredRows <- filterRows(joined, query.condition)
      groupedRows <- groupBy(query.groupBy, filteredRows, query.getAggregates)
      sortedRows <- sortRows(groupedRows, query.order)
      selectedColumns = project(query.projection, sortedRows)
    } yield if (query.distinct) selectedColumns.distinct else selectedColumns

  private def project(exprs: List[Expression], rows: List[Row]): List[Row] = exprs match {
    case Nil =>
      rows
    case _ =>
      val literals = exprs.collect {
        case lit: Literal => BodyAttribute(lit.value.toString, lit)
      }
      val names = exprs.collect {
        case Var(name)            => name
        case aggregate: Aggregate => aggregate.toString
      }
      rows.map(row => Row(row.projectMany(names).attributes ::: literals))
  }

  private def filterRows(rows: List[Row], condition: Option[Bool]): Either[SQLError, List[Row]] =
    condition match {
      case None       => Right(rows)
      case Some(cond) => BoolInterpreter.eval(cond, rows)
    }

  private def sortRows(rows: List[Row], order: List[Order]): Either[SQLError, List[Row]] = {
    def sortRowsWithKey(rows: List[(Literal, Row)], condition: Int => Boolean): List[Row] =
      rows
        .sortWith {
          case ((key1, _), (key2, _)) => condition(Literal.compareUnsafe(key1, key2))
        }
        .map {
          case (_, row) => row
        }
    order match {
      case Nil =>
        Right(rows)
      case _ =>
        order.foldRight[Either[SQLError, List[Row]]](Right(rows))((order, rows) => {
          rows.flatMap {
            rowList =>
              val name = order.name
              val rowsWithKey = rowList
                .map(
                  row =>
                    Either.fromOption(
                      row.project(name).map(key => (key.literal, row)),
                      ColumnDoesNotExists(name)
                  ))
                .sequence[EitherSQLError, (Literal, Row)]
              order match {
                case Descending(_) =>
                  rowsWithKey.map(sortRowsWithKey(_, _ >= 0))
                case Ascending(_) =>
                  rowsWithKey.map(sortRowsWithKey(_, _ <= 0))
              }
          }
        })
    }
  }

  private def makeJoins(relation: Relation,
                        joins: List[Join],
                        schema: Schema): Either[SQLError, List[Row]] = {
    val joinsWithRelation =
      joins.map(join => schema.getRelation(join.name).map(JoinWithRelation(join, _)))
    joinsWithRelation.sequence[EitherSQLError, JoinWithRelation].flatMap(foldJoins(relation, _))
  }

  private def foldJoins(left: Relation,
                        joins: List[JoinWithRelation]): Either[SQLError, List[Row]] =
    joins match {
      case Nil => Right(left.body)
      case JoinWithRelation(join, relation) :: Nil =>
        makeJoin(left, relation, join)
      case JoinWithRelation(join, relation) :: tail =>
        makeJoin(left, relation, join).flatMap(result =>
          foldJoins(Relation("", Nil, None, left.heading ::: relation.heading, result), tail))
    }

  private def makeJoin(left: Relation, right: Relation, join: Join): Either[SQLError, List[Row]] = {
    val pairs = for {
      first <- left.body
      second <- right.body
    } yield (first, second)
    val crossProduct = pairs.map {
      case (first, second) => Row(first.attributes ::: second.attributes)
    }
    join match {
      case CrossJoin(_) =>
        Right(crossProduct)
      case InnerJoin(_, on) =>
        BoolInterpreter.eval(on, crossProduct)
      case LeftOuterJoin(_, on) =>
        val inner = BoolInterpreter.eval(on, crossProduct)
        makeOuterJoin(Nil, left.body, inner, right.heading, Nil)
      case RightOuterJoin(_, on) =>
        val inner = BoolInterpreter.eval(on, crossProduct)
        makeOuterJoin(left.heading, Nil, inner, Nil, right.body)
      case FullOuterJoin(_, on) =>
        val inner = BoolInterpreter.eval(on, crossProduct)
        makeOuterJoin(left.heading, left.body, inner, right.heading, right.body)
    }
  }

  private def makeOuterJoin(leftHeading: List[HeadingAttribute],
                            leftBody: List[Row],
                            innerJoin: Either[SQLError, List[Row]],
                            rightHeading: List[HeadingAttribute],
                            rightBody: List[Row]): Either[SQLError, List[Row]] = {

    def makeNULLRow(heading: List[HeadingAttribute]) =
      Row(heading.map(attribute => BodyAttribute(attribute.name, NULLLiteral)))

    sealed trait Side
    case object Left extends Side
    case object Right extends Side

    def joinWithNULL(outer: List[Row],
                     inner: List[Row],
                     nullRow: List[BodyAttribute],
                     side: Side) = {
      val innerOfOuter =
        inner.map(_.attributes.filterNot(a => nullRow.exists(_.name == a.name)))
      outer.map(_.attributes).filterNot(innerOfOuter.contains(_)).map { row =>
        side match {
          case Left  => Row(row ::: nullRow)
          case Right => Row(nullRow ::: row)
        }
      }
    }

    val leftNullRow = makeNULLRow(leftHeading)
    val rightNullRow = makeNULLRow(rightHeading)
    for {
      inner <- innerJoin
      leftJoin = joinWithNULL(leftBody, inner, rightNullRow.attributes, Left)
      rightJoin = joinWithNULL(rightBody, inner, leftNullRow.attributes, Right)
    } yield leftJoin ::: inner ::: rightJoin
  }

  case class Group(values: List[BodyAttribute]) extends AnyVal

  private def groupBy(names: List[String],
                      rows: List[Row],
                      aggregates: List[Aggregate]): Either[SQLError, List[Row]] = {
    def select(rows: List[Row], name: String): List[BodyAttribute] = rows.flatMap(_.project(name))
    def updateGroup(groups: Map[Group, List[Row]],
                    group: Group,
                    values: List[BodyAttribute]): Map[Group, List[Row]] =
      groups.get(group) match {
        case Some(values1) => groups.updated(group, Row(values) :: values1)
        case None          => groups + (group -> List(Row(values)))
      }
    def aggregate(groupRow: List[Row]): EitherSQLError[List[BodyAttribute]] =
      aggregates.traverse[EitherSQLError, BodyAttribute] { function =>
        val arguments = select(groupRow, function.argument).map(_.literal)
        function.eval(arguments).map(literal => BodyAttribute(function.toString, literal))
      }

    if (names.isEmpty && aggregates.isEmpty) Right(rows)
    else if (names.isEmpty && aggregates.nonEmpty) aggregate(rows).map(row => List(Row(row)))
    else {
      val grouped = rows.foldLeft(Map[Group, List[Row]]()) {
        case (groups, row) =>
          val (group, values) = row.attributes.partition {
            case attribute: BodyAttribute if names.contains(attribute.name) => true
            case _                                                          => false
          }
          updateGroup(groups, Group(group), values)
      }
      grouped.toList.traverse[EitherSQLError, Row] {
        case (group, groupRow) =>
          val aggregateResult = aggregate(groupRow)
          aggregateResult.map { result =>
            Row(group.values ::: result)
          }
      }
    }
  }
}
