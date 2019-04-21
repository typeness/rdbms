package io.github.typeness.rdbms

import cats.instances.list._
import cats.instances.either._
import cats.syntax.traverse._
import cats.syntax.either._
import cats.syntax.foldable._

object QueryBuilder extends BuilderUtils {

  private case class JoinWithRelation(join: Join, relation: Relation)

  def run(query: Query, schema: Schema): Either[SQLError, List[Row]] = query match {
    case sel: Select          => select(sel, schema)
    case union: Union         => unionSelect(union, schema)
    case unionAll: UnionAll   => unionAllSelect(unionAll, schema)
    case intersect: Intersect => intersectSelect(intersect, schema)
    case except: Except       => exceptSelect(except, schema)
  }

  private def unionSelect(union: Union, schema: Schema): Either[SQLError, List[Row]] = {
    val leftEither = run(union.left, schema)
    val rightEither = run(union.right, schema)
    for {
      left <- leftEither
      right <- rightEither
    } yield left.toSet.union(right.toSet).toList
  }

  private def unionAllSelect(unionAll: UnionAll, schema: Schema): Either[SQLError, List[Row]] = {
    val leftEither = run(unionAll.left, schema)
    val rightEither = run(unionAll.right, schema)
    for {
      left <- leftEither
      right <- rightEither
    } yield left ::: right
  }

  private def intersectSelect(intersect: Intersect, schema: Schema): Either[SQLError, List[Row]] = {
    val leftEither = run(intersect.left, schema)
    val rightEither = run(intersect.right, schema)
    for {
      left <- leftEither
      right <- rightEither
    } yield left.toSet.intersect(right.toSet).toList
  }

  private def exceptSelect(except: Except, schema: Schema): Either[SQLError, List[Row]] = {
    val leftEither = run(except.left, schema)
    val rightEither = run(except.right, schema)

    for {
      left <- leftEither
      right <- rightEither
    } yield left.toSet.diff(right.toSet).toList
  }

  private def select(query: Select, schema: Schema): Either[SQLError, List[Row]] =
    for {
      schema0 <- applyRelationAlias(query, schema)
      relation <- if (query.alias.nonEmpty) schema0.getRelation(query.alias)
      else schema0.getRelation(query.from)
      //    _ <- checkUndefinedNames(query.projection, relation.heading.map(_.name))
      joined <- makeJoins(relation, query.joins, schema0)
      filteredRows <- filterRows(joined, query.where)
      groupedRows <- groupBy(query.groupBy, filteredRows, query.getAggregates)
      havingResult <- having(groupedRows, query.having)
      sortedRows <- sortRows(havingResult, query.order)
      selectedColumns <- project(query.projection, sortedRows)
      projections = if (query.projection.nonEmpty) query.projection
      else selectedColumns.headOption.map(_.getNames.map(Var)).getOrElse(Nil)
      withCorrectedColumnsOrder <- reorderColumns(projections, selectedColumns)
    } yield if (query.distinct) withCorrectedColumnsOrder.distinct else withCorrectedColumnsOrder

  private def applyRelationAlias(query: Select, schema: Schema): Either[SQLError, Schema] =
    query.alias match {
      case Some(alias) =>
        val relation = schema.getRelation(query.from).map(_.copy(name = alias))
        relation.map(schema.update)
      case None =>
        Right(schema)
    }

  private def project(exprs: List[Projection], rows: List[Row]): Either[SQLError, List[Row]] =
    exprs match {
      case Nil =>
        Right(rows)
      case _ =>
        rows
          .traverse { row =>
            exprs.collect {
              case acc: Accessor =>
                row.projectEither(acc.show)
              case Var(name) =>
                row.projectEither(name)
              case aggregate: Aggregate =>
                row.projectEither(aggregate.toString)
              case Alias(v: Var, alias) =>
                row.projectEither(v.name).map(_.copy(name = alias))
              case Alias(v: Aggregate, alias) =>
                row.projectEither(v.show).map(_.copy(name = alias))
              case lit: Literal =>
                Right(BodyAttribute(lit.show, lit))
              case Alias(lit: Literal, alias) =>
                Right(BodyAttribute(alias, lit))
              case mult: Multiplication =>
                ArithmeticInterpreter
                  .calculate(mult.left, mult.right, row, mult.calc)
                  .map(x => BodyAttribute(mult.show, x))
              case plus: Plus =>
                ArithmeticInterpreter
                  .calculate(plus.left, plus.right, row, plus.calc)
                  .map(x => BodyAttribute(plus.show, x))
              case minus: Minus =>
                ArithmeticInterpreter
                  .calculate(minus.left, minus.right, row, minus.calc)
                  .map(x => BodyAttribute(minus.show, x))
            }.sequence
          }
          .map(_.map(Row.apply))
    }

  private def filterRows(rows: List[Row], condition: Option[Bool]): Either[SQLError, List[Row]] =
    condition match {
      case None       => Right(rows)
      case Some(cond) => BoolInterpreter.eval(cond, rows)
    }

  private def having(rows: List[Row], condition: Option[Bool]): Either[SQLError, List[Row]] =
    filterRows(rows, condition)

  private def sortRows(rows: List[Row], orders: List[Order]): Either[SQLError, List[Row]] = {
    def sortRowsWithKey(rows: List[(Literal, Row)], condition: Int => Boolean): List[Row] =
      rows
        .sortWith {
          case ((key1, _), (key2, _)) => condition(Literal.compareUnsafe(key1, key2))
        }
        .map {
          case (_, row) => row
        }
    orders match {
      case Nil =>
        Right(rows)
      case _ =>
        orders.reverse.foldLeftM(rows) {
          case (currentRows, currentOrder) =>
            val name = currentOrder.name
            val rowsWithKey =
              currentRows.traverse(row => row.projectEither(name).map(key => (key.literal, row)))
            currentOrder match {
              case Descending(_) =>
                rowsWithKey.map(sortRowsWithKey(_, _ >= 0))
              case Ascending(_) =>
                rowsWithKey.map(sortRowsWithKey(_, _ <= 0))
            }
        }
    }
  }

  private def makeJoins(relation: Relation,
                        joins: List[Join],
                        schema: Schema): Either[SQLError, List[Row]] =
    for {
      joinsWithRelation <- joins.traverse(join =>
        schema.getRelation(join.name).map(JoinWithRelation(join, _)))
      rows <- joinsWithRelation.foldLeftM(relation) {
        case (relation0, join) => makeJoin(relation0, join.relation, join.join)
      }
    } yield rows.body

  private def innerJoin(left: Relation,
                        right: Relation,
                        condition: Bool): Either[SQLError, List[Row]] = {

    def matches(left: Row, right: Row, condition: Bool): Either[SQLError, Boolean] = {
      val sumRow = List(Row(left.attributes ::: right.attributes))
      BoolInterpreter.eval(condition, sumRow).map(_ == sumRow)
    }

    left.body.flatTraverse { leftRow =>
      right.body.flatTraverse { rightRow =>
        val first = prefixRelationName(leftRow, left.name, rightRow)
        val second = prefixRelationName(rightRow, right.name, leftRow)
        matches(first, second, condition).map {
          case false => Nil
          case true  => List(Row(first.attributes ::: second.attributes))
        }
      }
    }
  }

  private def prefixRelationName(left: Row, relationName: String, right: Row): Row = {
    val names = right.getNames
    val attributes = left.attributes.map { bodyAttrib =>
      if (names.contains(bodyAttrib.name))
        bodyAttrib.copy(name = s"$relationName.${bodyAttrib.name}")
      else bodyAttrib
    }
    Row(attributes)
  }

  private def makeJoin(left: Relation, right: Relation, join: Join): Either[SQLError, Relation] = {

    val joined = join match {
      case CrossJoin(_) =>
        val pairs = for {
          first <- left.body
          second <- right.body
        } yield (first, second)
        val crossProduct = pairs.map {
          case (first, second) =>
            val firstRow = prefixRelationName(first, left.name, second)
            val secondRow = prefixRelationName(second, right.name, first)
            Row(firstRow.attributes ::: secondRow.attributes)
        }
        Right(crossProduct)
      case InnerJoin(_, on) =>
        innerJoin(left, right, on)
      case LeftOuterJoin(_, on) =>
        val inner = innerJoin(left, right, on)
        makeOuterJoin(Nil, left.body, inner, right.heading, Nil)
      case RightOuterJoin(_, on) =>
        val inner = innerJoin(left, right, on)
        makeOuterJoin(left.heading, Nil, inner, Nil, right.body)
      case FullOuterJoin(_, on) =>
        val inner = innerJoin(left, right, on)
        makeOuterJoin(left.heading, left.body, inner, right.heading, right.body)
    }
    joined.map(rows => Relation(right.name, Nil, None, left.heading ::: right.heading, rows))
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
    def select(rows: List[Row], name: String): List[BodyAttribute] =
      rows.flatMap(_.projectOption(name))
    def updateGroup(groups: Map[Group, List[Row]],
                    group: Group,
                    values: List[BodyAttribute]): Map[Group, List[Row]] =
      groups.get(group) match {
        case Some(values1) => groups.updated(group, Row(values) :: values1)
        case None          => groups + (group -> List(Row(values)))
      }
    def aggregate(groupRow: List[Row]): Either[SQLError, List[BodyAttribute]] =
      aggregates.traverse { function =>
        val arguments =
          if (function.argument == "*") groupRow.flatMap(_.getValues.headOption)
          else select(groupRow, function.argument).map(_.literal)
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
      grouped.toList.traverse {
        case (group, groupRow) =>
          val aggregateResult = aggregate(groupRow)
          aggregateResult.map { result =>
            Row(group.values ::: result)
          }
      }
    }
  }

  private def reorderColumns(projections: List[Projection],
                             rows: List[Row]): Either[MissingColumnName, List[Row]] = {
    val names = projections
      .map {
        case Var(name)                      => name
        case a: Accessor                    => a.show
        case Alias(_, alias)                => alias
        case literal: Literal               => literal.show
        case aggregate: Aggregate           => aggregate.toString
        case multiplication: Multiplication => multiplication.show
        case plus: Plus                     => plus.show
        case minus: Minus                   => minus.show
      }
      .zipWithIndex
      .toMap
    val eitherRows = rows.traverse { row =>
      row.attributes.traverse { attribute =>
        Either
          .fromOption(names.get(attribute.name), MissingColumnName(attribute.name))
          .map(index => (attribute, index))
      }
    }
    eitherRows.map(_.map(row => Row(row.sortBy(_._2).map(_._1))))
  }
}
