package io.github.typeness.rdbms

import Relation._


object SelectionBuilder extends BuilderUtils {

  private case class JoinWithRelation(join: Join, relation: Relation)

  def select(query: Select, schema: Schema): Either[SQLError, List[Row]] = for {
    relation <- schema.getRelation(query.from)
    //    _ <- checkUndefinedNames(query.projection, relation.heading.map(_.name))
    joined <- makeJoins(relation, query.joins, schema)
    selectedColumns = project(query.projection, joined)
    filteredRows <- filterRows(selectedColumns, query.condition)
    sortedRows = sortRows(filteredRows, query.order)
  } yield sortedRows

  private def project(names: List[String], rows: List[Row]): List[Row] = names match {
    case Nil => rows
    case _ => rows.map(_.filter(attribute => names.contains(attribute.name)))
  }

  private def filterRows(rows: List[Row], condition: Option[Bool]): Either[SQLError, List[Row]] = condition match {
    case None => Right(rows)
    case Some(cond) => BoolInterpreter.eval(cond, rows)
  }

  private def sortRows(filteredRows: List[Row], order: Option[Order]): List[Row] = order match {
    case None => filteredRows
    case _ => ???
  }

  private def makeJoins(relation: Relation, joins: List[Join], schema: Schema):
  Either[SQLError, List[Row]] = {
    val joinsWithRelation =
      joins.map(join => schema.getRelation(join.name).map(JoinWithRelation(join, _)))
    val joinsSequenced: Either[SchemaDoesNotExists, List[JoinWithRelation]] =
      joinsWithRelation.partition(_.isLeft) match {
        case (Nil, joinsRight) => Right(for (Right(join) <- joinsRight) yield join)
        case (error :: _, _) =>
            val Left(e) = error
            Left(e)
      }
    joinsSequenced.flatMap(foldJoins(relation, _))
  }

  private def foldJoins(left: Relation, joins: List[JoinWithRelation]): Either[SQLError, List[Row]] = joins match {
    case Nil => Right(left.body)
    case JoinWithRelation(join, relation) :: Nil =>
      makeJoin(left, relation, join)
    case JoinWithRelation(join, relation) :: tail =>
      makeJoin(left, relation, join).flatMap( result =>
        foldJoins(Relation("", Nil, None, left.heading ::: relation.heading, result), tail)
      )
  }

  private def makeJoin(left: Relation, right: Relation, join: Join): Either[SQLError, List[Row]] = {
    val pairs = for {
      first <- left.body
      second <- right.body
    } yield (first, second)
    val crossProduct = pairs.map{ case (first, second) => first ::: second}
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
      heading.map(attribute => BodyAttribute(attribute.name, NULLLiteral))

    sealed trait Side
    case object Left extends Side
    case object Right extends Side

    def joinWithNULL(
                      outer: List[Row],
                      inner: List[Row],
                      nullRow: List[BodyAttribute],
                      side: Side) = {
      val innerOfOuter = inner.map(_.filterNot(a => nullRow.exists(_.name == a.name)))
      outer.filterNot(innerOfOuter.contains(_)).map { row => side match {
        case Left => row ::: nullRow
        case Right => nullRow ::: row
      }}
    }

    val leftNullRow = makeNULLRow(leftHeading)
    val rightNullRow = makeNULLRow(rightHeading)
    for {
      inner <- innerJoin
      leftJoin = joinWithNULL(leftBody, inner, rightNullRow, Left)
      rightJoin = joinWithNULL(rightBody, inner, leftNullRow, Right)
    } yield leftJoin ::: inner ::: rightJoin
  }

}
