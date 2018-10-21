package io.github.typeness.rdbms

import Relation._

object SelectionBuilder extends BuilderUtils {

  def select(query: Select, relation: Relation): Either[SQLError, List[Row]] = for {
    _ <- checkUndefinedNames(query.names, relation.heading.map(_.name))
    selectedRows = getSelectedAttributes(query.names, relation.body)
    filteredRows <- filterRows(selectedRows, query.condition)
    sortedRows = sortRows(filteredRows, query.order)
  } yield sortedRows

  private def getSelectedAttributes(names: List[String], rows: List[Row]): List[Row] = names match {
    case Nil => rows
    case _ => rows.map(_.filter(attribute => names.contains(attribute.name)))
  }

  private def filterRows(rows: List[Row], condition: Option[Bool]): Either[SQLError, List[Row]] = condition match {
    case None => Right(rows)
    case Some(cond) => BoolInterpreter.eval(cond, rows)
  }

  def sortRows(filteredRows: List[Row], order: Option[Order]): List[Row] = order match {
    case None => filteredRows
    case _ => ???
  }

}
