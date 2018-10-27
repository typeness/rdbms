package io.github.typeness.rdbms

import Relation._

object ManipulationBuilder extends BuilderUtils {

  def insertRow(query: Insert, relation: Relation): Either[SQLError, Relation] = query match {
    case NamedInsert(_, row) =>
      val queryNames = row.map(_.name)
      val relationNames = relation.heading.map(_.name)
      for {
        _ <- checkUndefinedNames(queryNames, relationNames)
        _ <- checkNonUniqueNames(queryNames)
        checkedIdentity <- checkIdentity(row, relation.identity)
        (rowWithCheckedIdentity, newIdentity) = checkedIdentity
        missing <- getMissingAttributes(rowWithCheckedIdentity, relation.heading)
        //        _ <- checkTypes(row, relation)
        // _ <- checkUniqueViolation
      } yield appendRow(missing ::: rowWithCheckedIdentity, relation, newIdentity)
    case AnonymousInsert(_, row) =>
      val relationRowSize = relation.heading.size
      val expectedSize = if (relation.identity.isEmpty) relationRowSize else relationRowSize - 1
      if (row.size != expectedSize) Left(WrongNumberOfAttributes)
      else {
        val header =
          if (relation.identity.isEmpty) relation.heading
          else relation.heading.filter(_.properties.collect{case PrimaryKey => true}.isEmpty)
        val newRow = row.zip(header).map {
          case (value, HeadingAttribute(name, _, _)) => BodyAttribute(name, value)
        }
        for {
          checkedIdentity <- checkIdentity(newRow, relation.identity)
          (rowWithCheckedIdentity, newIdentity) = checkedIdentity
        } yield appendRow(rowWithCheckedIdentity, relation, newIdentity)
        //        for {
        // _ <- checkTypes(row, relation)
        // <- checkUniqueViolation
        // <- checkConstraints
        //        }
        //      } yield appendRow()
      }
  }

  def deleteRows(query: Delete, relation: Relation): Either[SQLError, Relation] = query.condition match {
    case None => Right(relation.copy(body = Nil))
    case Some(condition) =>
      for {
        matching <- BoolInterpreter.eval(condition, relation.body)
        rows = relation.body
      } yield relation.copy(body = rows.diff(matching))
  }

  def updateRows(query: Update, relation: Relation): Either[SQLError, Relation] = query.condition match {
    case None => Right(relation)
    case Some(condition) =>
      for {
        matching <- BoolInterpreter.eval(condition, relation.body)
        updatedRows = matching.map{ row =>
          row.map(attribute => query.updated.find(_.name==attribute.name).getOrElse(attribute))
        }
      } yield relation.copy(body = updatedRows ::: relation.body.diff(matching))
  }

  private def appendRow(values: Row, relation: Relation, newIdentity: Option[Identity]): Relation = {
    //    val sameAttributes =
    //      values.map(attribute => attribute.name -> attribute.value.value.typeOf) ==
    //      relation.heading.map(attribute => attribute.name -> attribute.domain)
    //    assert(sameAttributes)
    val currentBody = relation.body
    relation.copy(body = values :: currentBody, identity = newIdentity)
  }


  private def getMissingAttributes(row: Row, header: Header): Either[MissingColumnName, Row] = {
    val missing = header.filterNot(attribute => row.exists(_.name == attribute.name))
    val mandatory = missing.filter(_.properties.exists {
      case _: PrimaryKey.type => true
      case _: NotNULL.type => true
      case _ => false
    })
    mandatory match {
      case Nil => Right(fillMissingAttributes(missing))
      case missed :: _ => Left(MissingColumnName(missed.name))
    }
  }


  private def fillMissingAttributes(attributes: List[HeadingAttribute]): Row = {
    def getDefaultValue(properties: List[Property]): Value = {
      val haveDefault = properties.filter {
        case _: Default => true
        case _ => false
      }
      haveDefault match {
        case Default(value) :: _ => value
        case _ => Value(NULLLiteral)
      }
    }

    attributes.map(attribute =>
      BodyAttribute(attribute.name, getDefaultValue(attribute.properties))
    )
  }

  private def checkIdentity(attributes: Row, identity: Option[Identity]):
  Either[SQLError, (Row, Option[Identity])] =
    identity match {
      case None => Right((attributes, None))
      case Some(ident) =>
        if (attributes.exists(_.name == ident.name)) {
          Left(IdentityViolation(ident.name))
        } else {
          val primaryKey = BodyAttribute(ident.name, Value(IntegerLiteral(ident.current)))
          val newValue = ident.current + ident.step
          Right(
            (primaryKey :: attributes, Some(ident.copy(current = newValue)))
          )
        }
    }
}
