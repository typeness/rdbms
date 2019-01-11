package io.github.typeness.rdbms

import cats.instances.list._
import cats.instances.either._
import cats.syntax.traverse._
import cats.syntax.either._

import Relation._

object ManipulationBuilder extends BuilderUtils {

  def insertRow(query: Insert, relation: Relation): Either[SQLError, Relation] =
    query match {
      case NamedInsert(_, row) =>
        val queryNames = row.getNames
        val relationNames = relation.heading.map(_.name)
        for {
          _ <- checkUndefinedNames(queryNames, relationNames)
          _ <- checkNonUniqueNames(queryNames)
          checkedIdentity <- checkIdentity(row, relation.identity)
          (rowWithCheckedIdentity, newIdentity) = checkedIdentity
          missing <- getMissingAttributes(rowWithCheckedIdentity, relation.heading)
          filledRow = Row(missing.attributes ::: rowWithCheckedIdentity.attributes)
          typecheckedRow <- checkTypes(filledRow, relation.heading)
          // _ <- checkUniqueViolation
        } yield appendRow(typecheckedRow, relation, newIdentity)
      case AnonymousInsert(_, row) =>
        val relationRowSize = relation.heading.size
        val expectedSize =
          if (relation.identity.isEmpty) relationRowSize
          else relationRowSize - 1
        if (row.size != expectedSize) Left(WrongNumberOfAttributes)
        else {
          val header =
            if (relation.identity.isEmpty) relation.heading
            else
              relation.heading.filter(_.constraints.collect {
                case PrimaryKey => true
              }.isEmpty)
          val newRow = Row(
            row.zip(header).map {
              case (value, HeadingAttribute(name, _, _)) =>
                BodyAttribute(name, value)
            }
          )
          for {
            checkedIdentity <- checkIdentity(newRow, relation.identity)
            (rowWithCheckedIdentity, newIdentity) = checkedIdentity
            typeCheckedRow <- checkTypes(rowWithCheckedIdentity, relation.heading)
          } yield appendRow(typeCheckedRow, relation, newIdentity)
          //        for {
          // <- checkUniqueViolation
          // <- checkConstraints
          //        }
          //      } yield appendRow()
        }
    }

  def deleteRows(query: Delete, relation: Relation): Either[SQLError, Relation] =
    query.condition match {
      case None => Right(relation.copy(body = Nil))
      case Some(condition) =>
        for {
          matching <- BoolInterpreter.eval(condition, relation.body)
          rows = relation.body
        } yield relation.copy(body = rows.diff(matching))
    }

  def updateRows(query: Update, relation: Relation): Either[SQLError, Relation] =
    query.condition match {
      case None => Right(relation)
      case Some(condition) =>
        for {
          matchingRows <- BoolInterpreter.eval(condition, relation.body)
          updatedRows <- matchingRows.traverse { row =>
            val newRow =
              row.map(attrib => query.updated.projectOption(attrib.name).getOrElse(attrib))
            checkTypes(newRow, relation.heading)
          }
        } yield relation.copy(body = updatedRows ::: relation.body.diff(matchingRows))
    }

  private def appendRow(values: Row,
                        relation: Relation,
                        newIdentity: Option[Identity]): Relation = {
    //    val sameAttributes =
    //      values.map(attribute => attribute.name -> attribute.value.value.typeOf) ==
    //      relation.heading.map(attribute => attribute.name -> attribute.domain)
    //    assert(sameAttributes)
    val currentBody = relation.body
    relation.copy(body = values :: currentBody, identity = newIdentity)
  }

  private def checkTypes(row: Row, header: Header): Either[SQLError, Row] = {
    header
      .traverse { attrib =>
        val bodyAttribute = row.projectEither(attrib.name)
        bodyAttribute.flatMap(
          body =>
            if (body.literal.typeOf == attrib.domain || body.literal.typeOf == NullType) Right(body)
            else Left(TypeMismatch(attrib.domain, body.literal.typeOf, body.literal)))
      }
      .map(Row.apply)
  }

  private def getMissingAttributes(row: Row, header: Header): Either[MissingColumnName, Row] = {
    val missing = header.filter(attribute => row.projectOption(attribute.name).isEmpty)
    val mandatory = missing.filter(_.constraints.exists {
      case _: PrimaryKey.type => true
      case _: NotNULL.type    => true
      case _                  => false
    })
    mandatory match {
      case Nil         => Right(fillMissingAttributes(missing))
      case missed :: _ => Left(MissingColumnName(missed.name))
    }
  }

  private def fillMissingAttributes(attributes: List[HeadingAttribute]): Row = {
    def getDefaultValue(properties: List[Constraint]): Literal = {
      val haveDefault = properties.filter {
        case _: Default => true
        case _          => false
      }
      haveDefault match {
        case Default(value) :: _ => value
        case _                   => NULLLiteral
      }
    }

    Row(
      attributes.map(attribute =>
        BodyAttribute(attribute.name, getDefaultValue(attribute.constraints)))
    )
  }

  private def checkIdentity(
      row: Row,
      identityOption: Option[Identity]): Either[IdentityViolation, (Row, Option[Identity])] =
    identityOption match {
      case None => Right((row, None))
      case Some(identity) =>
        if (row.projectOption(identity.name).isDefined) {
          Left(IdentityViolation(identity.name))
        } else {
          val primaryKey =
            BodyAttribute(identity.name, IntegerLiteral(identity.current))
          val newValue = identity.current + identity.step
          Right(
            (Row(primaryKey :: row.attributes), Some(identity.copy(current = newValue)))
          )
        }
    }
}
