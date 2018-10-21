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
        missing <- getMissingAttributes(row, relation.heading)
        //        _ <- checkTypes(row, relation)
        // _ <- checkUniqueViolation
      } yield appendRow(missing ::: row, relation)
    case AnonymousInsert(_, row) => ???

  }

  private def appendRow(values: Row, relation: Relation): Relation = {
//    val sameAttributes =
//      values.map(attribute => attribute.name -> attribute.value.value.typeOf) ==
//      relation.heading.map(attribute => attribute.name -> attribute.domain)
//    assert(sameAttributes)
    val currentBody = relation.body
    relation.copy(body = values :: currentBody)
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
}
