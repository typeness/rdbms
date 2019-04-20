package io.github.typeness.rdbms

import cats.instances.list._
import cats.instances.either._
import cats.syntax.traverse._

object RelationBuilder extends BuilderUtils {

  def run(definition: Definition, schema: Schema): Either[SQLError, Schema] = definition match {
    case c: Create         => build(c, schema)
    case alter: AlterTable => alterTable(alter, schema)
    case d: DropTable      => drop(d, schema)
  }

  private def alterTable(alter: AlterTable, schema: Schema): Either[SQLError, Schema] =
    for {
      relation <- schema.getRelation(alter.relation)
      newRelation <- alter match {
        case add @ AlterAddColumn(_, _)       => addColumn(add, relation)
        case drop @ AlterDropColumn(_, _)     => dropColumn(drop, relation)
        case add @ AlterAddConstraint(_, _)   => addConstraint(add, relation)
        case drop @ AlterDropConstraint(_, _) => dropConstraint(drop, relation)
      }
    } yield schema.update(newRelation)

  private def dropColumn(dropColumn: AlterDropColumn,
                         relation: Relation): Either[SQLError, Relation] = {
    val newHeader = relation.heading.filter(_.name != dropColumn.column)
    val newBody = relation.body.map(_.filter(_.name != dropColumn.column))
    Right(relation.copy(heading = newHeader, body = newBody))
  }

  private def dropConstraint(dropConstraint: AlterDropConstraint,
                             relation: Relation): Either[SQLError, Relation] = {
    val newHeader = relation.heading.map { attrib =>
      val newConstraints = attrib.constraints.filterNot(_.name.contains(dropConstraint.constraint))
      attrib.copy(constraints = newConstraints)
    }
    Right(relation.copy(heading = newHeader))
  }

  private def addColumn(alterAddColumn: AlterAddColumn,
                        relation: Relation): Either[SQLError, Relation] = {
    val newHeading = alterAddColumn.headingAttribute :: relation.heading
    val newBody = relation.body.map(row =>
      Row(BodyAttribute(alterAddColumn.headingAttribute.name, NULLLiteral) :: row.attributes))
    Right(relation.copy(heading = newHeading, body = newBody))
  }

  private def addConstraint(alterAddConstraint: AlterAddConstraint,
                            relation: Relation): Either[SQLError, Relation] = {
    val newHeading = alterAddConstraint.constraint.names.foldLeft(relation.heading) {
      case (heading0, name) =>
        heading0.map { attrib =>
          val constraints = attrib.constraints
          if (attrib.name == name)
            attrib.copy(
              constraints = alterAddConstraint.constraint.toColumnConstraint :: constraints)
          else attrib
        }
    }
    Right(relation.copy(heading = newHeading))
  }

  private def build(query: Create, schema: Schema): Either[SQLError, Schema] =
    for {
      _ <- checkMultiplePrimaryKeys(query)
      createWithConstrains <- rewriteRelationConstrains(query)
      names = createWithConstrains.attributes.map(_.name)
      _ <- checkNonUniqueNames(names)
      primaryKeys = getPrimaryKeys(createWithConstrains)
      relation = Relation(createWithConstrains.name,
                          primaryKeys,
                          createWithConstrains.identity,
                          createWithConstrains.attributes,
                          Nil)
    } yield schema.update(relation)

  private def drop(query: DropTable, schema: Schema): Either[SQLError, Schema] =
    Right(
      Schema(schema.relations.-(query.name))
    )

//  def alterDrop(alter: AlterDrop): Either[SQLError, Relation] = ???

  private def rewriteRelationConstrains(create: Create): Either[SQLError, Create] = {

    def updateAttributeWithConstraint(attributes: Relation.Header,
                                      name: String,
                                      constraint: ColumnConstraint): List[HeadingAttribute] = {
      attributes.map {
        case attribute @ HeadingAttribute(`name`, _, constraints) =>
          val newConstraints = (constraint :: constraints).distinct
          attribute.copy(constraints = newConstraints)
        case a => a
      }
    }

    def updateHeader(names: List[String],
                     attributes: Relation.Header,
                     constraint: ColumnConstraint): List[HeadingAttribute] = {
      names.foldLeft(attributes) {
        case (attributes0, name) => updateAttributeWithConstraint(attributes0, name, constraint)
      }
    }

    val constraintNames = create.relationConstraints.flatMap(_.names)
    val attributeNames = create.attributes.map(_.name)
    val newHeader = constraintNames.filter(!attributeNames.contains(_)) match {
      case name :: _ =>
        Left(ColumnDoesNotExists(name))
      case Nil =>
        Right(create.relationConstraints.foldLeft(create.attributes) {
          case (attributes, pKey @ PKeyRelationConstraint(names, _)) =>
            updateHeader(names, attributes, pKey.toColumnConstraint)
          case (attributes, fKey @ FKeyRelationConstraint(names, _, _, _, _, _)) =>
            updateHeader(names, attributes, fKey.toColumnConstraint)
        })
    }
    def getIdentity(attribute: HeadingAttribute): Either[MultipleIdentity, Option[Identity]] = {
      val identity = attribute.constraints.collect {
        case id: AttributeIdentity => id
      }
      identity match {
        case Nil =>
          Right(None)
        case id :: Nil =>
          Right(Some(id.toIdentity(attribute.name)))
        case id1 :: id2 :: _ =>
          Left(MultipleIdentity(id1.toIdentity(attribute.name), id2.toIdentity(attribute.name)))
      }
    }
    val newIdentity = create.attributes
      .traverse(getIdentity)
      .map(_.flatten)
      .flatMap {
        case Nil             => Right(None)
        case id :: Nil       => Right(Some(id))
        case id1 :: id2 :: _ => Left(MultipleIdentity(id1, id2))
      }
    for {
      header <- newHeader
      identity <- newIdentity
    } yield create.copy(attributes = header, identity = identity)
  }

  def checkMultiplePrimaryKeys(query: Create): Either[SQLError, Unit] = {
    getPrimaryKeys(query) match {
      case a :: b :: _ => Left(MultiplePrimaryKeys(a, b))
      case _           => Right(())
    }

  }

  private def getPrimaryKeys(query: Create): List[String] = {

    val attributes = query.attributes
    def isPrimaryKey(property: ColumnConstraint): Boolean = property match {
      case PrimaryKey(_) => true
      case _             => false
    }

    attributes.collect {
      case attribute: HeadingAttribute if attribute.constraints.exists(isPrimaryKey) =>
        attribute.name
    }
  }

}
