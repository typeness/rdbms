package io.github.typeness.rdbms

object RelationBuilder extends BuilderUtils {

  def run(definition: Definition, schema: Schema): Either[SQLError, Schema] = definition match {
    case c: Create    => build(c, schema)
    case _: AlterAdd  => ???
    case _: AlterDrop => ???
    case d: Drop      => drop(d, schema)
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

  private def drop(query: Drop, schema: Schema): Either[SQLError, Schema] =
    Right(
      Schema(schema.relations.-(query.name))
    )

//  def alterDrop(alter: AlterDrop): Either[SQLError, Relation] = ???

  private def rewriteRelationConstrains(create: Create): Either[ColumnDoesNotExists, Create] = {

    def updateAttributeWithConstraint(attributes: Relation.Header,
                                      name: String,
                                      constraint: Constraint): List[HeadingAttribute] = {
      attributes.map {
        case attribute @ HeadingAttribute(`name`, _, constraints) =>
          val newConstraints = (constraint :: constraints).distinct
          attribute.copy(constraints = newConstraints)
        case a => a
      }
    }

    def updateHeader(names: List[String],
                     attributes: Relation.Header,
                     constraint: Constraint): List[HeadingAttribute] = {
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
          case (attributes, PKeyRelationConstraint(names, name)) =>
            updateHeader(names, attributes, PrimaryKey(name))
          case (attributes,
                FKeyRelationConstraint(names,
                                       pKeyRelationName,
                                       pKeyColumnName,
                                       onDelete,
                                       onUpdate, name)) =>
            updateHeader(names,
                         attributes,
                         ForeignKey(pKeyColumnName, pKeyRelationName, onUpdate, onDelete, name))
        })
    }
    for {
      header <- newHeader
    } yield create.copy(attributes = header)
  }

  def checkMultiplePrimaryKeys(query: Create): Either[SQLError, Unit] = {
    getPrimaryKeys(query) match {
      case a :: b :: _ => Left(MultiplePrimaryKeys(a, b))
      case _            => Right(())
    }

  }

  private def getPrimaryKeys(query: Create): List[String] = {

    val attributes = query.attributes
    def isPrimaryKey(property: Constraint): Boolean = property match {
      case PrimaryKey(_) => true
      case _          => false
    }

    attributes.collect {
      case attribute: HeadingAttribute if attribute.constraints.exists(isPrimaryKey) =>
        attribute.name
    }
  }

}
