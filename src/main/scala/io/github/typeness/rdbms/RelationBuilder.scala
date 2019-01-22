package io.github.typeness.rdbms

object RelationBuilder extends BuilderUtils {

  def run(definition: Definition, schema: Schema): Either[SQLError, Schema] = definition match {
    case c: Create     => build(c, schema)
    case _: AlterAdd  => ???
    case _: AlterDrop => ???
    case d: Drop       => drop(d, schema)
  }

  private def build(query: Create, schema: Schema): Either[SQLError, Schema] =
    for {
      primaryKey <- getPrimaryKey(query)
      names = query.attributes.map(_.name)
      _ <- checkNonUniqueNames(names)
      relation = Relation(query.name, primaryKey, query.identity, query.attributes, Nil)
    } yield schema.update(relation)

  private def drop(query: Drop, schema: Schema): Either[SQLError, Schema] =
    Right(
      Schema(schema.relations.-(query.name))
    )

//  def alterDrop(alter: AlterDrop): Either[SQLError, Relation] = ???

  private def getPrimaryKey(query: Create): Either[SQLError, List[String]] =
    query.primaryKeys match {
      case Nil => getPrimaryKeyFromAttributes(query.attributes).map(_.toList)
      case declaredPrimaryKeys =>
        val declaredNames = query.attributes.map(_.name)
        for {
          _ <- checkUndefinedNames(declaredPrimaryKeys, declaredNames)
        } yield declaredPrimaryKeys
    }

  private def getPrimaryKeyFromAttributes(
      attributes: List[HeadingAttribute]): Either[SQLError, Option[String]] = {

    def isPrimaryKey(property: Constraint): Boolean = property match {
      case PrimaryKey => true
      case _          => false
    }

    val primaryKeys = attributes.collect {
      case attribute: HeadingAttribute if attribute.constraints.exists(isPrimaryKey) =>
        attribute.name
    }
    primaryKeys match {
      case Nil        => Right(None)
      case key :: Nil => Right(Some(key))
      case _          => Left(MultiplePrimaryKeys)
    }
  }

}
