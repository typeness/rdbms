package io.github.typeness.rdbms

object RelationBuilder extends BuilderUtils {

  def build(query: Create): Either[SQLError, Relation] =
    for {
      primaryKey <- getPrimaryKey(query)
      names = query.attributes.map(_.name)
      _ <- checkNonUniqueNames(names)
    } yield Relation(query.name, primaryKey, query.identity, query.attributes, Nil)

  def drop(query: Drop, schema: Schema): Either[SQLError, Schema] =
    Right(
      Schema(schema.relations.filter(_.name != query.name))
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
