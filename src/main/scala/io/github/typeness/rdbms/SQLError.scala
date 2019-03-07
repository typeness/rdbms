package io.github.typeness.rdbms

sealed trait SQLError
case class MultiplePrimaryKeys(first: String, second: String) extends SQLError
case class MultipleColumnNames(name: String) extends SQLError
case class ColumnDoesNotExists(name: String) extends SQLError
case class MissingColumnName(name: String) extends SQLError
case object WrongNumberOfAttributes extends SQLError
case class IdentityViolation(name: String) extends SQLError
case class RelationDoesNotExists(name: String) extends SQLError
case class TypeMismatch(first: AnyType, second: AnyType, literal: Literal) extends SQLError
case class CheckViolation(headingAttribute: HeadingAttribute, literal: Literal) extends SQLError
case class UniqueViolation(bodyAttribute: BodyAttribute) extends SQLError
case class PrimaryKeyDuplicate(values: List[BodyAttribute]) extends SQLError
case class ForeignKeyViolation(relationName: String, fKeyName: String) extends SQLError
case class MultipleIdentity(first: Identity, second: Identity) extends SQLError
case class PrimaryKeyDoesNotExist(fKeyRelationName: String,
                                  fKeyName: String,
                                  pKeyRelationName: String,
                                  pKeyName: String,
                                  value: Literal)
    extends SQLError
