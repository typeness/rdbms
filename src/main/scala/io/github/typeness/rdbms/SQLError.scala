package io.github.typeness.rdbms

import io.github.typeness.rdbms.Relation.Header

sealed trait SQLError
case class MultiplePrimaryKeys(first: AttributeName, second: AttributeName) extends SQLError
case class MultipleColumnNames(name: AttributeName) extends SQLError
case class ColumnDoesNotExists(name: AttributeName) extends SQLError
case class MissingColumnName(name: AttributeName) extends SQLError
case class WrongNumberOfAttributes(relationHeader: Header, rows: List[Literal]) extends SQLError
case class IdentityViolation(name: AttributeName) extends SQLError
case class RelationDoesNotExists(name: RelationName) extends SQLError
case class TypeMismatch(first: AnyType, second: AnyType, literal: Literal) extends SQLError
case class CheckViolation(headingAttribute: HeadingAttribute, literal: Literal) extends SQLError
case class UniqueViolation(bodyAttribute: BodyAttribute) extends SQLError
case class PrimaryKeyDuplicate(values: List[BodyAttribute]) extends SQLError
case class ForeignKeyViolation(relationName: RelationName, fKeyName: AttributeName) extends SQLError
case class MultipleIdentity(first: Identity, second: Identity) extends SQLError
case class PrimaryKeyDoesNotExist(fKeyRelationName: RelationName,
                                  fKeyName: AttributeName,
                                  pKeyRelationName: RelationName,
                                  pKeyName: AttributeName,
                                  value: Literal)
    extends SQLError
