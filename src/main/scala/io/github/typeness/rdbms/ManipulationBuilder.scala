package io.github.typeness.rdbms

import cats.instances.list._
import cats.instances.either._
import cats.syntax.traverse._
import cats.syntax.foldable._

import Relation._

object ManipulationBuilder extends BuilderUtils {

  def run(manipulation: Manipulation, schema: Schema): Either[SQLError, Schema] =
    manipulation match {
      case insert: Insert =>
        for {
          relation <- schema.getRelation(insert.to)
          newSchema <- insertRow(insert, relation, schema)
        } yield newSchema
      case delete: Delete =>
        for {
          relation <- schema.getRelation(delete.name)
          newSchema <- deleteRows(delete, relation, schema)
        } yield newSchema
      case update: Update =>
        for {
          relation <- schema.getRelation(update.name)
          newSchema <- updateRows(update, relation, schema)
        } yield newSchema
    }

  private def insertRow(query: Insert,
                        relation: Relation,
                        schema: Schema): Either[SQLError, Schema] =
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
          checkedRow <- checkChecks(typecheckedRow, relation.heading)
          checkedUniqueViolation <- checkUnique(checkedRow, relation)
          checkedForeignKeysReferences <- checkForeignKeysReferences(checkedUniqueViolation,
                                                                     relation,
                                                                     schema)
          updatedRelation = appendRow(checkedForeignKeysReferences, relation, newIdentity)
        } yield schema.update(updatedRelation)
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
            checkedRow <- checkChecks(typeCheckedRow, relation.heading)
            checkedUniqueViolation <- checkUnique(checkedRow, relation)
            checkedPrimaryKeyDuplicates <- checkPrimaryKeyDuplicate(checkedUniqueViolation,
                                                                    relation,
                                                                    schema)
            checkedForeignKeysReferences <- checkForeignKeysReferences(checkedPrimaryKeyDuplicates,
                                                                       relation,
                                                                       schema)
            updatedRelation = appendRow(checkedForeignKeysReferences, relation, newIdentity)
          } yield schema.update(updatedRelation)
        }
    }

  private def deleteRows(query: Delete,
                         relation: Relation,
                         schema: Schema): Either[SQLError, Schema] = {

    def delete(matchingRows: List[Row]): Either[SQLError, Schema] = {
      val rows = relation.body
      val newRelation = relation.copy(body = rows.diff(matchingRows))
      onDeletePrimaryKey(matchingRows.zip(matchingRows), newRelation, schema.update(newRelation))
    }
    query.condition match {
      case None =>
        delete(relation.body)
      case Some(condition) =>
        BoolInterpreter.eval(condition, relation.body).flatMap(delete)
    }
  }

  private def updateRows(query: Update,
                         relation: Relation,
                         schema: Schema): Either[SQLError, Schema] = {
    def update(matchingRows: List[Row]): Either[SQLError, Schema] = {
      for {
        updatedRows <- matchingRows.traverse { row =>
          val newRow =
            row.map(attrib => query.updated.projectOption(attrib.name).getOrElse(attrib))
          checkTypes(newRow, relation.heading)
        }
        checkedForeignKeysReferences <- updatedRows.traverse(
          checkForeignKeysReferences(_, relation, schema))
        newRelation = relation.copy(
          body = checkedForeignKeysReferences ::: relation.body.diff(matchingRows))
        newSchema <- onUpdatePrimaryKey(matchingRows.zip(updatedRows),
                                        newRelation,
                                        schema.update(newRelation))
      } yield newSchema
    }
    query.condition match {
      case None =>
        update(relation.body)
        Right(schema.update(relation))
      case Some(condition) =>
        BoolInterpreter.eval(condition, relation.body).flatMap(update)
    }
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

  private def checkForeignKeysReferences(row: Row,
                                         relation: Relation,
                                         schema: Schema): Either[SQLError, Row] = {

    def pKeyRelationContainsFKey(value: BodyAttribute,
                                 key: ForeignKey): Either[SQLError, BodyAttribute] = {
      val select = Select(List(Var(key.primaryKeyName)),
                          key.pKeyRelationName,
                          Nil,
                          Some(Equals(key.primaryKeyName, value.literal)),
                          Nil,
                          None,
                          Nil)
      val contains = QueryBuilder.run(select, schema).map(_.nonEmpty)
      contains.flatMap {
        case true =>
          Right(value)
        case false =>
          Left(
            PrimaryKeyDoesNotExist(relation.name,
                                   value.name,
                                   key.pKeyRelationName,
                                   key.primaryKeyName,
                                   value.literal))
      }
    }

    val foreignKeysEither = relation.getForeignKeys.traverse {
      case (name, key) =>
        for {
          value <- row.projectEither(name)
        } yield (value, key)
    }

    for {
      foreignKeys <- foreignKeysEither
      _ <- foreignKeys.traverse {
        case (value, key) => pKeyRelationContainsFKey(value, key)
      }
    } yield row
  }

  private def checkConstraint(
      row: Row,
      header: Header,
      constraint: HeadingAttribute => BodyAttribute => Either[SQLError, BodyAttribute])
    : Either[SQLError, Row] =
    header
      .traverse { attrib =>
        val bodyAttribute = row.projectEither(attrib.name)
        bodyAttribute.flatMap(constraint(attrib))
      }
      .map(Row.apply)

  private def checkTypes(row: Row, header: Header): Either[SQLError, Row] =
    checkConstraint(
      row,
      header,
      attrib =>
        body =>
          if (body.literal.typeOf == attrib.domain || body.literal.typeOf == NullType) Right(body)
          else Left(TypeMismatch(attrib.domain, body.literal.typeOf, body.literal))
    )

  private def checkChecks(row: Row, header: Header): Either[SQLError, Row] =
    checkConstraint(
      row,
      header,
      attrib =>
        body => {
          val checks = attrib.constraints.collect {
            case c: Check => c.bool
          }
          if (checks.forall(
                bool => BoolInterpreter.eval(bool, List(Row(body))) == Right(List(Row(body)))))
            Right(body)
          else Left(CheckViolation(attrib, body.literal))
      }
    )

  private def checkPrimaryKeyDuplicate(row: Row,
                                       relation: Relation,
                                       schema: Schema): Either[SQLError, Row] = {
//    val pKeys = relation.primaryKeys.traverse(row.projectEither)
    val pKeys = relation.getPrimaryKeys.traverse(key => row.projectEither(key.name))
    pKeys.flatMap {
      case Nil => Right(row)
      case key :: keys =>
        val condition = keys.foldLeft[Bool](Equals(key.name, key.literal)) {
          case (equals, key0) => And(equals, Equals(key0.name, key0.literal))
        }
        val select = Select(Nil, relation.name, Nil, Some(condition), Nil, None, Nil)
        QueryBuilder.run(select, schema).map(_.isEmpty).flatMap {
          case true  => Right(row)
          case false => Left(PrimaryKeyDuplicate(key :: keys))
        }
    }
  }

  private def checkUnique(row: Row, relation: Relation): Either[SQLError, Row] =
    checkConstraint(
      row,
      relation.heading,
      attrib =>
        body => {
          val uniqueDisallowed = attrib.constraints.collect {
            case unique: Unique.type => unique
          }.nonEmpty
          if (uniqueDisallowed) {
            val select = Select(List(Var(body.name)),
                                relation.name,
                                Nil,
                                Some(Equals(body.name, body.literal)),
                                Nil,
                                None,
                                Nil)
            val notFound = QueryBuilder.run(select, Schema(List(relation))).map(_.isEmpty)
            notFound.flatMap {
              case true  => Right(body)
              case false => Left(UniqueViolation(body))
            }
          } else Right(body)
      }
    )

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

  type FKeyName = String

  private case class PKeyFKeyRelations(pKeyRelation: Relation,
                                       fKeyRelation: Relation,
                                       keyPairs: List[(HeadingAttribute, ForeignKey, FKeyName)])

  private object PKeyFKeyRelations {
    def apply(pKeyRelation: Relation, fKeyRelation: Relation): PKeyFKeyRelations = {
      val pKeyRelationName = pKeyRelation.name
      val primaryKeys = pKeyRelation.getPrimaryKeys
      val pKeysNames = primaryKeys.map(_.name)
      val keyPairs = fKeyRelation.heading.flatMap { attribute =>
        attribute.constraints.collect {
          case fKey @ ForeignKey(pkName, `pKeyRelationName`, _, _) if pKeysNames.contains(pkName) =>
            (primaryKeys.find(_.name == pkName).get, fKey, attribute.name)
        }
      }
      PKeyFKeyRelations(pKeyRelation, fKeyRelation, keyPairs)
    }
  }

  private sealed trait ModifyTrigger {
    def getFunction(foreignKey: ForeignKey)
      : (Relation, FKeyName, ForeignKey, BodyAttribute, BodyAttribute, Schema) => Either[SQLError,
                                                                                         List[Row]]
  }
  private case object DeleteTrigger extends ModifyTrigger {
    override def getFunction(foreignKey: ForeignKey): (Relation,
                                                       FKeyName,
                                                       ForeignKey,
                                                       BodyAttribute,
                                                       BodyAttribute,
                                                       Schema) => Either[SQLError, List[Row]] =
      foreignKey.onDelete match {
        case NoAction =>
          onModifyNoAction
        case Cascade =>
          onModifyDeleteCascade
        case SetNULL =>
          onModifySetNULL
        case SetDefault =>
          onModifySetDefault
      }
  }

  private case object UpdateTrigger extends ModifyTrigger {
    override def getFunction(foreignKey: ForeignKey): (Relation,
                                                       FKeyName,
                                                       ForeignKey,
                                                       BodyAttribute,
                                                       BodyAttribute,
                                                       Schema) => Either[SQLError, List[Row]] =
      foreignKey.onUpdate match {
        case NoAction =>
          onModifyNoAction
        case Cascade =>
          onModifyUpdateCascade
        case SetNULL =>
          onModifySetNULL
        case SetDefault =>
          onModifySetDefault
      }
  }

  private def onModifyPrimaryKey(matchingRows: List[(Row, Row)],
                                 pKeyRelation: Relation,
                                 schema: Schema,
                                 trigger: ModifyTrigger): Either[SQLError, Schema] = {

    def updateRow(relationPair: PKeyFKeyRelations,
                  pKey: HeadingAttribute,
                  relation: List[Row],
                  fKey: ForeignKey,
                  fKeyName: FKeyName): Either[SQLError, List[Row]] = {
      val eitherPrimaryKeyValues =
        matchingRows.traverse {
          case (oldRow, newRow) =>
            oldRow
              .projectEither(pKey.name)
              .flatMap(old => newRow.projectEither(pKey.name).flatMap(`new` => Right((old, `new`))))
        }
      eitherPrimaryKeyValues.flatMap { primaryKeyValues =>
        primaryKeyValues.foldLeft[Either[SQLError, List[Row]]](Right(relation)) {
          case (Right(relation2), (oldAttributte: BodyAttribute, newAttribute: BodyAttribute)) =>
            trigger.getFunction(fKey)(relationPair.fKeyRelation.copy(body = relation2),
                                      fKeyName,
                                      fKey,
                                      oldAttributte,
                                      newAttribute,
                                      schema)
          case (left, _) => left
        }
      }
    }

    val relationPairs =
      schema.relations.values.toList
        .filterNot(_.name == pKeyRelation.name)
        .map(PKeyFKeyRelations(pKeyRelation, _))
    relationPairs
      .traverse { relationPair =>
        relationPair.keyPairs
          .foldLeftM(relationPair.fKeyRelation.body) {
            case (relation, (pKey, fKey, fKeyName)) =>
              updateRow(relationPair, pKey, relation, fKey, fKeyName)
          }
          .map(rows => relationPair.fKeyRelation.copy(body = rows))
      }
      .map(_.foldLeft(schema)(_.update(_)))
  }

  private def onUpdatePrimaryKey(modifiedRows: List[(Row, Row)],
                                 pKeyRelation: Relation,
                                 schema: Schema): Either[SQLError, Schema] =
    onModifyPrimaryKey(modifiedRows, pKeyRelation, schema, UpdateTrigger)

  private def onDeletePrimaryKey(modifiedRows: List[(Row, Row)],
                                 pKeyRelation: Relation,
                                 schema: Schema): Either[SQLError, Schema] =
    onModifyPrimaryKey(modifiedRows, pKeyRelation, schema, DeleteTrigger)

  private def onModifyDeleteCascade(fKeyRelation: Relation,
                                    foreignKeyName: FKeyName,
                                    foreignKey: ForeignKey,
                                    oldKey: BodyAttribute,
                                    newKey: BodyAttribute,
                                    schema: Schema): Either[SQLError, List[Row]] = {
    val delete = Delete(fKeyRelation.name, Some(Equals(foreignKeyName, oldKey.literal)))
    ManipulationBuilder
      .deleteRows(delete, fKeyRelation, schema)
      .flatMap(_.getRelation(fKeyRelation.name))
      .map(_.body)
  }

  private def onModifyChangeFKey(fKeyRelation: Relation,
                                 foreignKeyName: FKeyName,
                                 foreignKey: ForeignKey,
                                 oldKey: BodyAttribute,
                                 newKey: Literal,
                                 schema: Schema): Either[SQLError, List[Row]] = {
    val update = Update(fKeyRelation.name,
                        Row(BodyAttribute(foreignKeyName, newKey)),
                        Some(Equals(foreignKeyName, oldKey.literal)))
    ManipulationBuilder
      .updateRows(update, fKeyRelation, schema)
      .flatMap(_.getRelation(fKeyRelation.name))
      .map(_.body)
  }

  private def onModifyUpdateCascade(fKeyRelation: Relation,
                                    foreignKeyName: FKeyName,
                                    foreignKey: ForeignKey,
                                    oldKey: BodyAttribute,
                                    newKey: BodyAttribute,
                                    schema: Schema): Either[SQLError, List[Row]] =
    onModifyChangeFKey(fKeyRelation, foreignKeyName, foreignKey, oldKey, newKey.literal, schema)

  private def onModifySetNULL(fKeyRelation: Relation,
                              foreignKeyName: FKeyName,
                              foreignKey: ForeignKey,
                              oldKey: BodyAttribute,
                              newKey: BodyAttribute,
                              schema: Schema): Either[SQLError, List[Row]] =
    onModifyChangeFKey(fKeyRelation, foreignKeyName, foreignKey, oldKey, NULLLiteral, schema)

  private def onModifySetDefault(fKeyRelation: Relation,
                                 foreignKeyName: FKeyName,
                                 foreignKey: ForeignKey,
                                 oldKey: BodyAttribute,
                                 newKey: BodyAttribute,
                                 schema: Schema): Either[SQLError, List[Row]] = {
    val defaultOrNULL = fKeyRelation.heading
      .find(_.name == foreignKeyName)
      .map(_.constraints)
      .flatMap(_.collectFirst { case d: Default => d.value })
      .getOrElse(NULLLiteral)
    onModifyChangeFKey(fKeyRelation, foreignKeyName, foreignKey, oldKey, defaultOrNULL, schema)
  }

  private def onModifyNoAction(fKeyRelation: Relation,
                               foreignKeyName: FKeyName,
                               foreignKey: ForeignKey,
                               oldKey: BodyAttribute,
                               newKey: BodyAttribute,
                               schema: Schema): Either[SQLError, List[Row]] = {
    val select = Select(List(Var(foreignKeyName)),
                        fKeyRelation.name,
                        Nil,
                        Some(Equals(foreignKeyName, oldKey.literal)),
                        Nil,
                        None,
                        Nil)
    QueryBuilder.run(select, schema).flatMap {
      case Nil => Right(fKeyRelation.body)
      case _   => Left(ForeignKeyViolation(fKeyRelation.name, foreignKeyName))
    }
  }
}
