package io.github.typeness.rdbms

import fastparse.Parsed
import cats.syntax.foldable._

object SchemaFormatSQL extends SchemaFormat[String] {
  def serialize(relation: Relation): String = {
    val creation = createTable(inSquareBrackets(relation.name.value), relation.heading, relation.relationConstraints)
    val insertion = insertInto(inSquareBrackets(relation.name.value), relation.body, relation.identity)
    val alteration  = alterTable(inSquareBrackets(relation.name.value), relation.relationConstraints)
    str"$creation\n\n$insertion\n\n$alteration"
  }

  private def alterTable(name: String, constraints: List[RelationConstraint]): String = {
    constraints.reverse.map {
      case FKeyRelationConstraint(names, pKeyRelationName, pKeyColumnName, _, _, constraintName) =>
        str"ALTER TABLE $name ADD CONSTRAINT ${constraintName.getOrElse("")} FOREIGN KEY(${names.mkString(",")}) REFERENCES ${pKeyRelationName.value} (${pKeyColumnName.value})"
      case DefaultRelationConstraint(columnName, value, constraintName) =>
        str"ALTER TABLE $name ADD CONSTRAINT ${constraintName.getOrElse("")} DEFAULT (${value.show}) for $columnName"
      case CheckRelationConstraint(condition, constraintName) =>
        str"ALTER TABLE $name ADD CONSTRAINT ${constraintName.getOrElse("")} CHECK (${condition.show})"
        // other constrains are serialized during create table query
      case _ => ""
    }.mkString("\n\n")
  }

  private def createTable(name: String, heading: Relation.Header, constraints: List[RelationConstraint]): String = {
    val attributesWithoutNamedConstraints = heading.map {h =>
      h.copy(constraints = h.constraints.filter(_.name.isEmpty))
    }
    val attributes = attributesWithoutNamedConstraints
      .map { attrib =>
        str"${inSquareBrackets(attrib.name.value)} ${attrib.domain.show} ${attrib.constraints.map(_.show).mkString(" ")}"
      }
      .mkString(",\n")
    val pKeysConstraint = constraints.collect {
      case c@PKeyRelationConstraint(_, _) => c
    }
    val primaryKeysString = pKeysConstraint match {
      case Nil => ""
      case head :: _ => str" CONSTRAINT ${head.constraintName.getOrElse("")} PRIMARY KEY(${head.names.mkString(",")})\n"
    }
    str"CREATE TABLE $name (\n$attributes,\n $primaryKeysString)"
  }

  private def insertInto(name: String, rows: List[Row], identityOption: Option[Identity]): String = {
    def rowWithoutIdentity(row: Row): Row = identityOption match {
      case Some(identity) => row.filter(_.name != identity.name)
      case None => row
    }
    val attributes = rows.reverse
      .map { row =>
        val filteredRow = rowWithoutIdentity(row)
        val literals = filteredRow.attributes.map(_.literal.show).mkString(",")
        str"($literals)"
      }
      .mkString(",\n")
    if (attributes.nonEmpty) {
      val names = rowWithoutIdentity(rows.head).attributes.map(_.name.value).mkString(",")
      str"INSERT INTO $name ($names) VALUES \n$attributes\n"
    }
    else ""
  }

  private def inSquareBrackets(name: String): String =
    str"[$name]"

  def serialize(schema: Schema): String = schema.relations.values.map(serialize).toList.mkString("")

  def deserialize(t: String): Either[SQLError, Schema] = {
    val Parsed.Success(trees, _) = SQLParser.parseMany(t)
    trees.foldLeftM(Schema()) {
      case (schema, tree) =>
        SQLInterpreter.run(tree, schema).map {
          case SchemaResult(newSchema) => newSchema
          case RowsResult(_) => schema
        }
    }
  }
}
