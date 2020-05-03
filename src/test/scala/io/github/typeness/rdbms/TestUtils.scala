package io.github.typeness.rdbms

import fastparse.Parsed
import cats.syntax.foldable._

object TestUtils extends App {
  def createSchemaFromFile(source: String): Either[SQLError, Schema] =
    createSchemaFromSQL(scala.io.Source.fromResource(source).mkString)

  def createSchemaFromSQL(sql: String): Either[SQLError, Schema] = {
    val Parsed.Success(trees, _) = SQLParser.parseMany(sql)
    trees.foldLeftM(Schema()) {
      case (schema, tree) =>
        SQLInterpreter.run(tree, schema).map {
          case SchemaResult(newSchema) => newSchema
          case RowsResult(_) => schema
        }
    }
  }

}
