package io.github.typeness.rdbms

import fastparse.Parsed
import cats.syntax.foldable._
import cats.instances.list._
import cats.instances.either._

object TestUtils {
  def createSchemaFromFile(source: String): Either[SQLError, Schema] =
    createSchemaFromSQL(scala.io.Source.fromResource(source).mkString)

  def createSchemaFromSQL(sql: String): Either[SQLError, Schema] = {
    val Parsed.Success(trees, _) = SQLParser.parseMany(sql)
    trees.foldLeftM(Schema(Nil)) {
      case (schema, tree) =>
        SQLInterpreter.run(tree, schema).map {
          case SchemaResult(newSchema) => newSchema
          case RowsResult(_) => schema
        }
    }
  }

}
