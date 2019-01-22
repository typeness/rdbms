package io.github.typeness.rdbms
import org.scalatest.FunSuite

import fastparse._

class IntegrationTest extends FunSuite {

  private def createSchema(source: String): Either[SQLError, Schema] = {
    val code = scala.io.Source.fromResource(source).mkString
    val Parsed.Success(trees, _) = SQLParser.parseMany(code)
    trees.foldLeft[Either[SQLError, Schema]](Right(Schema(Nil))) {
      case (Right(schema), tree) =>
        SQLInterpreter.run(tree, schema) match {
          case Right(SchemaResult(newSchema)) => Right(newSchema)
          case Right(RowsResult(_)) => Right(schema)
          case Left(error) => Left(error)
        }
      case (Left(error), _) => Left(error)
    }
  }

  test("Create empty schema") {
    val Right(SchemaResult(schema)) = SQLInterpreter.runFromResource("empty.sql")
    val Right(RowsResult(rows)) = SQLInterpreter.runFromResource("t1.sql", schema)
    assert(rows == Nil)
  }

  test("Create PracownicyUrlopySchema") {
    val schema = createSchema("pracownicyUrlopySchema.sql")
  }
}
