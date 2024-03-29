package io.github.typeness.rdbms
import fastparse._

sealed trait InterpreterResult
case class SchemaResult(schema: Schema) extends InterpreterResult
case class RowsResult(rows: List[Row]) extends InterpreterResult

object SQLInterpreter {
  def runFromFile(filename: String,
                  schema: Schema = Schema()): Either[SQLError, InterpreterResult] = {
    val source = scala.io.Source.fromFile(filename)
    try {
      runFromSource(source.mkString, schema)
    } finally {
      source.close()
    }
  }

  def runFromResource(resource: String,
                      schema: Schema = Schema()): Either[SQLError, InterpreterResult] = {
    val source = scala.io.Source.fromResource(resource)
    try {
      runFromSource(source.mkString, schema)
    } finally {
      source.close()
    }
  }

  def runFromSource(source: String,
                    schema: Schema = Schema()): Either[SQLError, InterpreterResult] = {
    val Parsed.Success(tree, _) = SQLParser.parse(source)
    run(tree, schema)
  }

  def run(tree: SQL, schema: Schema): Either[SQLError, InterpreterResult] = tree match {
    case manipulation: Manipulation =>
      ManipulationBuilder.run(manipulation, schema).map(SchemaResult)
    case definition: Definition => RelationBuilder.run(definition, schema).map(SchemaResult)
    case _: Control             => ???
    case query: Query           => QueryBuilder.run(query, schema).map(RowsResult)
  }
}
