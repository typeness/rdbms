package io.github.typeness.rdbms
import org.scalatest.FunSuite

class IntegrationTest extends FunSuite {
  test("Create empty schema") {
    val Right(SchemaResult(schema)) = SQLInterpreter.runFromResource("empty.sql")
    val Right(RowsResult(rows)) = SQLInterpreter.runFromResource("t1.sql", schema)
    assert(rows == Nil)
  }
}
