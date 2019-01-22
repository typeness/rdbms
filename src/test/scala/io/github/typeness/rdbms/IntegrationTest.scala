package io.github.typeness.rdbms
import org.scalatest.FunSuite

class IntegrationTest extends FunSuite {
  test("Urlopy") {
    val Right(SchemaResult(schema)) = SQLInterpreter.runFromResource("schema.sql")
    val Right(RowsResult(rows)) = SQLInterpreter.runFromResource("t1.sql", schema)
    assert(rows == Nil)
  }
}
