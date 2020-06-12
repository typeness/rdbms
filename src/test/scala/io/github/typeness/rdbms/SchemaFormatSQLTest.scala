package io.github.typeness.rdbms

import io.github.typeness.rdbms.TestUtils._
import org.scalatest.funsuite.AnyFunSuite

class SchemaFormatSQLTest extends AnyFunSuite {
  test("Serialize and parse back northwind database") {
    for {
      schema <- readFromSQLFile("northwind.sql")
      serialized = SchemaFormatSQL.serialize(schema)
      schema2 <- SchemaFormatSQL.deserialize(serialized)
    } yield assert(schema2 == schema)
  }
}
