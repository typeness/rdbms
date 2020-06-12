package io.github.typeness.rdbms

import io.github.typeness.rdbms.TestUtils.readFromSQLFile
import org.scalatest.funsuite.AnyFunSuite

class SchemaFormatBinaryTest extends AnyFunSuite {
  test("Serialize and deserialize northwind database") {
    for {
      schema <- readFromSQLFile("northwind.sql")
      serialized = SchemaFormatBinary.serialize(schema)
      schema2 <- SchemaFormatBinary.deserialize(serialized)
    } yield assert(schema2 == schema)
  }
}
