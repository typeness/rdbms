package io.github.typeness.rdbms

import io.github.typeness.rdbms.TestUtils._
import org.scalatest.funsuite.AnyFunSuite

class SchemaSerializerToSQLTest extends AnyFunSuite {
  test("Serialize and parse back northwind database") {
    for {
      schema <- createSchemaFromFile("northwind.sql")
      serialized = SchemaSerializerToSQL.serialize(schema).mkString("")
      _ = println(serialized)
      schema2 <- createSchemaFromSQL(serialized)
    } yield assert(schema2 == schema)
  }
}
