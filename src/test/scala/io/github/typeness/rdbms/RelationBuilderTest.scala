package io.github.typeness.rdbms

import org.scalatest.FunSuite

class RelationBuilderTest extends FunSuite {
  test("Table definition with multiple primary keys") {
    /*
    CREATE TABLE Urlopy(
      NrPrac INT PRIMARY KEY,
      OdKiedy DATE PRIMARY KEY,
      DoKiedy DATE
    )
     */
    val query = Create(
      "Urlopy",
      List(
        HeadingAttribute("NrPrac", IntegerType, List(PrimaryKey)),
        HeadingAttribute("OdKiedy", DateType, List(PrimaryKey)),
        HeadingAttribute("DoKiedy", DateType, Nil)
      ),
      Nil
    )
    assert(RelationBuilder.build(query) == Left(MultiplePrimaryKeys))
  }
  test("Non-unique names in table definition") {
    /*
    CREATE TABLE Test(
      name INT PRIMARY KEY
      name DATE
    )
     */
    val query = Create(
      "Test",
      List(
        HeadingAttribute("name", IntegerType, List(PrimaryKey)),
        HeadingAttribute("name", DateType, Nil)
      ),
      Nil
    )
    assert(RelationBuilder.build(query) == Left(MultipleColumnNames("name")))
  }
  test("Primary key referencing non-existing column name") {
    /*
    CREATE TABLE Test(
      name STRING
      PRIMARY KEY(id)
    )
     */
    val query = Create(
      "Test",
      List(
        HeadingAttribute("name", StringType, Nil)
      ),
      List("id")
    )
    assert(RelationBuilder.build(query) == Left(ColumnDoesNotExists("id")))
  }
}
