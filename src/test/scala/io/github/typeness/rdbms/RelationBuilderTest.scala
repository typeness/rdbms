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
      Nil,
      None
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
      Nil,
      None
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
      List("id"),
      None
    )
    assert(RelationBuilder.build(query) == Left(ColumnDoesNotExists("id")))
  }

  test("DROP TABLE Pracownicy1") {
    val relation1 = Relation(
      "Pracownicy1",
      Nil,
      None,
      List(
        HeadingAttribute("Nr", IntegerType, List(PrimaryKey)),
        HeadingAttribute("Nazwisko", StringType, List(NotNULL)),
      ),
      Nil
    )
    val relation2 = Relation(
      "Pracownicy2",
      Nil,
      None,
      List(
        HeadingAttribute("Nr", IntegerType, List(PrimaryKey)),
        HeadingAttribute("Imie", StringType, List(NotNULL)),
      ),
      Nil
    )
    val schema = Schema(
      List(
        relation1,
        relation2
      )
    )
    val query = Drop("Pracownicy1")
    val isDeleted = for {
      newSchema <- RelationBuilder.drop(query, schema)
    } yield
      !newSchema.relations.contains(relation1) && newSchema.relations.contains(
        relation2)
    assert(isDeleted == Right(true))
  }
}
