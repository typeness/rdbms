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
        HeadingAttribute("NrPrac", IntegerType, List(PrimaryKey())),
        HeadingAttribute("OdKiedy", DateType, List(PrimaryKey())),
        HeadingAttribute("DoKiedy", DateType, Nil)
      ),
      Nil,
      None
    )
    assert(RelationBuilder.run(query, Schema(Nil)) == Left(MultiplePrimaryKeys("NrPrac", "OdKiedy")))
  }
  test("Non-unique names in table definition") {
    /*
    CREATE TABLE Test(
      name INT PRIMARY KEY,
      name DATE,
    )
     */
    val query = Create(
      "Test",
      List(
        HeadingAttribute("name", IntegerType, List(PrimaryKey())),
        HeadingAttribute("name", DateType, Nil)
      ),
      Nil,
      None
    )
    assert(RelationBuilder.run(query, Schema(Nil)) == Left(MultipleColumnNames("name")))
  }
  test("Primary key referencing non-existing column name") {
    /*
    CREATE TABLE Test(
      name NVARCHAR(50),
      PRIMARY KEY(id)
    )
     */
    val query = Create(
      "Test",
      List(
        HeadingAttribute("name", NVarCharType(50), Nil)
      ),
      List(PKeyRelationConstraint(List("id"))),
      None
    )
    assert(RelationBuilder.run(query, Schema(Nil)) == Left(ColumnDoesNotExists("id")))
  }

  test("DROP TABLE Pracownicy1") {
    val relation1 = Relation(
      "Pracownicy1",
      Nil,
      None,
      List(
        HeadingAttribute("Nr", IntegerType, List(PrimaryKey())),
        HeadingAttribute("Nazwisko", NVarCharType(50), List(NotNULL())),
      ),
      Nil,
      Nil
    )
    val relation2 = Relation(
      "Pracownicy2",
      Nil,
      None,
      List(
        HeadingAttribute("Nr", IntegerType, List(PrimaryKey())),
        HeadingAttribute("Imie", NVarCharType(50), List(NotNULL())),
      ),
      Nil,
      Nil
    )
    val schema = Schema(
      List(
        relation1,
        relation2
      )
    )
    val query = DropTable("Pracownicy1")
    val Right(newSchema) = for {
      newSchema <- RelationBuilder.run(query, schema)
    } yield newSchema
    assert(!newSchema.relations.contains(relation1.name) && newSchema.relations.contains(relation2.name))
  }
}
