package io.github.typeness.rdbms

import org.scalatest.funsuite.AnyFunSuite

class RelationBuilderTest extends AnyFunSuite {
  test("Table definition with multiple primary keys") {
    /*
    CREATE TABLE Urlopy(
      NrPrac INT PRIMARY KEY,
      OdKiedy DATE PRIMARY KEY,
      DoKiedy DATE
    )
     */
    val query = Create(
      rel"Urlopy",
      List(
        HeadingAttribute(col"NrPrac", IntegerType, List(PrimaryKey())),
        HeadingAttribute(col"OdKiedy", DateType, List(PrimaryKey())),
        HeadingAttribute(col"DoKiedy", DateType, Nil)
      ),
      Nil,
      None
    )
    assert(RelationBuilder.run(query, Schema()) == Left(MultiplePrimaryKeys(col"NrPrac", col"OdKiedy")))
  }
  test("Non-unique names in table definition") {
    /*
    CREATE TABLE Test(
      name INT PRIMARY KEY,
      name DATE,
    )
     */
    val query = Create(
      rel"Test",
      List(
        HeadingAttribute(col"name", IntegerType, List(PrimaryKey())),
        HeadingAttribute(col"name", DateType, Nil)
      ),
      Nil,
      None
    )
    assert(RelationBuilder.run(query, Schema()) == Left(MultipleColumnNames(col"name")))
  }
  test("Primary key referencing non-existing column name") {
    /*
    CREATE TABLE Test(
      name NVARCHAR(50),
      PRIMARY KEY(id)
    )
     */
    val query = Create(
      rel"Test",
      List(
        HeadingAttribute(col"name", NVarCharType(50), Nil)
      ),
      List(PKeyRelationConstraint(List("id"))),
      None
    )
    assert(RelationBuilder.run(query, Schema()) == Left(ColumnDoesNotExists(col"id")))
  }

  test("DROP TABLE Pracownicy1") {
    val relation1 = Relation(
      rel"Pracownicy1",
      Nil,
      None,
      List(
        HeadingAttribute(col"Nr", IntegerType, List(PrimaryKey())),
        HeadingAttribute(col"Nazwisko", NVarCharType(50), List(NotNULL())),
      ),
      Nil,
      Nil
    )
    val relation2 = Relation(
      rel"Pracownicy2",
      Nil,
      None,
      List(
        HeadingAttribute(col"Nr", IntegerType, List(PrimaryKey())),
        HeadingAttribute(col"Imie", NVarCharType(50), List(NotNULL())),
      ),
      Nil,
      Nil
    )
    val schema = Schema(
        relation1,
        relation2
    )
    val query = DropTable(rel"Pracownicy1")
    val Right(newSchema) = for {
      newSchema <- RelationBuilder.run(query, schema)
    } yield newSchema
    assert(!newSchema.relations.has(relation1.name) && newSchema.relations.has(relation2.name))
  }
}
