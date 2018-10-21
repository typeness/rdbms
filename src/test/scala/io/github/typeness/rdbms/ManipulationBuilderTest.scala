package io.github.typeness.rdbms

import org.scalatest.FunSuite

class ManipulationBuilderTest extends FunSuite {

  /*
  CREATE TABLE Pracownicy(
    Nr INT PRIMARY KEY,
    Nazwisko NVARCHAR(50) NOT NULL,
    Imie NVARCHAR(50) NOT NULL,
    Stawka MONEY,
    DataZatrudnienia DATE,
    LiczbaDzieci TINYINT
  )
   */
  val pracownicy: Either[SQLError, Relation] = RelationBuilder.build(
    Create(
      "Pracownicy",
      List(
        HeadingAttribute("Nr", IntegerType, List(PrimaryKey)),
        HeadingAttribute("Nazwisko", StringType, List(NotNULL)),
        HeadingAttribute("Imie", StringType, List(NotNULL)),
        HeadingAttribute("Stawka", MoneyType, Nil),
        HeadingAttribute("DataZatrudnienia", IntegerType, Nil),
        HeadingAttribute("LiczbaDzieci", IntegerType, Nil),

      ),
      Nil
    )
  )

  test("Successful anonymous row insert into table Pracownicy") {
    /*
      INSERT INTO Pracownicy VALUES
      (1,'Kowal','Piotr', 500, '2010-01-01',2)
     */
    val row = List(
      Value(IntegerLiteral(1)),
      Value(StringLiteral("Kowal")),
      Value(StringLiteral("Piotr")),
      Value(IntegerLiteral(500)),
      Value(StringLiteral("2010-01-01")),
      Value(IntegerLiteral(2)),
    )
    val query = AnonymousInsert(
      "Pracownicy",
      row
    )
    val containsRow = for {
      relation <- pracownicy
      newRelation <- ManipulationBuilder.insertRow(query, relation)
      rows = newRelation.body.map(_.map(_.value))
    } yield rows.contains(row)
    assert(containsRow == Right(true))
  }

  test("Successful named row insert into table Pracownicy") {
    /*
      INSERT INTO Pracownicy(Nr,Nazwisko,Imie) VALUES
      (1,'Kowal','Piotr')
     */
    val row = List(
      BodyAttribute("Nr", Value(IntegerLiteral(1))),
      BodyAttribute("Nazwisko", Value(StringLiteral("Kowal"))),
      BodyAttribute("Imie", Value(StringLiteral("Piotr"))),
    )
    val query = NamedInsert(
      "Pracownicy",
      row
    )
    val rows = for {
      relation <- pracownicy
      newRelation <- ManipulationBuilder.insertRow(query, relation)
      rows = newRelation.body
    } yield rows
    assert(
      rows.map(_.contains(
        List(
          BodyAttribute("Stawka", Value(NULLLiteral)),
          BodyAttribute("DataZatrudnienia", Value(NULLLiteral)),
          BodyAttribute("LiczbaDzieci", Value(NULLLiteral)),
        ) ::: row
      )) == Right(true)
    )
  }

  test("Insert row with missing primary key 'Nr' from Pracownicy") {

  }
}
