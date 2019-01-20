package io.github.typeness.rdbms

import org.scalatest.FunSuite

class ManipulationBuilderTest extends FunSuite {

  import Relations._

  test("Successful anonymous row insert into table Pracownicy") {
    /*
      INSERT INTO Pracownicy VALUES
      (6,'Kowal','Piotr', 500, '2010-01-01',2)
     */
    val row = List(
      IntegerLiteral(6),
      StringLiteral("Kowal"),
      StringLiteral("Piotr"),
      MoneyLiteral(500),
      DateLiteral("2010-01-01"),
      IntegerLiteral(2),
    )
    val query = AnonymousInsert(
      "Pracownicy",
      row
    )
    val containsRow = for {
      newRelation <- ManipulationBuilder.insertRow(query, pracownicyRelation)
      rows = newRelation.body.map(_.getValues)
    } yield rows.contains(row)
    assert(containsRow == Right(true))
  }

  test("Successful named row insert into table Pracownicy") {
    /*
      INSERT INTO Pracownicy(Nr,Nazwisko,Imie) VALUES
      (6,'Kowal','Piotr')
     */
    val row = Row(
      BodyAttribute("Nr", IntegerLiteral(6)),
      BodyAttribute("Nazwisko", StringLiteral("Kowal")),
      BodyAttribute("Imie", StringLiteral("Piotr")),
    )
    val query = NamedInsert(
      "Pracownicy",
      row
    )
    val rows = for {
      newRelation <- ManipulationBuilder.insertRow(query, pracownicyRelation)
      rows = newRelation.body
    } yield rows
    assert(
      rows.map(
        _.contains(
          Row(row.attributes ::: List(BodyAttribute("Stawka", NULLLiteral),
                                      BodyAttribute("DataZatrudnienia", NULLLiteral),
                                      BodyAttribute("LiczbaDzieci", NULLLiteral))))) == Right(true)
    )
  }

  test("Insert row with missing primary key 'Nr' from Pracownicy") {}

  test("DELETE FROM Pracownicy WHERE Nr=1") {
    val query = Delete("Pracownicy", Some(Equals("Nr", IntegerLiteral(1))))
    val isDeleted = for {
      newSchema <- ManipulationBuilder.deleteRows(query, pracownicyRelation, schemaPracownicy)
      newRelation <- newSchema.getRelation("Pracownicy")
      rows = newRelation.body
    } yield !rows.contains(pracownicyRow1)
    assert(isDeleted.contains(true))
  }

  val pracownicyRelationWithIdentity = Relation(
    "Pracownicy2",
    List("Nr"),
    Some(Identity("Nr", 1, 1)),
    List(
      HeadingAttribute("Nr", IntegerType, List(PrimaryKey)),
      HeadingAttribute("Nazwisko", NVarCharType(50), List(NotNULL)),
      HeadingAttribute("Imie", NVarCharType(50), List(NotNULL)),
      HeadingAttribute("Stawka", MoneyType, Nil),
      HeadingAttribute("DataZatrudnienia", DateType, Nil),
      HeadingAttribute("LiczbaDzieci", IntegerType, Nil),
    ),
    Nil
  )

  test("Anonymous insert into table with defined Identity") {
    /*
      INSERT INTO Pracownicy2 VALUES
      ('Kowal','Piotr', 500, '2010-01-01',2)
     */
    val row = List(
      StringLiteral("Kowal"),
      StringLiteral("Piotr"),
      MoneyLiteral(500),
      DateLiteral("2010-01-01"),
      IntegerLiteral(2),
    )
    val rowWithID = IntegerLiteral(1) :: row
    val query = AnonymousInsert(
      "Pracownicy2",
      row
    )
    val containsRow = for {
      newRelation <- ManipulationBuilder.insertRow(query, pracownicyRelationWithIdentity)
      rows = newRelation.body.map(_.getValues)
    } yield rows.contains(rowWithID)
    assert(containsRow == Right(true))
  }

  test("Identity violation") {
    /*
      INSERT INTO Pracownicy2 (Nr,Nazwisko,Imie,Stawka,DataZatrudnienia, LiczbaDzieci) VALUES
      (1,'Nowak','Anna', 1600, '2012-01-01',2)
     */
    val query = NamedInsert(
      "Pracownicy2",
      Row(
        BodyAttribute("Nr", IntegerLiteral(1)),
        BodyAttribute("Nazwisko", StringLiteral("Anna")),
        BodyAttribute("Imie", StringLiteral("Nowak")),
        BodyAttribute("Stawka", IntegerLiteral(1600)),
        BodyAttribute("DataZatrudnienia", StringLiteral("2012-01-01")),
        BodyAttribute("LiczbaDzieci", IntegerLiteral(2)),
      )
    )

    val violation = for {
      newRelation <- ManipulationBuilder.insertRow(query, pracownicyRelationWithIdentity)
    } yield newRelation
    assert(violation == Left(IdentityViolation("Nr")))
  }

  test("Named insert into table with defined Identity") {
    /*
      INSERT INTO Pracownicy2 (Nr,Nazwisko,Imie,Stawka,DataZatrudnienia, LiczbaDzieci) VALUES
      (1,'Nowak','Anna', 1600, '2012-01-01',2)
     */
    val row = Row(
      BodyAttribute("Nazwisko", StringLiteral("Anna")),
      BodyAttribute("Imie", StringLiteral("Nowak")),
      BodyAttribute("Stawka", MoneyLiteral(1600)),
      BodyAttribute("DataZatrudnienia", DateLiteral("2012-01-01")),
      BodyAttribute("LiczbaDzieci", IntegerLiteral(2)),
    )
    val rowWithId1 =
      Row(BodyAttribute("Nr", IntegerLiteral(1)) :: row.attributes)
    val rowWithId2 =
      Row(BodyAttribute("Nr", IntegerLiteral(2)) :: row.attributes)
    val query1 = NamedInsert(
      "Pracownicy2",
      row
    )
    val query2 = NamedInsert(
      "Pracownicy2",
      row
    )
    val hasRow = for {
      newRelation1 <- ManipulationBuilder.insertRow(query1, pracownicyRelationWithIdentity)
      newRelation2 <- ManipulationBuilder.insertRow(query2, newRelation1)
      rows = newRelation2.body
    } yield
      rows.contains(rowWithId1) &&
        rows.contains(rowWithId2) && newRelation2.identity.contains(Identity("Nr", 3, 1))
    assert(hasRow == Right(true))
  }

  test("UPDATE Pracownicy SET Stawka = 1234, LiczbaDzieci=3 WHERE Nr=1") {
    val query = Update(
      "Pracownicy",
      Row(
        BodyAttribute("Stawka", MoneyLiteral(1234)),
        BodyAttribute("LiczbaDzieci", IntegerLiteral(3))
      ),
      Some(Equals("Nr", IntegerLiteral(1)))
    )
    val expectedRow = Row(
      BodyAttribute("Nr", IntegerLiteral(1)),
      BodyAttribute("Nazwisko", StringLiteral("Kowalski")),
      BodyAttribute("Imie", StringLiteral("Jan")),
      BodyAttribute("Stawka", MoneyLiteral(1234)),
      BodyAttribute("DataZatrudnienia", DateLiteral("2010-01-01")),
      BodyAttribute("LiczbaDzieci", IntegerLiteral(3))
    )
    val isUpdated = for {
      newSchema <- ManipulationBuilder.updateRows(query, pracownicyRelation, schemaPracownicy)
      newRelation <- newSchema.getRelation("Pracownicy")
      rows = newRelation.body
    } yield rows.contains(expectedRow) && !rows.contains(pracownicyRow1)
    assert(isUpdated == Right(true))
  }

  test("UPDATE when WHERE is not matching any rows") {
    val query = Update(
      "Pracownicy",
      Row(
        BodyAttribute("Stawka", IntegerLiteral(1234)),
      ),
      Some(Equals("Nr", IntegerLiteral(999)))
    )
    val noEffect = for {
      newSchema <- ManipulationBuilder.updateRows(query, pracownicyRelation, schemaPracownicy)
      newRelation <- newSchema.getRelation("Pracownicy")
    } yield newRelation.body == pracownicyRelation.body
    assert(noEffect == Right(true))
  }

  test("UNIQUE violation") {
    /*
     INSERT INTO Pracownicy VALUES
     (1,'Kowal','Piotr', 500, '2010-01-01',2)
    */
    val row = List(
      IntegerLiteral(1),
      StringLiteral("Kowal"),
      StringLiteral("Piotr"),
      MoneyLiteral(500),
      DateLiteral("2010-01-01"),
      IntegerLiteral(2),
    )
    val query = AnonymousInsert(
      "Pracownicy",
      row
    )
    val containsRow = for {
      newRelation <- ManipulationBuilder.insertRow(query, pracownicyRelation)
      rows = newRelation.body.map(_.getValues)
    } yield rows.contains(row)
    assert(containsRow == Left(UniqueViolation(BodyAttribute("Nr", IntegerLiteral(1)))))
  }

}
