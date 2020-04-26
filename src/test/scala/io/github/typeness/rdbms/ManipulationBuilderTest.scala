package io.github.typeness.rdbms

import org.scalatest.funsuite.AnyFunSuite

class ManipulationBuilderTest extends AnyFunSuite {

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
      rel"Pracownicy",
      List(row)
    )
    val Right(rows) = for {
      newSchema <- ManipulationBuilder.run(query, schemaPracownicyUrlopy)
      newRelation <- newSchema.getRelation(rel"Pracownicy")
      rows = newRelation.body.map(_.getValues)
    } yield rows
    assert(rows.contains(row))
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
      rel"Pracownicy",
      List(row)
    )
    val rows = for {
      newSchema <- ManipulationBuilder.run(query, schemaPracownicyUrlopy)
      newRelation <- newSchema.getRelation(rel"Pracownicy")
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
    val query = Delete(rel"Pracownicy", Some(Equals(Var("Nr"), IntegerLiteral(1))))
    val Right(rows) = for {
      newSchema <- ManipulationBuilder.run(query, schemaPracownicyUrlopy)
      newRelation <- newSchema.getRelation(rel"Pracownicy")
      rows = newRelation.body
    } yield rows
    assert(!rows.contains(pracownicyRow1))
  }

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
      rel"Pracownicy2",
      List(row)
    )
    val Right(rows) = for {
      newSchema <- ManipulationBuilder.run(query, schemaPracownicy2)
      newRelation <- newSchema.getRelation(rel"Pracownicy2")
      rows = newRelation.body.map(_.getValues)
    } yield rows
    assert(rows.contains(rowWithID))
  }

  test("Identity violation") {
    /*
      INSERT INTO Pracownicy2 (Nr,Nazwisko,Imie,Stawka,DataZatrudnienia, LiczbaDzieci) VALUES
      (1,'Nowak','Anna', 1600, '2012-01-01',2)
     */
    val query = NamedInsert(
      rel"Pracownicy2",
      List(Row(
        BodyAttribute("Nr", IntegerLiteral(1)),
        BodyAttribute("Nazwisko", StringLiteral("Anna")),
        BodyAttribute("Imie", StringLiteral("Nowak")),
        BodyAttribute("Stawka", IntegerLiteral(1600)),
        BodyAttribute("DataZatrudnienia", StringLiteral("2012-01-01")),
        BodyAttribute("LiczbaDzieci", IntegerLiteral(2)),
      ))
    )

    val violation = for {
      newSchema <- ManipulationBuilder.run(query, schemaPracownicy2)
    } yield newSchema
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
      rel"Pracownicy2",
      List(row)
    )
    val query2 = NamedInsert(
      rel"Pracownicy2",
      List(row)
    )
    val hasRow = for {
      newSchema1 <- ManipulationBuilder.run(query1, schemaPracownicy2)
      newSchema2 <- ManipulationBuilder.run(query2, newSchema1)
      newRelation2 <- newSchema2.getRelation(rel"Pracownicy2")
      rows = newRelation2.body
    } yield
      rows.contains(rowWithId1) &&
        rows.contains(rowWithId2) && newRelation2.identity.contains(Identity("Nr", 3, 1))
    assert(hasRow == Right(true))
  }

  test("UPDATE Pracownicy SET Stawka = 1234, LiczbaDzieci=3 WHERE Nr=1") {
    val query = Update(
      rel"Pracownicy",
      Row(
        BodyAttribute("Stawka", MoneyLiteral(1234)),
        BodyAttribute("LiczbaDzieci", IntegerLiteral(3))
      ),
      Some(Equals(Var("Nr"), IntegerLiteral(1)))
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
      newSchema <- ManipulationBuilder.run(query, schemaPracownicyUrlopy)
      newRelation <- newSchema.getRelation(rel"Pracownicy")
      rows = newRelation.body
    } yield rows.contains(expectedRow) && !rows.contains(pracownicyRow1)
    assert(isUpdated == Right(true))
  }

  test("UPDATE when WHERE is not matching any rows") {
    val query = Update(
      rel"Pracownicy",
      Row(
        BodyAttribute("Stawka", IntegerLiteral(1234)),
      ),
      Some(Equals(Var("Nr"), IntegerLiteral(999)))
    )
    val Right(newRelation) = for {
      newSchema <- ManipulationBuilder.run(query, schemaPracownicyUrlopy)
      newRelation <- newSchema.getRelation(rel"Pracownicy")
    } yield newRelation
    assert(newRelation.body == pracownicy.body)
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
      rel"Pracownicy",
      List(row)
    )
    val error = for {
      newSchema <- ManipulationBuilder.run(query, schemaPracownicyUrlopy)
      newRelation <- newSchema.getRelation(rel"Pracownicy")
      error = newRelation.body.map(_.getValues)
    } yield error
    assert(error == Left(PrimaryKeyDuplicate(List(BodyAttribute("Nr", IntegerLiteral(1))))))
  }

  test("PrimaryKeyDoesNotExist when inserting new row to relation with foreign key") {
    val insert = AnonymousInsert(
      rel"Urlopy",
      List(List(IntegerLiteral(123456), DateLiteral("2010-11-11"), DateLiteral("2010-11-12"))))
    val error = ManipulationBuilder.run(insert, schemaPracownicyUrlopy)
    assert(
      error == Left(
        PrimaryKeyDoesNotExist(rel"Urlopy", "NrPrac", rel"Pracownicy", "Nr", IntegerLiteral(123456))))
  }

  test("No PrimaryKeyDoesNotExist when inserting new row to relation with foreign key") {
    val row = List(IntegerLiteral(1), DateLiteral("2010-11-11"), DateLiteral("2010-11-12"))
    val insert = AnonymousInsert(rel"Urlopy", List(row))
    val newUrlopy =
      ManipulationBuilder.run(insert, schemaPracownicyUrlopy).flatMap(_.getRelation(rel"Urlopy")).map(_.body)
    assert(newUrlopy.map(_.map(_.attributes.map(_.literal)).contains(row)) == Right(true))
  }

  test("ForeignKeyViolation when deleting primary key") {
    val delete = Delete(rel"Pracownicy", Some(Equals(Var("Nr"), IntegerLiteral(5))))
    val error = ManipulationBuilder.run(delete, schemaPracownicyUrlopy)
    assert(error == Left(ForeignKeyViolation(rel"Urlopy", "NrPrac")))
  }

}
