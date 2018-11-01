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
      Nil,
      None
    )
  )

  test("Successful anonymous row insert into table Pracownicy") {
    /*
      INSERT INTO Pracownicy VALUES
      (1,'Kowal','Piotr', 500, '2010-01-01',2)
     */
    val row = List(
      IntegerLiteral(1),
      StringLiteral("Kowal"),
      StringLiteral("Piotr"),
      IntegerLiteral(500),
      StringLiteral("2010-01-01"),
      IntegerLiteral(2),
    )
    val query = AnonymousInsert(
      "Pracownicy",
      row
    )
    val containsRow = for {
      relation <- pracownicy
      newRelation <- ManipulationBuilder.insertRow(query, relation)
      rows = newRelation.body.map(_.getValues)
    } yield rows.contains(row)
    assert(containsRow == Right(true))
  }

  test("Successful named row insert into table Pracownicy") {
    /*
      INSERT INTO Pracownicy(Nr,Nazwisko,Imie) VALUES
      (1,'Kowal','Piotr')
     */
    val row = Row(
      BodyAttribute("Nr", IntegerLiteral(1)),
      BodyAttribute("Nazwisko", StringLiteral("Kowal")),
      BodyAttribute("Imie", StringLiteral("Piotr")),
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
      rows.map(_.contains(Row(List(
        BodyAttribute("Stawka", NULLLiteral),
        BodyAttribute("DataZatrudnienia", NULLLiteral),
        BodyAttribute("LiczbaDzieci", NULLLiteral),
      ) ::: row.attributes)
      )) == Right(true)
    )
  }

  test("Insert row with missing primary key 'Nr' from Pracownicy") {

  }

  /*
     INSERT INTO Pracownicy VALUES
     (1, 'Kowalski', 'Jan', 1500, '2010-01-01', 2)
    */
  val row1 = Row(
    BodyAttribute("Nr", IntegerLiteral(1)),
    BodyAttribute("Nazwisko", StringLiteral("Kowalski")),
    BodyAttribute("Imie", StringLiteral("Jan")),
    BodyAttribute("Stawka", IntegerLiteral(1500)),
    BodyAttribute("DataZatrudnienia", StringLiteral("2010-01-01")),
    BodyAttribute("LiczbaDzieci", IntegerLiteral(2)),

  )
  /*
   INSERT INTO Pracownicy VALUES
   (2, 'Nowak','Anna', 1600, '2012-01-01',2)
  */
  val row2 = Row(
    BodyAttribute("Nr", IntegerLiteral(2)),
    BodyAttribute("Nazwisko", StringLiteral("Nowak")),
    BodyAttribute("Imie", StringLiteral("Anna")),
    BodyAttribute("Stawka", IntegerLiteral(1600)),
    BodyAttribute("DataZatrudnienia", StringLiteral("2012-01-01")),
    BodyAttribute("LiczbaDzieci", IntegerLiteral(2)),

  )
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
  val relation = Relation(
    "Pracownicy",
    Nil,
    None,
    List(
      HeadingAttribute("Nr", IntegerType, List(PrimaryKey)),
      HeadingAttribute("Nazwisko", StringType, List(NotNULL)),
      HeadingAttribute("Imie", StringType, List(NotNULL)),
      HeadingAttribute("Stawka", MoneyType, Nil),
      HeadingAttribute("DataZatrudnienia", IntegerType, Nil),
      HeadingAttribute("LiczbaDzieci", IntegerType, Nil),
    ),
    List(
      row1,
      row2
    )
  )


  test("DELETE FROM Pracownicy WHERE Nr=1") {
    val query = Delete("Pracownicy", Some(Equals("Nr", IntegerLiteral(1))))
    val isDeleted = for {
      newRelation <- ManipulationBuilder.deleteRows(query, relation)
      rows = newRelation.body
    } yield !rows.contains(row1)
    assert(isDeleted.contains(true))
  }

  val pracownicy2: Either[SQLError, Relation] = RelationBuilder.build(
    Create(
      "Pracownicy2",
      List(
        HeadingAttribute("Nr", IntegerType, List(PrimaryKey)),
        HeadingAttribute("Nazwisko", StringType, List(NotNULL)),
        HeadingAttribute("Imie", StringType, List(NotNULL)),
        HeadingAttribute("Stawka", MoneyType, Nil),
        HeadingAttribute("DataZatrudnienia", IntegerType, Nil),
        HeadingAttribute("LiczbaDzieci", IntegerType, Nil),

      ),
      Nil,
      Some(Identity("Nr", 1, 1))
    )
  )

  test("Anonymous insert into table with defined Identity") {
    /*
      INSERT INTO Pracownicy2 VALUES
      ('Kowal','Piotr', 500, '2010-01-01',2)
     */
    val row = List(
      StringLiteral("Kowal"),
      StringLiteral("Piotr"),
      IntegerLiteral(500),
      StringLiteral("2010-01-01"),
      IntegerLiteral(2),
    )
    val rowWithID = IntegerLiteral(1) :: row
    val query = AnonymousInsert(
      "Pracownicy2",
      row
    )
    val containsRow = for {
      relation <- pracownicy2
      newRelation <- ManipulationBuilder.insertRow(query, relation)
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
      relation <- pracownicy2
      newRelation <- ManipulationBuilder.insertRow(query, relation)
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
      BodyAttribute("Stawka", IntegerLiteral(1600)),
      BodyAttribute("DataZatrudnienia", StringLiteral("2012-01-01")),
      BodyAttribute("LiczbaDzieci", IntegerLiteral(2)),
    )
    val rowWithId1 = Row(BodyAttribute("Nr", IntegerLiteral(1)) :: row.attributes)
    val rowWithId2 = Row(BodyAttribute("Nr", IntegerLiteral(2)) :: row.attributes)
    val query1 = NamedInsert(
      "Pracownicy2",
      row
    )
    val query2 = NamedInsert(
      "Pracownicy2",
      row
    )
    val hasRow = for {
      relation <- pracownicy2
      newRelation1 <- ManipulationBuilder.insertRow(query1, relation)
      newRelation2 <- ManipulationBuilder.insertRow(query2, newRelation1)
      rows = newRelation2.body
    } yield rows.contains(rowWithId1) &&
      rows.contains(rowWithId2) && newRelation2.identity.contains(Identity("Nr", 3, 1))
    assert(hasRow == Right(true))
  }

  test("UPDATE Pracownicy SET Stawka = 1234, LiczbaDzieci=3 WHERE Nr=1") {
    val query = Update(
      "Pracownicy",
      Row(
        BodyAttribute("Stawka", IntegerLiteral(1234)),
        BodyAttribute("LiczbaDzieci", IntegerLiteral(3))
      ),
      Some(Equals("Nr", IntegerLiteral(1)))
    )
    val expectedRow = Row(
      BodyAttribute("Nr", IntegerLiteral(1)),
      BodyAttribute("Nazwisko", StringLiteral("Kowalski")),
      BodyAttribute("Imie", StringLiteral("Jan")),
      BodyAttribute("Stawka", IntegerLiteral(1234)),
      BodyAttribute("DataZatrudnienia", StringLiteral("2010-01-01")),
      BodyAttribute("LiczbaDzieci", IntegerLiteral(3))
    )
    val isUpdated = for {
      newRelation <- ManipulationBuilder.updateRows(query, relation)
      rows = newRelation.body
    } yield rows.contains(expectedRow) && !rows.contains(row1)
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
      newRelation <- ManipulationBuilder.updateRows(query, relation)
    } yield newRelation.body == relation.body
    assert(noEffect == Right(true))
  }

}
