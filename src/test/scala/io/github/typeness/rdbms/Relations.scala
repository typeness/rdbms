package io.github.typeness.rdbms

object Relations {

  val relationA = Relation(
    "RelationA",
    List("a"),
    None,
    List(HeadingAttribute("a", IntegerType, List(PrimaryKey))),
    List(
      Row(BodyAttribute("a", IntegerLiteral(1))),
      Row(BodyAttribute("a", IntegerLiteral(2))),
      Row(BodyAttribute("a", IntegerLiteral(3)))
    )
  )

  val relationB = Relation(
    "RelationB",
    List("b"),
    None,
    List(HeadingAttribute("b", IntegerType, List(PrimaryKey))),
    List(
      Row(BodyAttribute("b", IntegerLiteral(2))),
      Row(BodyAttribute("b", IntegerLiteral(3))),
      Row(BodyAttribute("b", IntegerLiteral(4)))
    )
  )

  val relationC = Relation(
    "RelationC",
    List("c"),
    None,
    List(HeadingAttribute("c", IntegerType, List(PrimaryKey))),
    List(
      Row(BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("c", IntegerLiteral(5)))
    )
  )

  val schemaABC = Schema(List(relationA, relationB, relationC))

  /*
     INSERT INTO Pracownicy VALUES
     (1, 'Kowalski', 'Jan', 1500, '2010-01-01', 2)
   */
  val pracownicyRow1 = Row(
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
  val pracownicyRow2 = Row(
    BodyAttribute("Nr", IntegerLiteral(2)),
    BodyAttribute("Nazwisko", StringLiteral("Nowak")),
    BodyAttribute("Imie", StringLiteral("Anna")),
    BodyAttribute("Stawka", IntegerLiteral(1600)),
    BodyAttribute("DataZatrudnienia", StringLiteral("2012-01-01")),
    BodyAttribute("LiczbaDzieci", IntegerLiteral(2)),
  )

  /*
     INSERT INTO Pracownicy VALUES
     (3, 'Wrona','Adam', 1100, '2015-01-01',2)
   */
  val pracownicyRow3 = Row(
    BodyAttribute("Nr", IntegerLiteral(3)),
    BodyAttribute("Nazwisko", StringLiteral("Wrona")),
    BodyAttribute("Imie", StringLiteral("Adam")),
    BodyAttribute("Stawka", IntegerLiteral(1100)),
    BodyAttribute("DataZatrudnienia", StringLiteral("2015-01-01")),
    BodyAttribute("LiczbaDzieci", IntegerLiteral(2)),
  )

  /*
     INSERT INTO Pracownicy VALUES
     (4, 'Kowalski','Jacek', 0, '2015-03-07', 1)
   */
  val pracownicyRow4 = Row(
    BodyAttribute("Nr", IntegerLiteral(4)),
    BodyAttribute("Nazwisko", StringLiteral("Kowalski")),
    BodyAttribute("Imie", StringLiteral("Jacek")),
    BodyAttribute("Stawka", IntegerLiteral(0)),
    BodyAttribute("DataZatrudnienia", StringLiteral("2015-03-07")),
    BodyAttribute("LiczbaDzieci", IntegerLiteral(1)),
  )

  val pracownicyRow5 = Row(
    BodyAttribute("Nr", IntegerLiteral(5)),
    BodyAttribute("Nazwisko", StringLiteral("Grzyb")),
    BodyAttribute("Imie", StringLiteral("Tomasz")),
    BodyAttribute("Stawka", IntegerLiteral(4567)),
    BodyAttribute("DataZatrudnienia", StringLiteral("2013-06-10")),
    BodyAttribute("LiczbaDzieci", NULLLiteral),
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
  val pracownicyRelation = Relation(
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
      pracownicyRow1,
      pracownicyRow2,
      pracownicyRow3,
      pracownicyRow4,
      pracownicyRow5
    )
  )
  val schemaPracownicy = Schema(List(pracownicyRelation))

}
