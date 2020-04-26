package io.github.typeness.rdbms

object Relations {

  val relationA = Relation(
    "RelationA",
    List("a"),
    None,
    List(HeadingAttribute("a", IntegerType, List(PrimaryKey()))),
    List(
      Row(BodyAttribute("a", IntegerLiteral(1))),
      Row(BodyAttribute("a", IntegerLiteral(2))),
      Row(BodyAttribute("a", IntegerLiteral(3)))
    ),
    Nil
  )

  val relationB = Relation(
    "RelationB",
    List("b"),
    None,
    List(HeadingAttribute("b", IntegerType, List(PrimaryKey()))),
    List(
      Row(BodyAttribute("b", IntegerLiteral(2))),
      Row(BodyAttribute("b", IntegerLiteral(3))),
      Row(BodyAttribute("b", IntegerLiteral(4)))
    ),
    Nil
  )

  val relationC = Relation(
    "RelationC",
    List("c"),
    None,
    List(HeadingAttribute("c", IntegerType, List(PrimaryKey()))),
    List(
      Row(BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("c", IntegerLiteral(5)))
    ),
    Nil
  )

  val schemaABC = Schema(relationA, relationB, relationC)

  /*
     INSERT INTO Pracownicy VALUES
     (1, 'Kowalski', 'Jan', 1500, '2010-01-01', 2)
   */
  val pracownicyRow1 = Row(
    BodyAttribute("Nr", IntegerLiteral(1)),
    BodyAttribute("Nazwisko", StringLiteral("Kowalski")),
    BodyAttribute("Imie", StringLiteral("Jan")),
    BodyAttribute("Stawka", MoneyLiteral(1500)),
    BodyAttribute("DataZatrudnienia", DateLiteral("2010-01-01")),
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
    BodyAttribute("Stawka", MoneyLiteral(1600)),
    BodyAttribute("DataZatrudnienia", DateLiteral("2012-01-01")),
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
    BodyAttribute("Stawka", MoneyLiteral(1100)),
    BodyAttribute("DataZatrudnienia", DateLiteral("2015-01-01")),
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
    BodyAttribute("Stawka", MoneyLiteral(0)),
    BodyAttribute("DataZatrudnienia", DateLiteral("2015-03-07")),
    BodyAttribute("LiczbaDzieci", IntegerLiteral(1)),
  )

  val pracownicyRow5 = Row(
    BodyAttribute("Nr", IntegerLiteral(5)),
    BodyAttribute("Nazwisko", StringLiteral("Grzyb")),
    BodyAttribute("Imie", StringLiteral("Tomasz")),
    BodyAttribute("Stawka", MoneyLiteral(4567)),
    BodyAttribute("DataZatrudnienia", DateLiteral("2013-06-10")),
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
  val pracownicy = Relation(
    "Pracownicy",
    Nil,
    None,
    List(
      HeadingAttribute("Nr", IntegerType, List(PrimaryKey())),
      HeadingAttribute("Nazwisko", NVarCharType(50), List(NotNULL())),
      HeadingAttribute("Imie", NVarCharType(50), List(NotNULL())),
      HeadingAttribute("Stawka", MoneyType, Nil),
      HeadingAttribute("DataZatrudnienia", DateType, Nil),
      HeadingAttribute("LiczbaDzieci", IntegerType, Nil),
    ),
    List(
      pracownicyRow1,
      pracownicyRow2,
      pracownicyRow3,
      pracownicyRow4,
      pracownicyRow5
    ),
    Nil
  )

  /*
  CREATE TABLE Urlopy(
    NrPrac INT REFERENCES Pracownicy(Nr),
    OdKiedy DATE,
    DoKiedy DATE,
    PRIMARY KEY(NrPrac, OdKiedy)
  )
   */

  val urlopy = Relation(
    "Urlopy",
    Nil,
    None,
    List(
      HeadingAttribute("NrPrac",
                       IntegerType,
                       List(PrimaryKey(), ForeignKey("Nr", "Pracownicy", NoAction, NoAction))),
      HeadingAttribute("OdKiedy", DateType, List(PrimaryKey())),
      HeadingAttribute("DoKiedy", DateType, Nil),
    ),
    List(
      Row(BodyAttribute("NrPrac", IntegerLiteral(5)),
          BodyAttribute("OdKiedy", DateLiteral("2010-01-01")),
          BodyAttribute("DoKiedy", DateLiteral("2010-02-01")))
    ),
    Nil
  )

  val schemaPracownicyUrlopy = Schema(pracownicy, urlopy)

  val pracownicy2 = Relation(
    "Pracownicy2",
    List("Nr"),
    Some(Identity("Nr", 1, 1)),
    List(
      HeadingAttribute("Nr", IntegerType, List(PrimaryKey())),
      HeadingAttribute("Nazwisko", NVarCharType(50), List(NotNULL())),
      HeadingAttribute("Imie", NVarCharType(50), List(NotNULL())),
      HeadingAttribute("Stawka", MoneyType, Nil),
      HeadingAttribute("DataZatrudnienia", DateType, Nil),
      HeadingAttribute("LiczbaDzieci", IntegerType, Nil),
    ),
    Nil,
    Nil
  )

  val schemaPracownicy2 = Schema(pracownicy2)

}
