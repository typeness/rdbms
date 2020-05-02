package io.github.typeness.rdbms

object Relations {

  val relationA = Relation(
    rel"RelationA",
    List(col"a"),
    None,
    List(HeadingAttribute(col"a", IntegerType, List(PrimaryKey()))),
    List(
      Row(BodyAttribute(col"a", IntegerLiteral(1))),
      Row(BodyAttribute(col"a", IntegerLiteral(2))),
      Row(BodyAttribute(col"a", IntegerLiteral(3)))
    ),
    Nil
  )

  val relationB = Relation(
    rel"RelationB",
    List(col"b"),
    None,
    List(HeadingAttribute(col"b", IntegerType, List(PrimaryKey()))),
    List(
      Row(BodyAttribute(col"b", IntegerLiteral(2))),
      Row(BodyAttribute(col"b", IntegerLiteral(3))),
      Row(BodyAttribute(col"b", IntegerLiteral(4)))
    ),
    Nil
  )

  val relationC = Relation(
    rel"RelationC",
    List(col"c"),
    None,
    List(HeadingAttribute(col"c", IntegerType, List(PrimaryKey()))),
    List(
      Row(BodyAttribute(col"c", IntegerLiteral(3))),
      Row(BodyAttribute(col"c", IntegerLiteral(4))),
      Row(BodyAttribute(col"c", IntegerLiteral(5)))
    ),
    Nil
  )

  val schemaABC = Schema(relationA, relationB, relationC)

  /*
     INSERT INTO Pracownicy VALUES
     (1, 'Kowalski', 'Jan', 1500, '2010-01-01', 2)
   */
  val pracownicyRow1 = Row(
    BodyAttribute(col"Nr", IntegerLiteral(1)),
    BodyAttribute(col"Nazwisko", StringLiteral("Kowalski")),
    BodyAttribute(col"Imie", StringLiteral("Jan")),
    BodyAttribute(col"Stawka", MoneyLiteral(1500)),
    BodyAttribute(col"DataZatrudnienia", DateLiteral("2010-01-01")),
    BodyAttribute(col"LiczbaDzieci", IntegerLiteral(2)),
  )

  /*
     INSERT INTO Pracownicy VALUES
     (2, 'Nowak','Anna', 1600, '2012-01-01',2)
   */
  val pracownicyRow2 = Row(
    BodyAttribute(col"Nr", IntegerLiteral(2)),
    BodyAttribute(col"Nazwisko", StringLiteral("Nowak")),
    BodyAttribute(col"Imie", StringLiteral("Anna")),
    BodyAttribute(col"Stawka", MoneyLiteral(1600)),
    BodyAttribute(col"DataZatrudnienia", DateLiteral("2012-01-01")),
    BodyAttribute(col"LiczbaDzieci", IntegerLiteral(2)),
  )

  /*
     INSERT INTO Pracownicy VALUES
     (3, 'Wrona','Adam', 1100, '2015-01-01',2)
   */
  val pracownicyRow3 = Row(
    BodyAttribute(col"Nr", IntegerLiteral(3)),
    BodyAttribute(col"Nazwisko", StringLiteral("Wrona")),
    BodyAttribute(col"Imie", StringLiteral("Adam")),
    BodyAttribute(col"Stawka", MoneyLiteral(1100)),
    BodyAttribute(col"DataZatrudnienia", DateLiteral("2015-01-01")),
    BodyAttribute(col"LiczbaDzieci", IntegerLiteral(2)),
  )

  /*
     INSERT INTO Pracownicy VALUES
     (4, 'Kowalski','Jacek', 0, '2015-03-07', 1)
   */
  val pracownicyRow4 = Row(
    BodyAttribute(col"Nr", IntegerLiteral(4)),
    BodyAttribute(col"Nazwisko", StringLiteral("Kowalski")),
    BodyAttribute(col"Imie", StringLiteral("Jacek")),
    BodyAttribute(col"Stawka", MoneyLiteral(0)),
    BodyAttribute(col"DataZatrudnienia", DateLiteral("2015-03-07")),
    BodyAttribute(col"LiczbaDzieci", IntegerLiteral(1)),
  )

  val pracownicyRow5 = Row(
    BodyAttribute(col"Nr", IntegerLiteral(5)),
    BodyAttribute(col"Nazwisko", StringLiteral("Grzyb")),
    BodyAttribute(col"Imie", StringLiteral("Tomasz")),
    BodyAttribute(col"Stawka", MoneyLiteral(4567)),
    BodyAttribute(col"DataZatrudnienia", DateLiteral("2013-06-10")),
    BodyAttribute(col"LiczbaDzieci", NULLLiteral),
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
    rel"Pracownicy",
    Nil,
    None,
    List(
      HeadingAttribute(col"Nr", IntegerType, List(PrimaryKey())),
      HeadingAttribute(col"Nazwisko", NVarCharType(50), List(NotNULL())),
      HeadingAttribute(col"Imie", NVarCharType(50), List(NotNULL())),
      HeadingAttribute(col"Stawka", MoneyType, Nil),
      HeadingAttribute(col"DataZatrudnienia", DateType, Nil),
      HeadingAttribute(col"LiczbaDzieci", IntegerType, Nil),
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
    rel"Urlopy",
    Nil,
    None,
    List(
      HeadingAttribute(col"NrPrac",
                       IntegerType,
                       List(PrimaryKey(), ForeignKey(col"Nr", rel"Pracownicy", NoAction, NoAction))),
      HeadingAttribute(col"OdKiedy", DateType, List(PrimaryKey())),
      HeadingAttribute(col"DoKiedy", DateType, Nil),
    ),
    List(
      Row(BodyAttribute(col"NrPrac", IntegerLiteral(5)),
          BodyAttribute(col"OdKiedy", DateLiteral("2010-01-01")),
          BodyAttribute(col"DoKiedy", DateLiteral("2010-02-01")))
    ),
    Nil
  )

  val schemaPracownicyUrlopy = Schema(pracownicy, urlopy)

  val pracownicy2 = Relation(
    rel"Pracownicy2",
    List(col"Nr"),
    Some(Identity(col"Nr", 1, 1)),
    List(
      HeadingAttribute(col"Nr", IntegerType, List(PrimaryKey())),
      HeadingAttribute(col"Nazwisko", NVarCharType(50), List(NotNULL())),
      HeadingAttribute(col"Imie", NVarCharType(50), List(NotNULL())),
      HeadingAttribute(col"Stawka", MoneyType, Nil),
      HeadingAttribute(col"DataZatrudnienia", DateType, Nil),
      HeadingAttribute(col"LiczbaDzieci", IntegerType, Nil),
    ),
    Nil,
    Nil
  )

  val schemaPracownicy2 = Schema(pracownicy2)

}
