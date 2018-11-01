package io.github.typeness.rdbms

import io.github.typeness.rdbms
import org.scalatest.FunSuite

class SelectionBuilderTest extends FunSuite {

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
     INSERT INTO Pracownicy VALUES
     (3, 'Wrona','Adam', 1100, '2015-01-01',2)
   */
  val row3 = Row(
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
  val row4 = Row(
    BodyAttribute("Nr", IntegerLiteral(4)),
    BodyAttribute("Nazwisko", StringLiteral("Kowalski")),
    BodyAttribute("Imie", StringLiteral("Jacek")),
    BodyAttribute("Stawka", IntegerLiteral(0)),
    BodyAttribute("DataZatrudnienia", StringLiteral("2015-03-07")),
    BodyAttribute("LiczbaDzieci", IntegerLiteral(1)),
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
      row2,
      row3,
      row4
    )
  )
  val schema = Schema(List(relation))

  test("SELECT * FROM Pracownicy WHERE Nr=1") {

    /*
      SELECT * FROM Pracownicy WHERE Nr=1
     */
    val query = Select(Nil,
                       "Pracownicy",
                       Nil,
                       Some(Equals("Nr", IntegerLiteral(1))),
                       None)
    val hasRow1 = for {
      rows <- SelectionBuilder.select(query, schema)
    } yield rows == List(row1)
    assert(hasRow1.contains(true))
  }

  test("SELECT * FROM Pracownicy WHERE Nr=9999") {

    /*
      SELECT * FROM Pracownicy WHERE Nr=9999
     */
    val query = Select(Nil,
                       "Pracownicy",
                       Nil,
                       Some(Equals("Nr", IntegerLiteral(9999))),
                       None)
    val isEmpty = for {
      rows <- SelectionBuilder.select(query, schema)
    } yield rows == Nil
    assert(isEmpty.contains(true))
  }

  test("SELECT Nr, Nazwisko, Imie FROM Pracownicy") {

    /*
      SELECT Nr, Nazwisko, Imie FROM Pracownicy
     */
    val query =
      Select(List("Nr", "Nazwisko", "Imie"), "Pracownicy", Nil, None, None)
    val hasRow = for {
      rows <- SelectionBuilder.select(query, schema)
    } yield
      rows == List(
        Row(
          BodyAttribute("Nr", IntegerLiteral(1)),
          BodyAttribute("Nazwisko", StringLiteral("Kowalski")),
          BodyAttribute("Imie", StringLiteral("Jan"))
        ),
        Row(
          BodyAttribute("Nr", IntegerLiteral(2)),
          BodyAttribute("Nazwisko", StringLiteral("Nowak")),
          BodyAttribute("Imie", StringLiteral("Anna"))
        ),
        Row(
          BodyAttribute("Nr", IntegerLiteral(3)),
          BodyAttribute("Nazwisko", StringLiteral("Wrona")),
          BodyAttribute("Imie", StringLiteral("Adam"))
        ),
        Row(
          BodyAttribute("Nr", IntegerLiteral(4)),
          BodyAttribute("Nazwisko", StringLiteral("Kowalski")),
          BodyAttribute("Imie", StringLiteral("Jacek"))
        )
      )
    assert(hasRow.contains(true))
  }

  test("SELECT * FROM Pracownicy WHERE Nazwisko='Kowalski' OR Nazwisko='Nowak'") {

    /*
      SELECT * FROM Pracownicy WHERE Nazwisko='Kowalski' OR Nazwisko='Nowak'
     */
    val query = Select(
      Nil,
      "Pracownicy",
      Nil,
      Some(
        Or(
          Equals("Nazwisko", StringLiteral("Kowalski")),
          Equals("Nazwisko", StringLiteral("Nowak"))
        )),
      None,
    )
    val haveRows = for {
      rows <- SelectionBuilder.select(query, schema)
    } yield rows == List(row1, row4, row2)
    assert(haveRows.contains(true))
  }

  test("SELECT * FROM Pracownicy WHERE Nazwisko='Kowalski' AND Imie='Jacek'") {

    /*
      SELECT * FROM Pracownicy WHERE Nazwisko='Kowalski' AND Nazwisko='Nowak'
     */
    val query = Select(
      Nil,
      "Pracownicy",
      Nil,
      Some(
        And(
          Equals("Nazwisko", StringLiteral("Kowalski")),
          Equals("Imie", StringLiteral("Jacek"))
        )),
      None
    )
    val haveRows = for {
      rows <- SelectionBuilder.select(query, schema)
    } yield rows == List(row4)
    assert(haveRows.contains(true))
  }
}
