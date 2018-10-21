package io.github.typeness.rdbms

import io.github.typeness.rdbms
import org.scalatest.FunSuite

class SelectionBuilderTest extends FunSuite {

  /*
      INSERT INTO Pracownicy VALUES
      (1, 'Kowalski', 'Jan', 1500, '2010-01-01', 2)
     */
  val row1 = List(
    BodyAttribute("Nr", Value(IntegerLiteral(1))),
    BodyAttribute("Nazwisko", Value(StringLiteral("Kowalski"))),
    BodyAttribute("Imie", Value(StringLiteral("Jan"))),
    BodyAttribute("Stawka", Value(IntegerLiteral(1500))),
    BodyAttribute("DataZatrudnienia", Value(StringLiteral("2010-01-01"))),
    BodyAttribute("LiczbaDzieci", Value(IntegerLiteral(2))),

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
    List(
      HeadingAttribute("Nr", IntegerType, List(PrimaryKey)),
      HeadingAttribute("Nazwisko", StringType, List(NotNULL)),
      HeadingAttribute("Imie", StringType, List(NotNULL)),
      HeadingAttribute("Stawka", MoneyType, Nil),
      HeadingAttribute("DataZatrudnienia", IntegerType, Nil),
      HeadingAttribute("LiczbaDzieci", IntegerType, Nil),
    ),
    List(
      row1
    )
  )

  test("SELECT * FROM Pracownicy WHERE Nr=1") {

    /*
      SELECT * FROM Pracownicy WHERE Nr=1
     */
    val query = Select(Nil, "Pracownicy", Some(Equals("Nr", Value(IntegerLiteral(1)))), None)
    val hasRow1 = for {
      rows <- SelectionBuilder.select(query, relation)
    } yield rows == List(row1)
    assert(hasRow1.contains(true))
  }

  test("SELECT * FROM Pracownicy WHERE Nr=9999") {

    /*
      SELECT * FROM Pracownicy WHERE Nr=9999
     */
    val query = Select(Nil, "Pracownicy", Some(Equals("Nr", Value(IntegerLiteral(9999)))), None)
    val isEmpty = for {
      rows <- SelectionBuilder.select(query, relation)
    } yield rows == Nil
    assert(isEmpty.contains(true))
  }

  test("SELECT Nr, Nazwisko, Imie FROM Pracownicy") {

    /*
      SELECT Nr, Nazwisko, Imie FROM Pracownicy
     */
    val query = Select(List("Nr", "Nazwisko", "Imie"), "Pracownicy", None, None)
    val hasRow = for {
      rows <- SelectionBuilder.select(query, relation)
    } yield rows == List(
      List(
        BodyAttribute("Nr", Value(IntegerLiteral(1))),
        BodyAttribute("Nazwisko", Value(StringLiteral("Kowalski"))),
        BodyAttribute("Imie", Value(StringLiteral("Jan")))
      )
    )
    assert(hasRow.contains(true))
  }
}
