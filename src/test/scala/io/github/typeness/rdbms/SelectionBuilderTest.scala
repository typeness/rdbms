package io.github.typeness.rdbms

import org.scalatest.FunSuite

class SelectionBuilderTest extends FunSuite {

  import Relations._

  test("SELECT * FROM Pracownicy WHERE Nr=1") {

    /*
      SELECT * FROM Pracownicy WHERE Nr=1
     */
    val query = Select(Nil, "Pracownicy", Nil, Some(Equals("Nr", IntegerLiteral(1))), None)
    val hasRow1 = for {
      rows <- SelectionBuilder.select(query, schemaPracownicy)
    } yield rows == List(pracownicyRow1)
    assert(hasRow1.contains(true))
  }

  test("SELECT * FROM Pracownicy WHERE Nr=9999") {

    /*
      SELECT * FROM Pracownicy WHERE Nr=9999
     */
    val query = Select(Nil, "Pracownicy", Nil, Some(Equals("Nr", IntegerLiteral(9999))), None)
    val isEmpty = for {
      rows <- SelectionBuilder.select(query, schemaPracownicy)
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
      rows <- SelectionBuilder.select(query, schemaPracownicy)
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
        ),
        Row(
          BodyAttribute("Nr", IntegerLiteral(5)),
          BodyAttribute("Nazwisko", StringLiteral("Grzyb")),
          BodyAttribute("Imie", StringLiteral("Tomasz"))
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
      rows <- SelectionBuilder.select(query, schemaPracownicy)
    } yield rows == List(pracownicyRow1, pracownicyRow4, pracownicyRow2)
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
      rows <- SelectionBuilder.select(query, schemaPracownicy)
    } yield rows == List(pracownicyRow4)
    assert(haveRows.contains(true))
  }

  test("SELECT Nr FROM Pracownicy WHERE Nr = 1 UNION SELECT Nr FROM Pracownicy WHERE Nr = 2") {
    val query1 = Select(
      List("Nr"),
      "Pracownicy",
      Nil,
      Some(Equals("Nr", IntegerLiteral(1))),
      None
    )
    val query2 = Select(
      List("Nr"),
      "Pracownicy",
      Nil,
      Some(Equals("Nr", IntegerLiteral(2))),
      None
    )
    val union = Union(List(query1, query2))
    val expected = List(
      Row(BodyAttribute("Nr", IntegerLiteral(1))),
      Row(BodyAttribute("Nr", IntegerLiteral(2)))
    )
    val hasRows = for {
      result <- SelectionBuilder.unionSelect(union, schemaPracownicy)
    } yield result == expected
    assert(hasRows == Right(true))
  }

  test("SELECT DISTINCT Nazwisko FROM Pracownicy") {
    val query = Select(
      List("Nazwisko"),
      "Pracownicy",
      Nil,
      None,
      None,
      distinct = true
    )
    val expected = List(
      Row(BodyAttribute("Nazwisko", StringLiteral("Kowalski"))),
      Row(BodyAttribute("Nazwisko", StringLiteral("Nowak"))),
      Row(BodyAttribute("Nazwisko", StringLiteral("Wrona"))),
      Row(BodyAttribute("Nazwisko", StringLiteral("Grzyb"))),
    )
    val isDistinct = for {
      result <- SelectionBuilder.select(query, schemaPracownicy)
    } yield result == expected
    assert(isDistinct == Right(true))
  }

  test("SELECT Nr FROM Pracownicy WHERE Nr > 1") {
    val query = Select(
      List("Nr"),
      "Pracownicy",
      Nil,
      Some(Greater("Nr", IntegerLiteral(1))),
      None
    )
    val expected = List(
      Row(BodyAttribute("Nr", IntegerLiteral(2))),
      Row(BodyAttribute("Nr", IntegerLiteral(3))),
      Row(BodyAttribute("Nr", IntegerLiteral(4))),
      Row(BodyAttribute("Nr", IntegerLiteral(5))),
    )
    val isFiltered = for {
      result <- SelectionBuilder.select(query, schemaPracownicy)
    } yield result == expected
    assert(isFiltered == Right(true))
  }

  test("SELECT Nr FROM Pracownicy WHERE Nr <= 2") {
    val query = Select(
      List("Nr"),
      "Pracownicy",
      Nil,
      Some(LessOrEquals("Nr", IntegerLiteral(2))),
      None
    )
    val expected = List(
      Row(BodyAttribute("Nr", IntegerLiteral(1))),
      Row(BodyAttribute("Nr", IntegerLiteral(2))),
    )
    val isFiltered = for {
      result <- SelectionBuilder.select(query, schemaPracownicy)
    } yield result == expected
    assert(isFiltered == Right(true))
  }

  test("SELECT Nr FROM Pracownicy WHERE Nr BETWEEN 2 AND 3") {
    val query = Select(
      List("Nr"),
      "Pracownicy",
      Nil,
      Some(Between("Nr", IntegerLiteral(2), IntegerLiteral(3))),
      None
    )
    val expected = List(
      Row(BodyAttribute("Nr", IntegerLiteral(2))),
      Row(BodyAttribute("Nr", IntegerLiteral(3))),
    )
    val isFiltered = for {
      result <- SelectionBuilder.select(query, schemaPracownicy)
    } yield result == expected
    assert(isFiltered == Right(true))
  }

  test("SELECT Nr FROM Pracownicy WHERE LiczbaDzieci IS NULL") {
    val query = Select(
      List("Nr"),
      "Pracownicy",
      Nil,
      Some(IsNULL("LiczbaDzieci")),
      None
    )
    val expected = List(
      Row(BodyAttribute("Nr", IntegerLiteral(5))),
    )
    val isFiltered = for {
      result <- SelectionBuilder.select(query, schemaPracownicy)
    } yield result == expected
    assert(isFiltered == Right(true))
  }
}
