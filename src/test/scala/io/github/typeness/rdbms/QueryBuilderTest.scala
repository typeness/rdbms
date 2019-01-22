package io.github.typeness.rdbms

import org.scalatest.FunSuite

class QueryBuilderTest extends FunSuite {

  import Relations._

  test("SELECT * FROM Pracownicy WHERE Nr=1") {

    /*
      SELECT * FROM Pracownicy WHERE Nr=1
     */
    val query =
      Select(Nil, "Pracownicy", Nil, Some(Equals("Nr", IntegerLiteral(1))), Nil, None, Nil)
    val hasRow1 = for {
      rows <- QueryBuilder.run(query, schemaPracownicyUrlopy)
    } yield rows == List(pracownicyRow1)
    assert(hasRow1.contains(true))
  }

  test("SELECT * FROM Pracownicy WHERE Nr=9999") {

    /*
      SELECT * FROM Pracownicy WHERE Nr=9999
     */
    val query =
      Select(Nil, "Pracownicy", Nil, Some(Equals("Nr", IntegerLiteral(9999))), Nil, None, Nil)
    val isEmpty = for {
      rows <- QueryBuilder.run(query, schemaPracownicyUrlopy)
    } yield rows == Nil
    assert(isEmpty.contains(true))
  }

  test("SELECT Nr, Nazwisko, Imie FROM Pracownicy") {

    /*
      SELECT Nr, Nazwisko, Imie FROM Pracownicy
     */
    val query =
      Select(List(Var("Nr"), Var("Nazwisko"), Var("Imie")), "Pracownicy", Nil, None, Nil, None, Nil)
    val expected = List(
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
    val result = for {
      rows <- QueryBuilder.run(query, schemaPracownicyUrlopy)
    } yield rows
    assert(result == Right(expected))
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
      Nil,
      None,
      Nil,
    )
    val haveRows = for {
      rows <- QueryBuilder.run(query, schemaPracownicyUrlopy)
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
      Nil,
      None,
      Nil
    )
    val haveRows = for {
      rows <- QueryBuilder.run(query, schemaPracownicyUrlopy)
    } yield rows == List(pracownicyRow4)
    assert(haveRows.contains(true))
  }

  test("SELECT Nr FROM Pracownicy WHERE Nr = 1 UNION SELECT Nr FROM Pracownicy WHERE Nr = 2") {
    val query1 = Select(
      List(Var("Nr")),
      "Pracownicy",
      Nil,
      Some(Equals("Nr", IntegerLiteral(1))),
      Nil,
      None,
      Nil
    )
    val query2 = Select(
      List(Var("Nr")),
      "Pracownicy",
      Nil,
      Some(Equals("Nr", IntegerLiteral(2))),
      Nil,
      None,
      Nil
    )
    val union = Union(query1, query2)
    val expected = List(
      Row(BodyAttribute("Nr", IntegerLiteral(1))),
      Row(BodyAttribute("Nr", IntegerLiteral(2)))
    )
    val hasRows = for {
      result <- QueryBuilder.run(union, schemaPracownicyUrlopy)
    } yield result == expected
    assert(hasRows == Right(true))
  }

  test("SELECT DISTINCT Nazwisko FROM Pracownicy") {
    val query = Select(
      List(Var("Nazwisko")),
      "Pracownicy",
      Nil,
      None,
      Nil,
      None,
      Nil,
      distinct = true
    )
    val expected = List(
      Row(BodyAttribute("Nazwisko", StringLiteral("Kowalski"))),
      Row(BodyAttribute("Nazwisko", StringLiteral("Nowak"))),
      Row(BodyAttribute("Nazwisko", StringLiteral("Wrona"))),
      Row(BodyAttribute("Nazwisko", StringLiteral("Grzyb"))),
    )
    val isDistinct = for {
      result <- QueryBuilder.run(query, schemaPracownicyUrlopy)
    } yield result == expected
    assert(isDistinct == Right(true))
  }

  test("SELECT Nr FROM Pracownicy WHERE Nr > 1") {
    val query = Select(
      List(Var("Nr")),
      "Pracownicy",
      Nil,
      Some(Greater("Nr", IntegerLiteral(1))),
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute("Nr", IntegerLiteral(2))),
      Row(BodyAttribute("Nr", IntegerLiteral(3))),
      Row(BodyAttribute("Nr", IntegerLiteral(4))),
      Row(BodyAttribute("Nr", IntegerLiteral(5))),
    )
    val isFiltered = for {
      result <- QueryBuilder.run(query, schemaPracownicyUrlopy)
    } yield result == expected
    assert(isFiltered == Right(true))
  }

  test("SELECT Nr FROM Pracownicy WHERE Nr <= 2") {
    val query = Select(
      List(Var("Nr")),
      "Pracownicy",
      Nil,
      Some(LessOrEquals("Nr", IntegerLiteral(2))),
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute("Nr", IntegerLiteral(1))),
      Row(BodyAttribute("Nr", IntegerLiteral(2))),
    )
    val isFiltered = for {
      result <- QueryBuilder.run(query, schemaPracownicyUrlopy)
    } yield result == expected
    assert(isFiltered == Right(true))
  }

  test("SELECT Nr FROM Pracownicy WHERE Nr BETWEEN 2 AND 3") {
    val query = Select(
      List(Var("Nr")),
      "Pracownicy",
      Nil,
      Some(Between("Nr", IntegerLiteral(2), IntegerLiteral(3))),
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute("Nr", IntegerLiteral(2))),
      Row(BodyAttribute("Nr", IntegerLiteral(3))),
    )
    val isFiltered = for {
      result <- QueryBuilder.run(query, schemaPracownicyUrlopy)
    } yield result == expected
    assert(isFiltered == Right(true))
  }

  test("SELECT Nr FROM Pracownicy WHERE LiczbaDzieci IS NULL") {
    val query = Select(
      List(Var("Nr")),
      "Pracownicy",
      Nil,
      Some(IsNULL("LiczbaDzieci")),
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute("Nr", IntegerLiteral(5))),
    )
    val isFiltered = for {
      result <- QueryBuilder.run(query, schemaPracownicyUrlopy)
    } yield result == expected
    assert(isFiltered == Right(true))
  }

  test("SELECT Nr FROM Pracownicy ORDER BY Nr DESC") {
    val query = Select(
      List(Var("Nr")),
      "Pracownicy",
      Nil,
      None,
      Nil,
      None,
      List(Descending("Nr"))
    )
    val expected = List(
      Row(BodyAttribute("Nr", IntegerLiteral(5))),
      Row(BodyAttribute("Nr", IntegerLiteral(4))),
      Row(BodyAttribute("Nr", IntegerLiteral(3))),
      Row(BodyAttribute("Nr", IntegerLiteral(2))),
      Row(BodyAttribute("Nr", IntegerLiteral(1))),
    )
    val isSorted = for {
      result <- QueryBuilder.run(query, schemaPracownicyUrlopy)
    } yield result == expected
    assert(isSorted == Right(true))
  }
  test("SELECT Nr FROM Pracownicy ORDER BY Nr ASC") {
    val query = Select(
      List(Var("Nr")),
      "Pracownicy",
      Nil,
      None,
      Nil,
      None,
      List(Ascending("Nr"))
    )
    val expected = List(
      Row(BodyAttribute("Nr", IntegerLiteral(1))),
      Row(BodyAttribute("Nr", IntegerLiteral(2))),
      Row(BodyAttribute("Nr", IntegerLiteral(3))),
      Row(BodyAttribute("Nr", IntegerLiteral(4))),
      Row(BodyAttribute("Nr", IntegerLiteral(5))),
    )
    val isSorted = for {
      result <- QueryBuilder.run(query, schemaPracownicyUrlopy)
    } yield result == expected
    assert(isSorted == Right(true))
  }

  test("SELECT Nr, LiczbaDzieci FROM Pracownicy ORDER BY Nr ASC, LiczbaDzieci ASC") {
    val query = Select(
      List(Var("Nr"), Var("LiczbaDzieci")),
      "Pracownicy",
      Nil,
      None,
      Nil,
      None,
      List(Ascending("Nr"), Ascending("LiczbaDzieci"))
    )
    val expected = List(
      Row(BodyAttribute("Nr", IntegerLiteral(1)), BodyAttribute("LiczbaDzieci", IntegerLiteral(2))),
      Row(BodyAttribute("Nr", IntegerLiteral(2)), BodyAttribute("LiczbaDzieci", IntegerLiteral(2))),
      Row(BodyAttribute("Nr", IntegerLiteral(3)), BodyAttribute("LiczbaDzieci", IntegerLiteral(2))),
      Row(BodyAttribute("Nr", IntegerLiteral(4)), BodyAttribute("LiczbaDzieci", IntegerLiteral(1))),
      Row(BodyAttribute("Nr", IntegerLiteral(5)), BodyAttribute("LiczbaDzieci", NULLLiteral)),
    )
    val isSorted = for {
      result <- QueryBuilder.run(query, schemaPracownicyUrlopy)
    } yield result
    assert(isSorted == Right(expected))
  }

  test("SELECT a FROM RelationA INTERSECT SELECT a FROM RelationA WHERE a=3") {
    val query1 = Select(
      List(Var("a")),
      "RelationA",
      Nil,
      None,
      Nil,
      None,
      Nil
    )
    val query2 = Select(
      List(Var("a")),
      "RelationA",
      Nil,
      Some(Equals("a", IntegerLiteral(3))),
      Nil,
      None,
      Nil
    )
    val intersect = Intersect(query1, query2)
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(3)))
    )
    val result = QueryBuilder.run(intersect, schemaABC)
    assert(result == Right(expected))
  }

  test("SELECT MAX(a) FROM RelationA") {
    val query = Select(
      List(Max("a")),
      "RelationA",
      Nil,
      None,
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute("Max(a)", IntegerLiteral(3)))
    )
    val result = QueryBuilder.run(query, schemaABC)
    assert(result == Right(expected))
  }

  test("SELECT MIN(a) FROM RelationA") {
    val query = Select(
      List(Min("a")),
      "RelationA",
      Nil,
      None,
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute("Min(a)", IntegerLiteral(1)))
    )
    val result = QueryBuilder.run(query, schemaABC)
    assert(result == Right(expected))
  }

  test("SELECT COUNT(a) FROM RelationA") {
    val query = Select(
      List(Count("a")),
      "RelationA",
      Nil,
      None,
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute("Count(a)", IntegerLiteral(3)))
    )
    val result = QueryBuilder.run(query, schemaABC)
    assert(result == Right(expected))
  }

  test("SELECT SUM(a) FROM RelationA") {
    val query = Select(
      List(Sum("a")),
      "RelationA",
      Nil,
      None,
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute("Sum(a)", IntegerLiteral(1 + 2 + 3)))
    )
    val result = QueryBuilder.run(query, schemaABC)
    assert(result == Right(expected))
  }

  test("SELECT AVG(a) FROM RelationA") {
    val query = Select(
      List(Avg("a")),
      "RelationA",
      Nil,
      None,
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute("Avg(a)", IntegerLiteral(2)))
    )
    val result = QueryBuilder.run(query, schemaABC)
    assert(result == Right(expected))
  }

  test("SELECT Nr FROM Pracownicy GROUP BY Nr ORDER BY Nr") {
    val query = Select(
      List(Var("Nr")),
      "Pracownicy",
      Nil,
      None,
      List("Nr"),
      None,
      List(Ascending("Nr"))
    )
    val expected = List(
      Row(BodyAttribute("Nr", IntegerLiteral(1))),
      Row(BodyAttribute("Nr", IntegerLiteral(2))),
      Row(BodyAttribute("Nr", IntegerLiteral(3))),
      Row(BodyAttribute("Nr", IntegerLiteral(4))),
      Row(BodyAttribute("Nr", IntegerLiteral(5)))
    )
    val resullt = QueryBuilder.run(query, schemaPracownicyUrlopy)
    assert(resullt == Right(expected))
  }

  test("SELECT Nazwisko, COUNT(Nr) FROM Pracownicy GROUP BY Nazwisko ORDER BY Count(Nr) DESC") {
    val query = Select(
      List(Var("Nazwisko"), Count("Nr")),
      "Pracownicy",
      Nil,
      None,
      List("Nazwisko"),
      None,
      List(Descending("Count(Nr)"))
    )
    val expected = List(
      Row(
        List(BodyAttribute("Nazwisko", StringLiteral("Kowalski")),
             BodyAttribute("Count(Nr)", IntegerLiteral(2)))),
      Row(
        List(BodyAttribute("Nazwisko", StringLiteral("Grzyb")),
             BodyAttribute("Count(Nr)", IntegerLiteral(1)))),
      Row(
        List(BodyAttribute("Nazwisko", StringLiteral("Wrona")),
             BodyAttribute("Count(Nr)", IntegerLiteral(1)))),
      Row(
        List(BodyAttribute("Nazwisko", StringLiteral("Nowak")),
             BodyAttribute("Count(Nr)", IntegerLiteral(1))))
    )

    val result = QueryBuilder.run(query, schemaPracownicyUrlopy)
    assert(result == Right(expected))
  }

  test(
    "SELECT Nazwisko, 1, COUNT(Nr) FROM Pracownicy GROUP BY Nazwisko HAVING Count(Nr) >= 2 ORDER BY Count(Nr) DESC") {
    val query = Select(
      List(Var("Nazwisko"), IntegerLiteral(1), Count("Nr")),
      "Pracownicy",
      Nil,
      None,
      List("Nazwisko"),
      Some(GreaterOrEquals("Count(Nr)", IntegerLiteral(2))),
      List(Descending("Count(Nr)"))
    )
    val expected = List(
      Row(
        List(BodyAttribute("Nazwisko", StringLiteral("Kowalski")),
             BodyAttribute("1", IntegerLiteral(1)),
             BodyAttribute("Count(Nr)", IntegerLiteral(2)))
      )
    )
    val result = QueryBuilder.run(query, schemaPracownicyUrlopy)
    assert(result == Right(expected))
  }

}
