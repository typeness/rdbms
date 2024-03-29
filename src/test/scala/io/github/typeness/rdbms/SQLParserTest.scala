package io.github.typeness.rdbms
import fastparse._
import org.scalatest.funsuite.AnyFunSuite

class SQLParserTest extends AnyFunSuite {
  test("INSERT INTO Abc VALUES (1, 123, NULL, '2010-01-01')") {
    val sql = "INSERT INTO Abc VALUES (1, 123, NULL, '2010-01-01')"
    val expected = AnonymousInsert(
      rel"Abc",
      List(List(IntegerLiteral(1), IntegerLiteral(123), NULLLiteral, DateLiteral("'2010-01-01'")))
    )
    val Parsed.Success(value, _) = SQLParser.parse(sql)
    assert(value == expected)
  }

  test("INSERT INTO Pracownicy (Nr, Nazwisko, Imie) VALUES (1, 'Kowal', 'Piotr')") {
    val sql = "INSERT INTO Pracownicy (Nr, Nazwisko, Imie) VALUES (1, 'Kowal', 'Piotr')"
    val expected = NamedInsert(
      rel"Pracownicy",
      List(Row(
        BodyAttribute(col"Nr", IntegerLiteral(1)),
        BodyAttribute(col"Nazwisko", StringLiteral("Kowal")),
        BodyAttribute(col"Imie", StringLiteral("Piotr")),
      ))
    )
    val Parsed.Success(value, _) = SQLParser.parse(sql)
    assert(value == expected)
  }

  test("DELETE FROM Pracownicy WHERE Nr = 1") {
    val sql = "DELETE FROM Pracownicy WHERE Nr = 1"
    val expected = Delete(rel"Pracownicy", Some(Equals(Var(col"Nr"), IntegerLiteral(1))))
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("UPDATE Pracownicy SET Stawka = 1234, LiczbaDzieci = 3 WHERE Nr = 1") {
    val sql = "UPDATE Pracownicy SET Stawka = 1234, LiczbaDzieci = 3 WHERE Nr = 1"
    val expected = Update(
      rel"Pracownicy",
      Row(
        List(BodyAttribute(col"Stawka", IntegerLiteral(1234)),
             BodyAttribute(col"LiczbaDzieci", IntegerLiteral(3)))),
      Some(Equals(Var(col"Nr"), IntegerLiteral(1)))
    )
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("SELECT * FROM Pracownicy WHERE Nr = 1") {
    val sql = "SELECT * FROM Pracownicy WHERE Nr = 1"
    val expected =
      Select(List(), Some(rel"Pracownicy"), List(), Some(Equals(Var(col"Nr"), IntegerLiteral(1))), Nil, None, List())
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("SELECT Nr, Nazwisko, Imie FROM Pracownicy") {
    val sql = "SELECT Nr, Nazwisko, Imie FROM Pracownicy"
    val expected =
      Select(List(Var(col"Nr"), Var(col"Nazwisko"), Var(col"Imie")),
             Some(rel"Pracownicy"),
             List(),
             None,
             Nil,
             None,
             List())
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("SELECT * FROM Pracownicy WHERE Nazwisko='Kowalski' OR Nazwisko='Nowak'") {
    val sql = "SELECT * FROM Pracownicy WHERE Nazwisko='Kowalski' OR Nazwisko='Nowak'"
    val expected = Select(List(),
                          Some(rel"Pracownicy"),
                          List(),
                          Some(
                            Or(Equals(Var(col"Nazwisko"), StringLiteral("Kowalski")),
                               Equals(Var(col"Nazwisko"), StringLiteral("Nowak")))),
                          Nil,
                          None,
                          List())
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("SELECT Nr, LiczbaDzieci FROM Pracownicy ORDER BY Nr ASC, LiczbaDzieci ASC") {
    val sql = "SELECT Nr, LiczbaDzieci FROM Pracownicy ORDER BY Nr ASC, LiczbaDzieci ASC"
    val expected = Select(List(Var(col"Nr"), Var(col"LiczbaDzieci")),
                          Some(rel"Pracownicy"),
                          List(),
                          None,
                          Nil,
                          None,
                          List(Ascending(col"Nr"), Ascending(col"LiczbaDzieci")))
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("SELECT a, b FROM RelationA CROSS JOIN RelationB") {
    val sql = "SELECT a, b FROM RelationA CROSS JOIN RelationB"
    val expected = Select(
      List(Var(col"a"), Var(col"b")),
      Some(rel"RelationA"),
      List(CrossJoin(rel"RelationB")),
      None,
      Nil,
      None,
      Nil
    )
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("SELECT a, b, c FROM RelationA CROSS JOIN RelationB CROSS JOIN RelationC") {
    val sql = "SELECT a, b, c FROM RelationA CROSS JOIN RelationB CROSS JOIN RelationC"
    val expected = Select(
      List(Var(col"a"), Var(col"b"), Var(col"c")),
      Some(rel"RelationA"),
      List(CrossJoin(rel"RelationB"), CrossJoin(rel"RelationC")),
      None,
      Nil,
      None,
      Nil
    )
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("SELECT a, b, c FROM RelationA INNER JOIN RelationB ON a = b INNER JOIN RelationC ON a = c") {
    val sql =
      "SELECT a, b, c FROM RelationA INNER JOIN RelationB ON a = b INNER JOIN RelationC ON a = c"
    val expected = Select(
      List(Var(col"a"), Var(col"b"), Var(col"c")),
      Some(rel"RelationA"),
      List(
        InnerJoin(rel"RelationB", Equals(Var(col"a"), Var(col"b"))),
        InnerJoin(rel"RelationC", Equals(Var(col"a"), Var(col"c"))),
      ),
      None,
      Nil,
      None,
      Nil
    )
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test(
    "SELECT a, b, c FROM RelationA LEFT OUTER JOIN RelationB ON a = b LEFT OUTER JOIN RelationC ON a = c") {
    val sql =
      "SELECT a, b, c FROM RelationA LEFT OUTER JOIN RelationB ON a = b LEFT OUTER JOIN RelationC ON a = c"
    val expected = Select(
      List(Var(col"a"), Var(col"b"), Var(col"c")),
      Some(rel"RelationA"),
      List(
        LeftOuterJoin(rel"RelationB", Equals(Var(col"a"), Var(col"b"))),
        LeftOuterJoin(rel"RelationC", Equals(Var(col"a"), Var(col"c"))),
      ),
      None,
      Nil,
      None,
      Nil
    )
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test(
    "SELECT a, b, c FROM RelationA RIGHT OUTER JOIN RelationB ON a = b RIGHT OUTER JOIN RelationC ON a = c") {
    val sql =
      "SELECT a, b, c FROM RelationA RIGHT OUTER JOIN RelationB ON a = b RIGHT OUTER JOIN RelationC ON a = c"
    val expected = Select(
      List(Var(col"a"), Var(col"b"), Var(col"c")),
      Some(rel"RelationA"),
      List(
        RightOuterJoin(rel"RelationB", Equals(Var(col"a"), Var(col"b"))),
        RightOuterJoin(rel"RelationC", Equals(Var(col"a"), Var(col"c"))),
      ),
      None,
      Nil,
      None,
      Nil
    )
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test(
    "SELECT a, b, c FROM RelationA FULL OUTER JOIN RelationB ON a = b FULL OUTER JOIN RelationC ON a = c") {
    val sql =
      "SELECT a, b, c FROM RelationA FULL OUTER JOIN RelationB ON a = b FULL OUTER JOIN RelationC ON a = c"
    val expected = Select(
      List(Var(col"a"), Var(col"b"), Var(col"c")),
      Some(rel"RelationA"),
      List(
        FullOuterJoin(rel"RelationB", Equals(Var(col"a"), Var(col"b"))),
        FullOuterJoin(rel"RelationC", Equals(Var(col"a"), Var(col"c"))),
      ),
      None,
      Nil,
      None,
      Nil
    )
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("SELECT Nr FROM Pracownicy WHERE Nr = 1 UNION SELECT Nr FROM Pracownicy WHERE Nr = 2") {
    val sql = "SELECT Nr FROM Pracownicy WHERE Nr = 1 UNION SELECT Nr FROM Pracownicy WHERE Nr = 2"
    val query1 = Select(
      List(Var(col"Nr")),
      Some(rel"Pracownicy"),
      Nil,
      Some(Equals(Var(col"Nr"), IntegerLiteral(1))),
      Nil,
      None,
      Nil
    )
    val query2 = Select(
      List(Var(col"Nr")),
      Some(rel"Pracownicy"),
      Nil,
      Some(Equals(Var(col"Nr"), IntegerLiteral(2))),
      Nil,
      None,
      Nil
    )
    val expected = Union(query1, query2)
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("SELECT a FROM RelationA INTERSECT SELECT a FROM RelationA WHERE a=3") {
    val sql = "SELECT a FROM RelationA INTERSECT SELECT a FROM RelationA WHERE a=3"
    val query1 = Select(
      List(Var(col"a")),
      Some(rel"RelationA"),
      Nil,
      None,
      Nil,
      None,
      Nil
    )
    val query2 = Select(
      List(Var(col"a")),
      Some(rel"RelationA"),
      Nil,
      Some(Equals(Var(col"a"), IntegerLiteral(3))),
      Nil,
      None,
      Nil
    )
    val expected = Intersect(query1, query2)
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("CREATE TABLE Urlopy(NrPrac INT PRIMARY KEY, OdKiedy DATE PRIMARY KEY, DoKiedy DATE)") {
    val sql = "CREATE TABLE Urlopy(NrPrac INT PRIMARY KEY, OdKiedy DATE PRIMARY KEY, DoKiedy DATE)"
    val expected = Create(
      rel"Urlopy",
      List(
        HeadingAttribute(col"NrPrac", IntegerType, List(PrimaryKey())),
        HeadingAttribute(col"OdKiedy", DateType, List(PrimaryKey())),
        HeadingAttribute(col"DoKiedy", DateType, Nil)
      ),
      Nil,
      None
    )
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test(
    "CREATE TABLE PRacoWniCy(NrPrac INT pRiMaRY KeY, PESEL CHAR(11) UNIQUE NOT NULL, Nazwisko NVARCHAR(50) NOT NULL, Imie NVARCHAR(50) NOT NULL, Stawka MONEY NULL, [Data urodzenia] DATE, LiczbaDzieci TINYINT, Premia DECIMAL(5,3))") {
    val sql =
      "CREATE TABLE PRacoWniCy(NrPrac INT pRiMaRY KeY, PESEL CHAR(11) UNIQUE NOT NULL, Nazwisko NVARCHAR(50) NOT NULL, Imie NVARCHAR(50) NOT NULL, Stawka MONEY NULL, [Data urodzenia] DATE, LiczbaDzieci TINYINT, Premia DECIMAL(5,3))"
    val Parsed.Success(_, _) = SQLParser.parse(sql)
  }

  test("SELECT Nazwisko, COUNT(Nr) FROM Pracownicy GROUP BY Nazwisko ORDER BY Count(Nr) DESC") {
    val sql = "SELECT Nazwisko, COUNT(Nr) FROM Pracownicy GROUP BY Nazwisko ORDER BY Count(Nr) DESC"
    val expected = Select(
      List(Var(col"Nazwisko"), Count("Nr")),
      Some(rel"Pracownicy"),
      Nil,
      None,
      List(col"Nazwisko"),
      None,
      List(Descending(col"Count(Nr)"))
    )
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("SELECT Nazwisko, COUNT(Nr) FROM Pracownicy GROUP BY Nazwisko HAVING Count(Nr) >= 2 ORDER BY Count(Nr) DESC") {
    val sql = "SELECT Nazwisko, COUNT(Nr) FROM Pracownicy GROUP BY Nazwisko HAVING Count(Nr) >= 2 ORDER BY Count(Nr) DESC"
    val expected = Select(
      List(Var(col"Nazwisko"), Count("Nr")),
      Some(rel"Pracownicy"),
      Nil,
      None,
      List(col"Nazwisko"),
      Some(GreaterOrEquals(Count("Nr"), IntegerLiteral(2))),
      List(Descending(col"Count(Nr)"))
    )
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

}
