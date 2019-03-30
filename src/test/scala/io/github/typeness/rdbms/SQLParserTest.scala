package io.github.typeness.rdbms
import fastparse._
import org.scalatest.FunSuite

class SQLParserTest extends FunSuite {
  test("INSERT INTO Abc VALUES (1, 123, NULL, '2010-01-01')") {
    val sql = "INSERT INTO Abc VALUES (1, 123, NULL, '2010-01-01')"
    val expected = AnonymousInsert(
      "Abc",
      List(List(IntegerLiteral(1), IntegerLiteral(123), NULLLiteral, DateLiteral("'2010-01-01'")))
    )
    val Parsed.Success(value, _) = SQLParser.parse(sql)
    assert(value == expected)
  }

  test("INSERT INTO Pracownicy (Nr, Nazwisko, Imie) VALUES (1, 'Kowal', 'Piotr')") {
    val sql = "INSERT INTO Pracownicy (Nr, Nazwisko, Imie) VALUES (1, 'Kowal', 'Piotr')"
    val expected = NamedInsert(
      "Pracownicy",
      List(Row(
        BodyAttribute("Nr", IntegerLiteral(1)),
        BodyAttribute("Nazwisko", StringLiteral("Kowal")),
        BodyAttribute("Imie", StringLiteral("Piotr")),
      ))
    )
    val Parsed.Success(value, _) = SQLParser.parse(sql)
    assert(value == expected)
  }

  test("DELETE FROM Pracownicy WHERE Nr = 1") {
    val sql = "DELETE FROM Pracownicy WHERE Nr = 1"
    val expected = Delete("Pracownicy", Some(Equals(Var("Nr"), IntegerLiteral(1))))
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("UPDATE Pracownicy SET Stawka = 1234, LiczbaDzieci = 3 WHERE Nr = 1") {
    val sql = "UPDATE Pracownicy SET Stawka = 1234, LiczbaDzieci = 3 WHERE Nr = 1"
    val expected = Update(
      "Pracownicy",
      Row(
        List(BodyAttribute("Stawka", IntegerLiteral(1234)),
             BodyAttribute("LiczbaDzieci", IntegerLiteral(3)))),
      Some(Equals(Var("Nr"), IntegerLiteral(1)))
    )
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("SELECT * FROM Pracownicy WHERE Nr = 1") {
    val sql = "SELECT * FROM Pracownicy WHERE Nr = 1"
    val expected =
      Select(List(), Some("Pracownicy"), List(), Some(Equals(Var("Nr"), IntegerLiteral(1))), Nil, None, List())
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("SELECT Nr, Nazwisko, Imie FROM Pracownicy") {
    val sql = "SELECT Nr, Nazwisko, Imie FROM Pracownicy"
    val expected =
      Select(List(Var("Nr"), Var("Nazwisko"), Var("Imie")),
             Some("Pracownicy"),
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
                          Some("Pracownicy"),
                          List(),
                          Some(
                            Or(Equals(Var("Nazwisko"), StringLiteral("Kowalski")),
                               Equals(Var("Nazwisko"), StringLiteral("Nowak")))),
                          Nil,
                          None,
                          List())
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("SELECT Nr, LiczbaDzieci FROM Pracownicy ORDER BY Nr ASC, LiczbaDzieci ASC") {
    val sql = "SELECT Nr, LiczbaDzieci FROM Pracownicy ORDER BY Nr ASC, LiczbaDzieci ASC"
    val expected = Select(List(Var("Nr"), Var("LiczbaDzieci")),
                          Some("Pracownicy"),
                          List(),
                          None,
                          Nil,
                          None,
                          List(Ascending("Nr"), Ascending("LiczbaDzieci")))
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("SELECT a, b FROM RelationA CROSS JOIN RelationB") {
    val sql = "SELECT a, b FROM RelationA CROSS JOIN RelationB"
    val expected = Select(
      List(Var("a"), Var("b")),
      Some("RelationA"),
      List(CrossJoin("RelationB")),
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
      List(Var("a"), Var("b"), Var("c")),
      Some("RelationA"),
      List(CrossJoin("RelationB"), CrossJoin("RelationC")),
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
      List(Var("a"), Var("b"), Var("c")),
      Some("RelationA"),
      List(
        InnerJoin("RelationB", Equals(Var("a"), Var("b"))),
        InnerJoin("RelationC", Equals(Var("a"), Var("c"))),
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
      List(Var("a"), Var("b"), Var("c")),
      Some("RelationA"),
      List(
        LeftOuterJoin("RelationB", Equals(Var("a"), Var("b"))),
        LeftOuterJoin("RelationC", Equals(Var("a"), Var("c"))),
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
      List(Var("a"), Var("b"), Var("c")),
      Some("RelationA"),
      List(
        RightOuterJoin("RelationB", Equals(Var("a"), Var("b"))),
        RightOuterJoin("RelationC", Equals(Var("a"), Var("c"))),
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
      List(Var("a"), Var("b"), Var("c")),
      Some("RelationA"),
      List(
        FullOuterJoin("RelationB", Equals(Var("a"), Var("b"))),
        FullOuterJoin("RelationC", Equals(Var("a"), Var("c"))),
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
      List(Var("Nr")),
      Some("Pracownicy"),
      Nil,
      Some(Equals(Var("Nr"), IntegerLiteral(1))),
      Nil,
      None,
      Nil
    )
    val query2 = Select(
      List(Var("Nr")),
      Some("Pracownicy"),
      Nil,
      Some(Equals(Var("Nr"), IntegerLiteral(2))),
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
      List(Var("a")),
      Some("RelationA"),
      Nil,
      None,
      Nil,
      None,
      Nil
    )
    val query2 = Select(
      List(Var("a")),
      Some("RelationA"),
      Nil,
      Some(Equals(Var("a"), IntegerLiteral(3))),
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
      "Urlopy",
      List(
        HeadingAttribute("NrPrac", IntegerType, List(PrimaryKey())),
        HeadingAttribute("OdKiedy", DateType, List(PrimaryKey())),
        HeadingAttribute("DoKiedy", DateType, Nil)
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
      List(Var("Nazwisko"), Count("Nr")),
      Some("Pracownicy"),
      Nil,
      None,
      List("Nazwisko"),
      None,
      List(Descending("Count(Nr)"))
    )
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

  test("SELECT Nazwisko, COUNT(Nr) FROM Pracownicy GROUP BY Nazwisko HAVING Count(Nr) >= 2 ORDER BY Count(Nr) DESC") {
    val sql = "SELECT Nazwisko, COUNT(Nr) FROM Pracownicy GROUP BY Nazwisko HAVING Count(Nr) >= 2 ORDER BY Count(Nr) DESC"
    val expected = Select(
      List(Var("Nazwisko"), Count("Nr")),
      Some("Pracownicy"),
      Nil,
      None,
      List("Nazwisko"),
      Some(GreaterOrEquals(Count("Nr"), IntegerLiteral(2))),
      List(Descending("Count(Nr)"))
    )
    val Parsed.Success(result, _) = SQLParser.parse(sql)
    assert(result == expected)
  }

}
