package io.github.typeness.rdbms
import io.github.typeness.rdbms.Relations.{pracownicyRow1, schemaPracownicy}
import org.scalatest.FunSuite

class RelationPrinterTest extends FunSuite {
  test("Show Pracownicy relation") {
    val string = RelationPrinter.makeString(Relations.pracownicyRelation.body)
    assert(string == "Nr Nazwisko Imie   Stawka DataZatrudnienia LiczbaDzieci\n-- -------- ------ ------ ---------------- ------------\n1  Kowalski Jan    1500   2010-01-01       2           \n2  Nowak    Anna   1600   2012-01-01       2           \n3  Wrona    Adam   1100   2015-01-01       2           \n4  Kowalski Jacek  0      2015-03-07       1           \n5  Grzyb    Tomasz 4567   2013-06-10       NULL        ")
  }
  test("SELECT * FROM Pracownicy WHERE Nr=1") {
    val query =
      Select(Nil, "Pracownicy", Nil, Some(Equals("Nr", IntegerLiteral(1))), Nil, None, Nil)
    val rows = for {
      rows <- QueryBuilder.makeQuery(query, schemaPracownicy)
      string = RelationPrinter.makeString(rows)
    } yield string
    assert(rows.right.get == "Nr Nazwisko Imie Stawka DataZatrudnienia LiczbaDzieci\n-- -------- ---- ------ ---------------- ------------\n1  Kowalski Jan  1500   2010-01-01       2           ")
  }
}
