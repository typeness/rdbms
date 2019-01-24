package io.github.typeness.rdbms
import org.scalatest.FunSuite

import fastparse._

import cats.instances.list._
import cats.instances.either._
import cats.syntax.foldable._

class IntegrationTest extends FunSuite {

  private def createSchema(source: String): Either[SQLError, Schema] = {
    val code = scala.io.Source.fromResource(source).mkString
    val Parsed.Success(trees, _) = SQLParser.parseMany(code)
    trees.foldLeftM(Schema(Nil)) {
      case (schema, tree) =>
        SQLInterpreter.run(tree, schema).map {
          case SchemaResult(newSchema) => newSchema
          case RowsResult(_)           => schema
        }
    }
  }

  test("Create empty schema") {
    val Right(SchemaResult(schema)) = SQLInterpreter.runFromResource("empty.sql")
    val Right(RowsResult(rows)) = SQLInterpreter.runFromResource("t1.sql", schema)
    assert(rows == Nil)
  }

  test("Create PracownicyUrlopySchema") {
    val schema = createSchema("pracownicyUrlopySchema.sql")
    assert(
      schema == Right(
        Schema(Map(
          "Pracownicy" -> Relation(
            "Pracownicy",
            List("Nr"),
            None,
            List(
              HeadingAttribute("Nr", IntegerType, List(PrimaryKey)),
              HeadingAttribute("Nazwisko", NVarCharType(50), List(NotNULL)),
              HeadingAttribute("Imie", NVarCharType(50), List(NotNULL)),
              HeadingAttribute("Stawka", IntegerType, List()),
              HeadingAttribute("DataZatrudnienia", DateType, List()),
              HeadingAttribute("LiczbaDzieci", IntegerType, List())
            ),
            List(
              Row(List(
                BodyAttribute("Nr", IntegerLiteral(3)),
                BodyAttribute("Nazwisko", StringLiteral("Wrona")),
                BodyAttribute("Imie", StringLiteral("Adam")),
                BodyAttribute("Stawka", IntegerLiteral(1100)),
                BodyAttribute("DataZatrudnienia", DateLiteral("'2015-01-01'")),
                BodyAttribute("LiczbaDzieci", IntegerLiteral(2))
              )),
              Row(List(
                BodyAttribute("Nr", IntegerLiteral(2)),
                BodyAttribute("Nazwisko", StringLiteral("Nowak")),
                BodyAttribute("Imie", StringLiteral("Anna")),
                BodyAttribute("Stawka", IntegerLiteral(1600)),
                BodyAttribute("DataZatrudnienia", DateLiteral("'2012-01-01'")),
                BodyAttribute("LiczbaDzieci", IntegerLiteral(1))
              )),
              Row(List(
                BodyAttribute("Nr", IntegerLiteral(1)),
                BodyAttribute("Nazwisko", StringLiteral("Kowal")),
                BodyAttribute("Imie", StringLiteral("Piotr")),
                BodyAttribute("Stawka", IntegerLiteral(1500)),
                BodyAttribute("DataZatrudnienia", DateLiteral("'2010-01-01'")),
                BodyAttribute("LiczbaDzieci", IntegerLiteral(2))
              ))
            )
          ),
          "Urlopy" -> Relation(
            "Urlopy",
            List("NrPrac", "OdKiedy"),
            None,
            List(
              HeadingAttribute("NrPrac",
                               IntegerType,
                               List(ForeignKey("Nr", "Pracownicy", NoAction, NoAction),
                                    PrimaryKey)),
              HeadingAttribute("OdKiedy", DateType, List(PrimaryKey)),
              HeadingAttribute("DoKiedy", DateType, List())
            ),
            List()
          )
        )))
    )
  }
}
