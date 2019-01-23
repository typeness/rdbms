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
            HeadingAttribute("Stawka", MoneyType, List()),
            HeadingAttribute("DataZatrudnienia", DateType, List()),
            HeadingAttribute("LiczbaDzieci", IntegerType, List())
          ),
          List()
        ),
        "Urlopy" -> Relation(
          "Urlopy",
          List("NrPrac", "OdKiedy"),
          None,
          List(
            HeadingAttribute("NrPrac",
                             IntegerType,
                             List(ForeignKey("Nr", "Pracownicy", NoAction, NoAction), PrimaryKey)),
            HeadingAttribute("OdKiedy", DateType, List(PrimaryKey)),
            HeadingAttribute("DoKiedy", DateType, List())
          ),
          List()
        )
      )))
  }
}
