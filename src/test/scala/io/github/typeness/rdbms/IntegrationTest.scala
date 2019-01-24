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

  private lazy val pracownicyUrlopy = createSchema("pracownicyUrlopySchema.sql")

  test("Create empty schema") {
    val Right(SchemaResult(schema)) = SQLInterpreter.runFromResource("empty.sql")
    val Right(RowsResult(rows)) = SQLInterpreter.runFromResource("t1.sql", schema)
    assert(rows == Nil)
  }

  test("Create PracownicyUrlopySchema") {
    assert(
      pracownicyUrlopy == Right(
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

  test("SELECT * FROM Pracownicy WHERE Nazwisko = 'Kowal'") {
    val result = for {
      schema <- pracownicyUrlopy
      rows <- SQLInterpreter.runFromResource("t2.sql", schema)
    } yield rows
    assert(
      result == Right(
        RowsResult(List(Row(List(
          BodyAttribute("Nr", IntegerLiteral(1)),
          BodyAttribute("Nazwisko", StringLiteral("Kowal")),
          BodyAttribute("Imie", StringLiteral("Piotr")),
          BodyAttribute("Stawka", IntegerLiteral(1500)),
          BodyAttribute("DataZatrudnienia", DateLiteral("'2010-01-01'")),
          BodyAttribute("LiczbaDzieci", IntegerLiteral(2))
        )))))
    )
  }

  test("Failure when inserting non-existing primary key as foreign key") {
    val result = for {
      schema <- pracownicyUrlopy
      newSchema <- SQLInterpreter.runFromResource("t3.sql", schema)
    } yield newSchema
    assert(
      result == Left(
        PrimaryKeyDoesNotExist("Urlopy", "NrPrac", "Pracownicy", "Nr", IntegerLiteral(23416))))
  }

  test("Success when inserting existing primary key as foreign key") {
    val Right(SchemaResult(result)) = for {
      schema <- pracownicyUrlopy
      newSchema <- SQLInterpreter.runFromResource("t4.sql", schema)
    } yield newSchema
    val urlopy = result.getRelation("Urlopy").map(_.body)
    assert(
      urlopy == Right(
        List(Row(List(BodyAttribute("NrPrac", IntegerLiteral(1)),
                      BodyAttribute("OdKiedy", DateLiteral("'2015-01-01'")),
                      BodyAttribute("DoKiedy", DateLiteral("'2015-01-05'")))))))
  }

  test("Failure when inserting duplicate primary key") {
    val Right(SchemaResult(newSchema)) = for {
      schema <- pracownicyUrlopy
      newSchema <- SQLInterpreter.runFromResource("t4.sql", schema)
    } yield newSchema
    val result = SQLInterpreter.runFromResource("t5.sql", newSchema)
    assert(
      result == Left(
        PrimaryKeyDuplicate(List(BodyAttribute("NrPrac", IntegerLiteral(1)),
                                 BodyAttribute("OdKiedy", DateLiteral("'2015-01-01'"))))))
  }

  test("Success when inserting unique primary key") {
    val Right(SchemaResult(newSchema)) = for {
      schema <- pracownicyUrlopy
      newSchema <- SQLInterpreter.runFromResource("t4.sql", schema)
    } yield newSchema
    val Right(SchemaResult(result)) = SQLInterpreter.runFromResource("t6.sql", newSchema)
    val urlopy = result.getRelation("Urlopy")
    assert(
      urlopy == Right(Relation(
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
        List(
          Row(List(BodyAttribute("NrPrac", IntegerLiteral(1)),
                   BodyAttribute("OdKiedy", DateLiteral("'2015-02-01'")),
                   BodyAttribute("DoKiedy", DateLiteral("'2015-02-07'")))),
          Row(List(BodyAttribute("NrPrac", IntegerLiteral(1)),
                   BodyAttribute("OdKiedy", DateLiteral("'2015-01-01'")),
                   BodyAttribute("DoKiedy", DateLiteral("'2015-01-05'"))))
        )
      )))
  }

}
