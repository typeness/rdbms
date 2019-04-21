package io.github.typeness.rdbms
import org.scalatest.FunSuite

import fastparse._

import cats.instances.list._
import cats.instances.either._
import cats.syntax.foldable._
import cats.syntax.traverse._

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
  private lazy val pracownicyUrlopy2 = createSchema("pracownicyUrlopySchema2.sql")
  private lazy val northwind = createSchema("northwind.sql")

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
              HeadingAttribute("Nr", IntegerType, List(PrimaryKey())),
              HeadingAttribute("Nazwisko", NVarCharType(50), List(NotNULL())),
              HeadingAttribute("Imie", NVarCharType(50), List(NotNULL())),
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
                                    PrimaryKey())),
              HeadingAttribute("OdKiedy", DateType, List(PrimaryKey())),
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
                           List(ForeignKey("Nr", "Pracownicy", NoAction, NoAction), PrimaryKey())),
          HeadingAttribute("OdKiedy", DateType, List(PrimaryKey())),
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

  test("Failure when deleting row with primary key referenced in other relation") {
    val Right(SchemaResult(newSchema)) = for {
      schema <- pracownicyUrlopy
      newSchema <- SQLInterpreter.runFromResource("t4.sql", schema)
    } yield newSchema
    val result = SQLInterpreter.runFromResource("t7.sql", newSchema)
    assert(result == Left(ForeignKeyViolation("Urlopy", "NrPrac")))
  }

  test("Success when deleting row with primary key not referenced in other relation") {
    val Right(SchemaResult(newSchema)) = for {
      schema <- pracownicyUrlopy
      newSchema <- SQLInterpreter.runFromResource("t4.sql", schema)
    } yield newSchema
    val Right(SchemaResult(result)) = SQLInterpreter.runFromResource("t8.sql", newSchema)
    val isDeleted = result
      .getRelation("Pracownicy")
      .flatMap(_.body.traverse(_.projectEither("Nr").map(_.literal.show)))
    assert(isDeleted.map(_.contains("2")) == Right(false))
  }

  test("Cascade trigger when deleting row with primary key referenced in other relation") {
    val Right(SchemaResult(newSchema)) = for {
      schema <- pracownicyUrlopy2
      newSchema <- SQLInterpreter.runFromResource("t4.sql", schema)
    } yield newSchema
    val Right(SchemaResult(result)) = SQLInterpreter.runFromResource("t7.sql", newSchema)
    val urlopyBody = result.getRelation("Urlopy").map(_.body)
    assert(urlopyBody == Right(Nil))
  }

  test("Cascade trigger when updating row with primary key referenced in other relation") {
    val Right(SchemaResult(newSchema)) = for {
      schema <- pracownicyUrlopy2
      newSchema <- SQLInterpreter.runFromResource("t4.sql", schema)
    } yield newSchema
    val Right(SchemaResult(result)) = SQLInterpreter.runFromResource("t10.sql", newSchema)
    val urlopyBody =
      result.getRelation("Urlopy").flatMap(_.body.traverse(_.projectEither("NrPrac")))
    assert(urlopyBody == Right(List(BodyAttribute("NrPrac", IntegerLiteral(333)))))
  }

  test("Failure when setting non-existing primary key as foreign key") {
    val Right(SchemaResult(newSchema)) = for {
      schema <- pracownicyUrlopy
      newSchema <- SQLInterpreter.runFromResource("t4.sql", schema)
    } yield newSchema
    val result = SQLInterpreter.runFromResource("t11.sql", newSchema)
    assert(
      result == Left(
        PrimaryKeyDoesNotExist("Urlopy", "NrPrac", "Pracownicy", "Nr", IntegerLiteral(2512))))
  }

  test("Column alias") {
    val Right(RowsResult(rows)) = for {
      schema <- pracownicyUrlopy
      result <- SQLInterpreter.runFromResource("t12.sql", schema)
    } yield result
    assert(
      rows == List(
        Row(List(BodyAttribute("Alias", StringLiteral("Wrona")),
                 BodyAttribute("Imie", StringLiteral("Adam")))),
        Row(List(BodyAttribute("Alias", StringLiteral("Nowak")),
                 BodyAttribute("Imie", StringLiteral("Anna")))),
        Row(List(BodyAttribute("Alias", StringLiteral("Kowal")),
                 BodyAttribute("Imie", StringLiteral("Piotr"))))
      ))
  }

  test("LIKE operator") {
    val Right(RowsResult(rows)) = for {
      schema <- pracownicyUrlopy
      result <- SQLInterpreter.runFromResource("t9.sql", schema)
    } yield result
    assert(
      rows == List(
        Row(List(BodyAttribute("Imie", StringLiteral("Anna")),
                 BodyAttribute("Nazwisko", StringLiteral("Nowak")))),
        Row(List(BodyAttribute("Imie", StringLiteral("Adam")),
                 BodyAttribute("Nazwisko", StringLiteral("Wrona"))))
      ))
  }

  test("Named constraints") {
    val Right(schema) = createSchema("t13.sql")
    assert(
      schema == Schema(
        Map("Dbo" -> Relation(
          "Dbo",
          List("ColumnA"),
          None,
          List(
            HeadingAttribute("ColumnA",
                             IntegerType,
                             List(ForeignKey("B", "TAB", NoAction, NoAction, Some("FKey")),
                                  PrimaryKey(Some("Test")),
                                  Unique(Some("ColumnAUnique"))))),
          List()
        )))
    )
  }

  test("ALTER TABLE") {
    val Right(schema) = createSchema("t14.sql")
    assert(
      schema == Schema(
        Map("Test" -> Relation(
          "Test",
          List("PKey"),
          None,
          List(
            HeadingAttribute("D", IntegerType, List(NULL(None))),
            HeadingAttribute("PKey", IntegerType, List(PrimaryKey(None))),
            HeadingAttribute("B", DateType, List()),
            HeadingAttribute("C",
                             IntegerType,
                             List(ForeignKey("X", "TestB", NoAction, Cascade, Some("FKey"))))
          ),
          List()
        )))
    )
  }

  test("SELECT * FROM Products") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t15.sql", schema)
    } yield result
    assert(rows.size == 77)
  }

  test("SELECT CustomerID, CompanyName, Region FROM Customers") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t16.sql", schema)
    } yield result
    assert(rows.size == 91)
  }

  test("SELECT CustomerID, CompanyName, Region FROM Customers WHERE Country = 'Poland'") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t17.sql", schema)
    } yield result
    assert(
      rows == List(
        Row(List(BodyAttribute("CustomerID", StringLiteral("WOLZA")),
                 BodyAttribute("CompanyName", StringLiteral("Wolski  Zajazd")),
                 BodyAttribute("Region", NULLLiteral)))))
  }

  test("SELECT * FROM Products WHERE UnitPrice >= 20.0 AND UnitPrice < 30.0") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t18.sql", schema)
    } yield result
    assert(rows.size == 13)
  }

  test("SELECT * FROM Products WHERE UnitPrice <= 20.0 AND UnitPrice >= 30.0") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t19.sql", schema)
    } yield result
    assert(rows.isEmpty)
  }

  test("SELECT * FROM Customers WHERE Region IS NULL") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t20.sql", schema)
    } yield result
    assert(rows.size == 60)
  }

  test("SELECT * FROM Customers WHERE Region IS NOT NULL") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t21.sql", schema)
    } yield result
    assert(rows.size == 31)
  }

  test("SELECT * FROM Customers WHERE NOT Region IS NULL ORDER BY Country, City") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t22.sql", schema)
    } yield result
    assert(
      rows.head
        .projectOption("Country")
        .contains(BodyAttribute("Country", StringLiteral("Brazil"))))
  }

  test("SELECT AVG(UnitPrice) FROM Products") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t23.sql", schema)
    } yield result
    assert(
      rows == List(Row(List(BodyAttribute("Avg(UnitPrice)", RealLiteral(28.866363636363637))))))
  }

  test("SELECT CategoryID, AVG(UnitPrice) FROM Products GROUP BY CategoryID") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t24.sql", schema)
    } yield result
    assert(
      rows ==
        List(
          Row(List(BodyAttribute("CategoryID", IntegerLiteral(8)),
                   BodyAttribute("Avg(UnitPrice)", RealLiteral(20.6825)))),
          Row(List(BodyAttribute("CategoryID", IntegerLiteral(1)),
                   BodyAttribute("Avg(UnitPrice)", RealLiteral(37.979166666666664)))),
          Row(List(BodyAttribute("CategoryID", IntegerLiteral(4)),
                   BodyAttribute("Avg(UnitPrice)", RealLiteral(28.73)))),
          Row(List(BodyAttribute("CategoryID", IntegerLiteral(3)),
                   BodyAttribute("Avg(UnitPrice)", RealLiteral(25.16)))),
          Row(List(BodyAttribute("CategoryID", IntegerLiteral(5)),
                   BodyAttribute("Avg(UnitPrice)", RealLiteral(20.25)))),
          Row(List(BodyAttribute("CategoryID", IntegerLiteral(6)),
                   BodyAttribute("Avg(UnitPrice)", RealLiteral(54.00666666666667)))),
          Row(List(BodyAttribute("CategoryID", IntegerLiteral(2)),
                   BodyAttribute("Avg(UnitPrice)", RealLiteral(23.0625)))),
          Row(List(BodyAttribute("CategoryID", IntegerLiteral(7)),
                   BodyAttribute("Avg(UnitPrice)", RealLiteral(32.37))))
        ))
  }

  test("SELECT 1 + 2 * 3 - 4") {
    val Right(RowsResult(rows)) = SQLInterpreter.runFromResource("t25.sql")
    assert(rows == List(Row(List(BodyAttribute("1+2*3-4", IntegerLiteral(3))))))
  }

  test("SELECT EmployeeID * 2 FROM Employees WHERE EmployeeID = 5") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t26.sql", schema)
    } yield result
    assert(rows == List(Row(List(BodyAttribute("EmployeeID*2", IntegerLiteral(10))))))
  }

  test(
    "SELECT P.CategoryID, CategoryName, AVG(UnitPrice) FROM Products AS P " +
      "JOIN Categories ON P.CategoryID=Categories.CategoryID GROUP BY P.CategoryID, CategoryName") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t27.sql", schema)
    } yield result
    assert(
      rows ==
        List(
          Row(List(
            BodyAttribute("P.CategoryID", IntegerLiteral(7)),
            BodyAttribute("CategoryName", StringLiteral("Produce")),
            BodyAttribute("Avg(UnitPrice)", RealLiteral(32.37))
          )),
          Row(List(
            BodyAttribute("P.CategoryID", IntegerLiteral(1)),
            BodyAttribute("CategoryName", StringLiteral("Beverages")),
            BodyAttribute("Avg(UnitPrice)", RealLiteral(37.979166666666664))
          )),
          Row(List(
            BodyAttribute("P.CategoryID", IntegerLiteral(5)),
            BodyAttribute("CategoryName", StringLiteral("Grains/Cereals")),
            BodyAttribute("Avg(UnitPrice)", RealLiteral(20.25))
          )),
          Row(List(
            BodyAttribute("P.CategoryID", IntegerLiteral(8)),
            BodyAttribute("CategoryName", StringLiteral("Seafood")),
            BodyAttribute("Avg(UnitPrice)", RealLiteral(20.6825))
          )),
          Row(List(
            BodyAttribute("P.CategoryID", IntegerLiteral(6)),
            BodyAttribute("CategoryName", StringLiteral("Meat/Poultry")),
            BodyAttribute("Avg(UnitPrice)", RealLiteral(54.00666666666667))
          )),
          Row(List(
            BodyAttribute("P.CategoryID", IntegerLiteral(4)),
            BodyAttribute("CategoryName", StringLiteral("Dairy Products")),
            BodyAttribute("Avg(UnitPrice)", RealLiteral(28.73))
          )),
          Row(List(
            BodyAttribute("P.CategoryID", IntegerLiteral(3)),
            BodyAttribute("CategoryName", StringLiteral("Confections")),
            BodyAttribute("Avg(UnitPrice)", RealLiteral(25.16))
          )),
          Row(List(
            BodyAttribute("P.CategoryID", IntegerLiteral(2)),
            BodyAttribute("CategoryName", StringLiteral("Condiments")),
            BodyAttribute("Avg(UnitPrice)", RealLiteral(23.0625))
          ))
        ))
  }

  test("SELECT COUNT(*) FROM Customers") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t28.sql", schema)
    } yield result
    assert(
      rows == List(Row(List(BodyAttribute("Count(*)", IntegerLiteral(91)))))
    )
  }

  test("SELECT * FROM Customers WHERE Country LIKE 'Poland' OR Country LIKE 'Germany'") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t29.sql", schema)
    } yield result
    assert(rows.size == 12)
  }

  test("SELECT * FROM Customers WHERE CustomerID LIKE 'N%' OR CustomerID LIKE 'C%'") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t30.sql", schema)
    } yield result
    assert(rows.size == 6)
  }

  test(
    "SELECT ProductName, CategoryName FROM Products JOIN Categories ON Products.CategoryID = Categories.CategoryID WHERE CategoryName LIKE 'C%'") {
//    val Right(RowsResult(rows)) = for {
//      schema <- northwind
//      result <- SQLInterpreter.runFromResource("t31.sql", schema)
//    } yield result
//    println(RelationPrinter.makeString(rows))
  }

  test("SELECT * FROM Customers WHERE REGION IS NULL") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t32.sql", schema)
    } yield result
    assert(rows.size == 60)
  }

  test(
    "SELECT DISTINCT COUNTRY FROM Customers JOIN Orders ON Customers.CustomerID = Orders.CustomerID") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t33.sql", schema)
    } yield result
    assert(rows.size == 21)
  }

  test(
    "SELECT ProductName FROM Products JOIN Categories ON Products.CategoryID = Categories.CategoryID " +
      "WHERE CategoryName LIKE 'Beverages' AND UnitPrice BETWEEN 20 AND 30") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t34.sql", schema)
    } yield result
    assert(rows.isEmpty)
  }

  test(
    "SELECT ContactName FROM Customers LEFT JOIN Orders ON Customers.CustomerID = Orders.CustomerID " +
      "WHERE OrderID IS NULL") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t35.sql", schema)
    } yield result
    assert(rows.size == 91)
  }

  test(
    "SELECT Country, COUNT(*) FROM Customers GROUP BY Country HAVING Country LIKE 'Poland' " +
      "OR Country LIKE 'Germany'") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t36.sql", schema)
    } yield result
    assert(
      rows ==
        List(
          Row(List(BodyAttribute("Country", StringLiteral("Poland")),
                   BodyAttribute("Count(*)", IntegerLiteral(1)))),
          Row(List(BodyAttribute("Country", StringLiteral("Germany")),
                   BodyAttribute("Count(*)", IntegerLiteral(11))))
        ))
  }

  test("SELECT CategoryID, AVG(UnitPrice) AS AveragePrice FROM Products GROUP BY CategoryID") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t37.sql", schema)
    } yield result
    assert(
      rows ==
        List(
          Row(
            List(BodyAttribute("CategoryID", IntegerLiteral(8)),
                 BodyAttribute("AveragePrice", RealLiteral(20.6825)))),
          Row(
            List(BodyAttribute("CategoryID", IntegerLiteral(1)),
                 BodyAttribute("AveragePrice", RealLiteral(37.979166666666664)))),
          Row(
            List(BodyAttribute("CategoryID", IntegerLiteral(4)),
                 BodyAttribute("AveragePrice", RealLiteral(28.73)))),
          Row(
            List(BodyAttribute("CategoryID", IntegerLiteral(3)),
                 BodyAttribute("AveragePrice", RealLiteral(25.16)))),
          Row(
            List(BodyAttribute("CategoryID", IntegerLiteral(5)),
                 BodyAttribute("AveragePrice", RealLiteral(20.25)))),
          Row(
            List(BodyAttribute("CategoryID", IntegerLiteral(6)),
                 BodyAttribute("AveragePrice", RealLiteral(54.00666666666667)))),
          Row(
            List(BodyAttribute("CategoryID", IntegerLiteral(2)),
                 BodyAttribute("AveragePrice", RealLiteral(23.0625)))),
          Row(
            List(BodyAttribute("CategoryID", IntegerLiteral(7)),
                 BodyAttribute("AveragePrice", RealLiteral(32.37))))
        )
    )
  }

  test("SELECT Orders.OrderID, OrderDate, Products.ProductID, ProductName, " +
    "[Order Details].UnitPrice, Quantity, CategoryName, Customers.CustomerID, " +
    "CompanyName FROM Customers JOIN Orders ON Customers.CustomerID = Orders.CustomerID " +
    "JOIN [Order Details] ON Orders.OrderID = [Order Details].OrderID " +
    "JOIN Products ON [Order Details].ProductID = Products.ProductID " +
    "JOIN Categories ON Products.CategoryID = Categories.CategoryID") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t38.sql", schema)
    } yield result
    assert(rows.size == 2155)
  }
}
