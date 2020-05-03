package io.github.typeness.rdbms
import cats.syntax.traverse._
import io.github.typeness.rdbms.TestUtils._
import org.scalatest.funsuite.AnyFunSuite

class IntegrationTest extends AnyFunSuite {

  private lazy val pracownicyUrlopy = createSchemaFromFile("pracownicyUrlopySchema.sql")
  private lazy val pracownicyUrlopy2 = createSchemaFromFile("pracownicyUrlopySchema2.sql")
  private lazy val northwind = createSchemaFromFile("northwind.sql")

  test("Create empty schema") {
    val Right(SchemaResult(schema)) = SQLInterpreter.runFromResource("empty.sql")
    val Right(RowsResult(rows)) = SQLInterpreter.runFromResource("t1.sql", schema)
    assert(rows == Nil)
  }

  test("Create PracownicyUrlopySchema") {
    assert(
      pracownicyUrlopy == Right(
        Schema(Map(
          rel"Pracownicy" -> Relation(
            rel"Pracownicy",
            List(col"Nr"),
            None,
            List(
              HeadingAttribute(col"Nr", IntegerType, List(PrimaryKey())),
              HeadingAttribute(col"Nazwisko", NVarCharType(50), List(NotNULL())),
              HeadingAttribute(col"Imie", NVarCharType(50), List(NotNULL())),
              HeadingAttribute(col"Stawka", IntegerType, List()),
              HeadingAttribute(col"DataZatrudnienia", DateType, List()),
              HeadingAttribute(col"LiczbaDzieci", IntegerType, List())
            ),
            List(
              Row(List(
                BodyAttribute(col"Nr", IntegerLiteral(3)),
                BodyAttribute(col"Nazwisko", StringLiteral("Wrona")),
                BodyAttribute(col"Imie", StringLiteral("Adam")),
                BodyAttribute(col"Stawka", IntegerLiteral(1100)),
                BodyAttribute(col"DataZatrudnienia", DateLiteral("'2015-01-01'")),
                BodyAttribute(col"LiczbaDzieci", IntegerLiteral(2))
              )),
              Row(List(
                BodyAttribute(col"Nr", IntegerLiteral(2)),
                BodyAttribute(col"Nazwisko", StringLiteral("Nowak")),
                BodyAttribute(col"Imie", StringLiteral("Anna")),
                BodyAttribute(col"Stawka", IntegerLiteral(1600)),
                BodyAttribute(col"DataZatrudnienia", DateLiteral("'2012-01-01'")),
                BodyAttribute(col"LiczbaDzieci", IntegerLiteral(1))
              )),
              Row(List(
                BodyAttribute(col"Nr", IntegerLiteral(1)),
                BodyAttribute(col"Nazwisko", StringLiteral("Kowal")),
                BodyAttribute(col"Imie", StringLiteral("Piotr")),
                BodyAttribute(col"Stawka", IntegerLiteral(1500)),
                BodyAttribute(col"DataZatrudnienia", DateLiteral("'2010-01-01'")),
                BodyAttribute(col"LiczbaDzieci", IntegerLiteral(2))
              ))
            ),
            Nil
          ),
          rel"Urlopy" -> Relation(
            rel"Urlopy",
            List(col"NrPrac", col"OdKiedy"),
            None,
            List(
              HeadingAttribute(col"NrPrac",
                               IntegerType,
                               List(ForeignKey(col"Nr", rel"Pracownicy", NoAction, NoAction),
                                    PrimaryKey())),
              HeadingAttribute(col"OdKiedy", DateType, List(PrimaryKey())),
              HeadingAttribute(col"DoKiedy", DateType, List())
            ),
            List(),
            List(PKeyRelationConstraint(List("NrPrac", "OdKiedy"),None), FKeyRelationConstraint(List("NrPrac"), rel"Pracownicy" , col"Nr",NoAction,NoAction,None))
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
          BodyAttribute(col"Nr", IntegerLiteral(1)),
          BodyAttribute(col"Nazwisko", StringLiteral("Kowal")),
          BodyAttribute(col"Imie", StringLiteral("Piotr")),
          BodyAttribute(col"Stawka", IntegerLiteral(1500)),
          BodyAttribute(col"DataZatrudnienia", DateLiteral("'2010-01-01'")),
          BodyAttribute(col"LiczbaDzieci", IntegerLiteral(2))
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
        PrimaryKeyDoesNotExist(rel"Urlopy", col"NrPrac", rel"Pracownicy" , col"Nr", IntegerLiteral(23416))))
  }

  test("Success when inserting existing primary key as foreign key") {
    val Right(SchemaResult(result)) = for {
      schema <- pracownicyUrlopy
      newSchema <- SQLInterpreter.runFromResource("t4.sql", schema)
    } yield newSchema
    val urlopy = result.getRelation(rel"Urlopy").map(_.body)
    assert(
      urlopy == Right(
        List(Row(List(BodyAttribute(col"NrPrac", IntegerLiteral(1)),
                      BodyAttribute(col"OdKiedy", DateLiteral("'2015-01-01'")),
                      BodyAttribute(col"DoKiedy", DateLiteral("'2015-01-05'")))))))
  }

  test("Failure when inserting duplicate primary key") {
    val Right(SchemaResult(newSchema)) = for {
      schema <- pracownicyUrlopy
      newSchema <- SQLInterpreter.runFromResource("t4.sql", schema)
    } yield newSchema
    val result = SQLInterpreter.runFromResource("t5.sql", newSchema)
    assert(
      result == Left(
        PrimaryKeyDuplicate(List(BodyAttribute(col"NrPrac", IntegerLiteral(1)),
                                 BodyAttribute(col"OdKiedy", DateLiteral("'2015-01-01'"))))))
  }

  test("Success when inserting unique primary key") {
    val Right(SchemaResult(newSchema)) = for {
      schema <- pracownicyUrlopy
      newSchema <- SQLInterpreter.runFromResource("t4.sql", schema)
    } yield newSchema
    val Right(SchemaResult(result)) = SQLInterpreter.runFromResource("t6.sql", newSchema)
    val urlopy = result.getRelation(rel"Urlopy")
    assert(
      urlopy == Right(Relation(
        rel"Urlopy",
        List(col"NrPrac", col"OdKiedy"),
        None,
        List(
          HeadingAttribute(col"NrPrac",
                           IntegerType,
                           List(ForeignKey(col"Nr", rel"Pracownicy", NoAction, NoAction), PrimaryKey())),
          HeadingAttribute(col"OdKiedy", DateType, List(PrimaryKey())),
          HeadingAttribute(col"DoKiedy", DateType, List())
        ),
        List(
          Row(List(BodyAttribute(col"NrPrac", IntegerLiteral(1)),
                   BodyAttribute(col"OdKiedy", DateLiteral("'2015-02-01'")),
                   BodyAttribute(col"DoKiedy", DateLiteral("'2015-02-07'")))),
          Row(List(BodyAttribute(col"NrPrac", IntegerLiteral(1)),
                   BodyAttribute(col"OdKiedy", DateLiteral("'2015-01-01'")),
                   BodyAttribute(col"DoKiedy", DateLiteral("'2015-01-05'"))))
        ),
        List(PKeyRelationConstraint(List("NrPrac", "OdKiedy"),None), FKeyRelationConstraint(List("NrPrac"), rel"Pracownicy" ,col"Nr",NoAction,NoAction,None))
      )))
  }

  test("Failure when deleting row with primary key referenced in other relation") {
    val Right(SchemaResult(newSchema)) = for {
      schema <- pracownicyUrlopy
      newSchema <- SQLInterpreter.runFromResource("t4.sql", schema)
    } yield newSchema
    val result = SQLInterpreter.runFromResource("t7.sql", newSchema)
    assert(result == Left(ForeignKeyViolation(rel"Urlopy" , col"NrPrac")))
  }

  test("Success when deleting row with primary key not referenced in other relation") {
    val Right(SchemaResult(newSchema)) = for {
      schema <- pracownicyUrlopy
      newSchema <- SQLInterpreter.runFromResource("t4.sql", schema)
    } yield newSchema
    val Right(SchemaResult(result)) = SQLInterpreter.runFromResource("t8.sql", newSchema)
    val isDeleted = result
      .getRelation(rel"Pracownicy")
      .flatMap(_.body.traverse(_.projectEither(col"Nr").map(_.literal.show)))
    assert(isDeleted.map(_.has("2")) == Right(false))
  }

  test("Cascade trigger when deleting row with primary key referenced in other relation") {
    val Right(SchemaResult(newSchema)) = for {
      schema <- pracownicyUrlopy2
      newSchema <- SQLInterpreter.runFromResource("t4.sql", schema)
    } yield newSchema
    val Right(SchemaResult(result)) = SQLInterpreter.runFromResource("t7.sql", newSchema)
    val urlopyBody = result.getRelation(rel"Urlopy").map(_.body)
    assert(urlopyBody == Right(Nil))
  }

  test("Cascade trigger when updating row with primary key referenced in other relation") {
    val Right(SchemaResult(newSchema)) = for {
      schema <- pracownicyUrlopy2
      newSchema <- SQLInterpreter.runFromResource("t4.sql", schema)
    } yield newSchema
    val Right(SchemaResult(result)) = SQLInterpreter.runFromResource("t10.sql", newSchema)
    val urlopyBody =
      result.getRelation(rel"Urlopy").flatMap(_.body.traverse(_.projectEither(col"NrPrac")))
    assert(urlopyBody == Right(List(BodyAttribute(col"NrPrac", IntegerLiteral(333)))))
  }

  test("Failure when setting non-existing primary key as foreign key") {
    val Right(SchemaResult(newSchema)) = for {
      schema <- pracownicyUrlopy
      newSchema <- SQLInterpreter.runFromResource("t4.sql", schema)
    } yield newSchema
    val result = SQLInterpreter.runFromResource("t11.sql", newSchema)
    assert(
      result == Left(
        PrimaryKeyDoesNotExist(rel"Urlopy", col"NrPrac", rel"Pracownicy", col"Nr", IntegerLiteral(2512))))
  }

  test("Column alias") {
    val Right(RowsResult(rows)) = for {
      schema <- pracownicyUrlopy
      result <- SQLInterpreter.runFromResource("t12.sql", schema)
    } yield result
    assert(
      rows == List(
        Row(List(BodyAttribute(col"Alias", StringLiteral("Wrona")),
                 BodyAttribute(col"Imie", StringLiteral("Adam")))),
        Row(List(BodyAttribute(col"Alias", StringLiteral("Nowak")),
                 BodyAttribute(col"Imie", StringLiteral("Anna")))),
        Row(List(BodyAttribute(col"Alias", StringLiteral("Kowal")),
                 BodyAttribute(col"Imie", StringLiteral("Piotr"))))
      ))
  }

  test("LIKE operator") {
    val Right(RowsResult(rows)) = for {
      schema <- pracownicyUrlopy
      result <- SQLInterpreter.runFromResource("t9.sql", schema)
    } yield result
    assert(
      rows == List(
        Row(List(BodyAttribute(col"Imie", StringLiteral("Anna")),
                 BodyAttribute(col"Nazwisko", StringLiteral("Nowak")))),
        Row(List(BodyAttribute(col"Imie", StringLiteral("Adam")),
                 BodyAttribute(col"Nazwisko", StringLiteral("Wrona"))))
      ))
  }

  test("Named constraints") {
    val Right(schema) = createSchemaFromFile("t13.sql")
    assert(
      schema == Schema(
        Map(rel"Dbo" -> Relation(
          rel"Dbo",
          List(col"ColumnA"),
          None,
          List(
            HeadingAttribute(col"ColumnA",
                             IntegerType,
                             List(ForeignKey(col"B", rel"TAB", NoAction, NoAction, Some("FKey")),
                                  PrimaryKey(Some("Test")),
                                  Unique(Some("ColumnAUnique"))))),
          List(),
          List(PKeyRelationConstraint(List("ColumnA"),Some("Test")), FKeyRelationConstraint(List("ColumnA"), rel"TAB",col"B",NoAction,NoAction,Some("FKey")))
        )))
    )
  }

  test("ALTER TABLE") {
    val Right(schema) = createSchemaFromFile("t14.sql")
    assert(
      schema == Schema(
        Map(rel"Test" -> Relation(
          rel"Test",
          List(col"PKey"),
          None,
          List(
            HeadingAttribute(col"D", IntegerType, List(NULL(None))),
            HeadingAttribute(col"PKey", IntegerType, List(PrimaryKey(None))),
            HeadingAttribute(col"B", DateType, List()),
            HeadingAttribute(col"C",
                             IntegerType,
                             List(ForeignKey(col"X", rel"TestB", NoAction, Cascade, Some("FKey"))))
          ),
          List(),
          List(FKeyRelationConstraint(List("C"), rel"TestB",col"X",Cascade,NoAction,Some("FKey")))
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
        Row(List(BodyAttribute(col"CustomerID", StringLiteral("WOLZA")),
                 BodyAttribute(col"CompanyName", StringLiteral("Wolski  Zajazd")),
                 BodyAttribute(col"Region", NULLLiteral)))))
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
        .projectOption(col"Country")
        .has(BodyAttribute(col"Country", StringLiteral("Brazil"))))
  }

  test("SELECT AVG(UnitPrice) FROM Products") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t23.sql", schema)
    } yield result
    assert(
      rows == List(Row(List(BodyAttribute(col"Avg(UnitPrice)", RealLiteral(28.866363636363637))))))
  }

  test("SELECT CategoryID, AVG(UnitPrice) FROM Products GROUP BY CategoryID") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t24.sql", schema)
    } yield result
    assert(
      rows.toSet ==
        Set(
          Row(List(BodyAttribute(col"CategoryID", IntegerLiteral(8)),
                   BodyAttribute(col"Avg(UnitPrice)", RealLiteral(20.6825)))),
          Row(List(BodyAttribute(col"CategoryID", IntegerLiteral(1)),
                   BodyAttribute(col"Avg(UnitPrice)", RealLiteral(37.979166666666664)))),
          Row(List(BodyAttribute(col"CategoryID", IntegerLiteral(4)),
                   BodyAttribute(col"Avg(UnitPrice)", RealLiteral(28.73)))),
          Row(List(BodyAttribute(col"CategoryID", IntegerLiteral(3)),
                   BodyAttribute(col"Avg(UnitPrice)", RealLiteral(25.16)))),
          Row(List(BodyAttribute(col"CategoryID", IntegerLiteral(5)),
                   BodyAttribute(col"Avg(UnitPrice)", RealLiteral(20.25)))),
          Row(List(BodyAttribute(col"CategoryID", IntegerLiteral(6)),
                   BodyAttribute(col"Avg(UnitPrice)", RealLiteral(54.00666666666667)))),
          Row(List(BodyAttribute(col"CategoryID", IntegerLiteral(2)),
                   BodyAttribute(col"Avg(UnitPrice)", RealLiteral(23.0625)))),
          Row(List(BodyAttribute(col"CategoryID", IntegerLiteral(7)),
                   BodyAttribute(col"Avg(UnitPrice)", RealLiteral(32.37))))
        ))
  }

  test("SELECT 1 + 2 * 3 - 4") {
    val Right(RowsResult(rows)) = SQLInterpreter.runFromResource("t25.sql")
    assert(rows == List(Row(List(BodyAttribute(col"1+2*3-4", IntegerLiteral(3))))))
  }

  test("SELECT EmployeeID * 2 FROM Employees WHERE EmployeeID = 5") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t26.sql", schema)
    } yield result
    assert(rows == List(Row(List(BodyAttribute(col"EmployeeID*2", IntegerLiteral(10))))))
  }

  test(
    "SELECT P.CategoryID, CategoryName, AVG(UnitPrice) FROM Products AS P " +
      "JOIN Categories ON P.CategoryID=Categories.CategoryID GROUP BY P.CategoryID, CategoryName") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t27.sql", schema)
    } yield result
    assert(
      rows.toSet ==
        Set(
          Row(List(
            BodyAttribute(col"P.CategoryID", IntegerLiteral(7)),
            BodyAttribute(col"CategoryName", StringLiteral("Produce")),
            BodyAttribute(col"Avg(UnitPrice)", RealLiteral(32.37))
          )),
          Row(List(
            BodyAttribute(col"P.CategoryID", IntegerLiteral(1)),
            BodyAttribute(col"CategoryName", StringLiteral("Beverages")),
            BodyAttribute(col"Avg(UnitPrice)", RealLiteral(37.979166666666664))
          )),
          Row(List(
            BodyAttribute(col"P.CategoryID", IntegerLiteral(5)),
            BodyAttribute(col"CategoryName", StringLiteral("Grains/Cereals")),
            BodyAttribute(col"Avg(UnitPrice)", RealLiteral(20.25))
          )),
          Row(List(
            BodyAttribute(col"P.CategoryID", IntegerLiteral(8)),
            BodyAttribute(col"CategoryName", StringLiteral("Seafood")),
            BodyAttribute(col"Avg(UnitPrice)", RealLiteral(20.6825))
          )),
          Row(List(
            BodyAttribute(col"P.CategoryID", IntegerLiteral(6)),
            BodyAttribute(col"CategoryName", StringLiteral("Meat/Poultry")),
            BodyAttribute(col"Avg(UnitPrice)", RealLiteral(54.00666666666667))
          )),
          Row(List(
            BodyAttribute(col"P.CategoryID", IntegerLiteral(4)),
            BodyAttribute(col"CategoryName", StringLiteral("Dairy Products")),
            BodyAttribute(col"Avg(UnitPrice)", RealLiteral(28.73))
          )),
          Row(List(
            BodyAttribute(col"P.CategoryID", IntegerLiteral(3)),
            BodyAttribute(col"CategoryName", StringLiteral("Confections")),
            BodyAttribute(col"Avg(UnitPrice)", RealLiteral(25.16))
          )),
          Row(List(
            BodyAttribute(col"P.CategoryID", IntegerLiteral(2)),
            BodyAttribute(col"CategoryName", StringLiteral("Condiments")),
            BodyAttribute(col"Avg(UnitPrice)", RealLiteral(23.0625))
          ))
        ))
  }

  test("SELECT COUNT(*) FROM Customers") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t28.sql", schema)
    } yield result
    assert(
      rows == List(Row(List(BodyAttribute(col"Count(*)", IntegerLiteral(91)))))
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
          Row(List(BodyAttribute(col"Country", StringLiteral("Poland")),
                   BodyAttribute(col"Count(*)", IntegerLiteral(1)))),
          Row(List(BodyAttribute(col"Country", StringLiteral("Germany")),
                   BodyAttribute(col"Count(*)", IntegerLiteral(11))))
        ))
  }

  test("SELECT CategoryID, AVG(UnitPrice) AS AveragePrice FROM Products GROUP BY CategoryID") {
    val Right(RowsResult(rows)) = for {
      schema <- northwind
      result <- SQLInterpreter.runFromResource("t37.sql", schema)
    } yield result
    assert(
      rows.toSet ==
        Set(
          Row(
            List(BodyAttribute(col"CategoryID", IntegerLiteral(8)),
                 BodyAttribute(col"AveragePrice", RealLiteral(20.6825)))),
          Row(
            List(BodyAttribute(col"CategoryID", IntegerLiteral(1)),
                 BodyAttribute(col"AveragePrice", RealLiteral(37.979166666666664)))),
          Row(
            List(BodyAttribute(col"CategoryID", IntegerLiteral(4)),
                 BodyAttribute(col"AveragePrice", RealLiteral(28.73)))),
          Row(
            List(BodyAttribute(col"CategoryID", IntegerLiteral(3)),
                 BodyAttribute(col"AveragePrice", RealLiteral(25.16)))),
          Row(
            List(BodyAttribute(col"CategoryID", IntegerLiteral(5)),
                 BodyAttribute(col"AveragePrice", RealLiteral(20.25)))),
          Row(
            List(BodyAttribute(col"CategoryID", IntegerLiteral(6)),
                 BodyAttribute(col"AveragePrice", RealLiteral(54.00666666666667)))),
          Row(
            List(BodyAttribute(col"CategoryID", IntegerLiteral(2)),
                 BodyAttribute(col"AveragePrice", RealLiteral(23.0625)))),
          Row(
            List(BodyAttribute(col"CategoryID", IntegerLiteral(7)),
                 BodyAttribute(col"AveragePrice", RealLiteral(32.37))))
        )
    )
  }

  test(
    "SELECT Orders.OrderID, OrderDate, Products.ProductID, ProductName, " +
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
