package io.github.typeness.rdbms

import org.scalatest.FunSuite

class JoinsTest extends FunSuite {

  val relation1 = Relation(
    "Relation1",
    List("a"),
    None,
    List(HeadingAttribute("a", IntegerType, List(PrimaryKey))),
    List(
      Row(BodyAttribute("a", IntegerLiteral(1))),
      Row(BodyAttribute("a", IntegerLiteral(2))),
      Row(BodyAttribute("a", IntegerLiteral(3)))
    )
  )

  val relation2 = Relation(
    "Relation2",
    List("b"),
    None,
    List(HeadingAttribute("b", IntegerType, List(PrimaryKey))),
    List(
      Row(BodyAttribute("b", IntegerLiteral(2))),
      Row(BodyAttribute("b", IntegerLiteral(3))),
      Row(BodyAttribute("b", IntegerLiteral(4)))
    )
  )

  val relation3 = Relation(
    "Relation3",
    List("c"),
    None,
    List(HeadingAttribute("c", IntegerType, List(PrimaryKey))),
    List(
      Row(BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("c", IntegerLiteral(5)))
    )
  )

  val schema = Schema(List(relation1, relation2, relation3))

  test("CROSS JOIN on 2 relations") {
    val join = Select(
      List("a", "b"),
      "Relation1",
      List(CrossJoin("Relation2")),
      None,
      None
    )
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", IntegerLiteral(2))),
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(2))),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(2))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(4)))
    )
    val result = SelectionBuilder.select(join, schema)
    assert(result == Right(expected))
  }

  test("CROSS JOIN on 3 relations") {
    val join = Select(
      List("a", "b", "c"),
      "Relation1",
      List(CrossJoin("Relation2"), CrossJoin("Relation3")),
      None,
      None
    )
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", IntegerLiteral(2)), BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", IntegerLiteral(2)), BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", IntegerLiteral(2)), BodyAttribute("c", IntegerLiteral(5))),
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", IntegerLiteral(3)), BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", IntegerLiteral(3)), BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", IntegerLiteral(3)), BodyAttribute("c", IntegerLiteral(5))),
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", IntegerLiteral(4)), BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", IntegerLiteral(4)), BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", IntegerLiteral(4)), BodyAttribute("c", IntegerLiteral(5))),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(2)), BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(2)), BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(2)), BodyAttribute("c", IntegerLiteral(5))),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(3)), BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(3)), BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(3)), BodyAttribute("c", IntegerLiteral(5))),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(4)), BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(4)), BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(4)), BodyAttribute("c", IntegerLiteral(5))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(2)), BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(2)), BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(2)), BodyAttribute("c", IntegerLiteral(5))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(3)), BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(3)), BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(3)), BodyAttribute("c", IntegerLiteral(5))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(4)), BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(4)), BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(4)), BodyAttribute("c", IntegerLiteral(5))),
    )
    val result = SelectionBuilder.select(join, schema)
    assert(result == Right(expected))
  }

  test("INNER JOIN on 2 relations") {
    val join = Select(
      List("a", "b"),
      "Relation1",
      List(InnerJoin("Relation2", Equals("a", Var("b")))),
      None,
      None
    )
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(2))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(3))),
    )
    val result = SelectionBuilder.select(join, schema)
    assert(result == Right(expected))
  }

  test("INNER JOIN on 3 relations") {
    val join = Select(
      List("a", "b", "c"),
      "Relation1",
      List(
        InnerJoin("Relation2", Equals("a", Var("b"))),
        InnerJoin("Relation3", Equals("a", Var("c"))),
      ),
      None,
      None
    )
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(3)), BodyAttribute("c", IntegerLiteral(3))),
    )
    val result = SelectionBuilder.select(join, schema)
    assert(result == Right(expected))
  }

  test("LEFT OUTER JOIN on 2 relations") {
    val join = Select(
      List("a", "b"),
      "Relation1",
      List(LeftOuterJoin("Relation2", Equals("a", Var("b")))),
      None,
      None
    )
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", NULLLiteral)),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(2))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(3))),
    )
    val result = SelectionBuilder.select(join, schema)
    assert(result == Right(expected))
  }

  test("LEFT OUTER JOIN on 3 relations") {
    val join = Select(
      List("a", "b", "c"),
      "Relation1",
      List(
        LeftOuterJoin("Relation2", Equals("a", Var("b"))),
        LeftOuterJoin("Relation3", Equals("a", Var("c"))),
      ),
      None,
      None
    )
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", NULLLiteral), BodyAttribute("c", NULLLiteral)),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(2)), BodyAttribute("c", NULLLiteral)),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(3)), BodyAttribute("c", IntegerLiteral(3))),
    )
    val result = SelectionBuilder.select(join, schema)
    assert(result == Right(expected))
  }

  test("RIGHT OUTER JOIN on 2 relations") {
    val join = Select(
      List("a", "b"),
      "Relation1",
      List(RightOuterJoin("Relation2", Equals("a", Var("b")))),
      None,
      None
    )
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(2))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(3))),
      Row(BodyAttribute("a", NULLLiteral), BodyAttribute("b", IntegerLiteral(4))),
    )
    val result = SelectionBuilder.select(join, schema)
    assert(result == Right(expected))
  }

  test("RIGHT OUTER JOIN on 3 relations") {
    val join = Select(
      List("a", "b", "c"),
      "Relation1",
      List(
        RightOuterJoin("Relation2", Equals("a", Var("b"))),
        RightOuterJoin("Relation3", Equals("a", Var("c"))),
      ),
      None,
      None
    )
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(3)), BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", NULLLiteral), BodyAttribute("b", NULLLiteral), BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", NULLLiteral), BodyAttribute("b", NULLLiteral), BodyAttribute("c", IntegerLiteral(5))),
    )
    val result = SelectionBuilder.select(join, schema)
    assert(result == Right(expected))
  }

  test("FULL OUTER JOIN on 2 relations") {
    val join = Select(
      List("a", "b"),
      "Relation1",
      List(FullOuterJoin("Relation2", Equals("a", Var("b")))),
      None,
      None
    )
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", NULLLiteral)),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(2))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(3))),
      Row(BodyAttribute("a", NULLLiteral), BodyAttribute("b", IntegerLiteral(4))),
    )
    val result = SelectionBuilder.select(join, schema)
    assert(result == Right(expected))
  }

  test("FULL OUTER JOIN on 3 relations") {
    val join = Select(
      List("a", "b", "c"),
      "Relation1",
      List(
        FullOuterJoin("Relation2", Equals("a", Var("b"))),
        FullOuterJoin("Relation3", Equals("a", Var("c"))),
      ),
      None,
      None
    )
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", NULLLiteral), BodyAttribute("c", NULLLiteral)),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(2)), BodyAttribute("c", NULLLiteral)),
      Row(BodyAttribute("a", NULLLiteral), BodyAttribute("b", IntegerLiteral(4)), BodyAttribute("c", NULLLiteral)),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(3)), BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", NULLLiteral), BodyAttribute("b", NULLLiteral), BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", NULLLiteral), BodyAttribute("b", NULLLiteral), BodyAttribute("c", IntegerLiteral(5))),
    )
    val result = SelectionBuilder.select(join, schema)
    assert(result == Right(expected))
  }
}
