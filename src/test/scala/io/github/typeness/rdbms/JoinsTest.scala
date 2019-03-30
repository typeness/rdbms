package io.github.typeness.rdbms

import org.scalatest.FunSuite

class JoinsTest extends FunSuite {

  import Relations._

  test("CROSS JOIN on 2 relations") {
    val join = Select(
      List(Var("a"), Var("b")),
      Some("RelationA"),
      List(CrossJoin("RelationB")),
      None,
      Nil,
      None,
      Nil
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
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("CROSS JOIN on 3 relations") {
    val join = Select(
      List(Var("a"), Var("b"), Var("c")),
      Some("RelationA"),
      List(CrossJoin("RelationB"), CrossJoin("RelationC")),
      None,
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(1)),
          BodyAttribute("b", IntegerLiteral(2)),
          BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(1)),
          BodyAttribute("b", IntegerLiteral(2)),
          BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(1)),
          BodyAttribute("b", IntegerLiteral(2)),
          BodyAttribute("c", IntegerLiteral(5))),
      Row(BodyAttribute("a", IntegerLiteral(1)),
          BodyAttribute("b", IntegerLiteral(3)),
          BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(1)),
          BodyAttribute("b", IntegerLiteral(3)),
          BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(1)),
          BodyAttribute("b", IntegerLiteral(3)),
          BodyAttribute("c", IntegerLiteral(5))),
      Row(BodyAttribute("a", IntegerLiteral(1)),
          BodyAttribute("b", IntegerLiteral(4)),
          BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(1)),
          BodyAttribute("b", IntegerLiteral(4)),
          BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(1)),
          BodyAttribute("b", IntegerLiteral(4)),
          BodyAttribute("c", IntegerLiteral(5))),
      Row(BodyAttribute("a", IntegerLiteral(2)),
          BodyAttribute("b", IntegerLiteral(2)),
          BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(2)),
          BodyAttribute("b", IntegerLiteral(2)),
          BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(2)),
          BodyAttribute("b", IntegerLiteral(2)),
          BodyAttribute("c", IntegerLiteral(5))),
      Row(BodyAttribute("a", IntegerLiteral(2)),
          BodyAttribute("b", IntegerLiteral(3)),
          BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(2)),
          BodyAttribute("b", IntegerLiteral(3)),
          BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(2)),
          BodyAttribute("b", IntegerLiteral(3)),
          BodyAttribute("c", IntegerLiteral(5))),
      Row(BodyAttribute("a", IntegerLiteral(2)),
          BodyAttribute("b", IntegerLiteral(4)),
          BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(2)),
          BodyAttribute("b", IntegerLiteral(4)),
          BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(2)),
          BodyAttribute("b", IntegerLiteral(4)),
          BodyAttribute("c", IntegerLiteral(5))),
      Row(BodyAttribute("a", IntegerLiteral(3)),
          BodyAttribute("b", IntegerLiteral(2)),
          BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(3)),
          BodyAttribute("b", IntegerLiteral(2)),
          BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(3)),
          BodyAttribute("b", IntegerLiteral(2)),
          BodyAttribute("c", IntegerLiteral(5))),
      Row(BodyAttribute("a", IntegerLiteral(3)),
          BodyAttribute("b", IntegerLiteral(3)),
          BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(3)),
          BodyAttribute("b", IntegerLiteral(3)),
          BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(3)),
          BodyAttribute("b", IntegerLiteral(3)),
          BodyAttribute("c", IntegerLiteral(5))),
      Row(BodyAttribute("a", IntegerLiteral(3)),
          BodyAttribute("b", IntegerLiteral(4)),
          BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", IntegerLiteral(3)),
          BodyAttribute("b", IntegerLiteral(4)),
          BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", IntegerLiteral(3)),
          BodyAttribute("b", IntegerLiteral(4)),
          BodyAttribute("c", IntegerLiteral(5))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("INNER JOIN on 2 relations") {
    val join = Select(
      List(Var("a"), Var("b")),
      Some("RelationA"),
      List(InnerJoin("RelationB", Equals(Var("a"), Var("b")))),
      None,
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(2))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(3))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("INNER JOIN on 3 relations") {
    val join = Select(
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
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(3)),
          BodyAttribute("b", IntegerLiteral(3)),
          BodyAttribute("c", IntegerLiteral(3))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("LEFT OUTER JOIN on 2 relations") {
    val join = Select(
      List(Var("a"), Var("b")),
      Some("RelationA"),
      List(LeftOuterJoin("RelationB", Equals(Var("a"), Var("b")))),
      None,
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", NULLLiteral)),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(2))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(3))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("LEFT OUTER JOIN on 3 relations") {
    val join = Select(
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
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(1)),
          BodyAttribute("b", NULLLiteral),
          BodyAttribute("c", NULLLiteral)),
      Row(BodyAttribute("a", IntegerLiteral(2)),
          BodyAttribute("b", IntegerLiteral(2)),
          BodyAttribute("c", NULLLiteral)),
      Row(BodyAttribute("a", IntegerLiteral(3)),
          BodyAttribute("b", IntegerLiteral(3)),
          BodyAttribute("c", IntegerLiteral(3))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("RIGHT OUTER JOIN on 2 relations") {
    val join = Select(
      List(Var("a"), Var("b")),
      Some("RelationA"),
      List(RightOuterJoin("RelationB", Equals(Var("a"), Var("b")))),
      None,
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(2))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(3))),
      Row(BodyAttribute("a", NULLLiteral), BodyAttribute("b", IntegerLiteral(4))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("RIGHT OUTER JOIN on 3 relations") {
    val join = Select(
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
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(3)),
          BodyAttribute("b", IntegerLiteral(3)),
          BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", NULLLiteral),
          BodyAttribute("b", NULLLiteral),
          BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", NULLLiteral),
          BodyAttribute("b", NULLLiteral),
          BodyAttribute("c", IntegerLiteral(5))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("FULL OUTER JOIN on 2 relations") {
    val join = Select(
      List(Var("a"), Var("b")),
      Some("RelationA"),
      List(FullOuterJoin("RelationB", Equals(Var("a"), Var("b")))),
      None,
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(1)), BodyAttribute("b", NULLLiteral)),
      Row(BodyAttribute("a", IntegerLiteral(2)), BodyAttribute("b", IntegerLiteral(2))),
      Row(BodyAttribute("a", IntegerLiteral(3)), BodyAttribute("b", IntegerLiteral(3))),
      Row(BodyAttribute("a", NULLLiteral), BodyAttribute("b", IntegerLiteral(4))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("FULL OUTER JOIN on 3 relations") {
    val join = Select(
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
    val expected = List(
      Row(BodyAttribute("a", IntegerLiteral(1)),
          BodyAttribute("b", NULLLiteral),
          BodyAttribute("c", NULLLiteral)),
      Row(BodyAttribute("a", IntegerLiteral(2)),
          BodyAttribute("b", IntegerLiteral(2)),
          BodyAttribute("c", NULLLiteral)),
      Row(BodyAttribute("a", NULLLiteral),
          BodyAttribute("b", IntegerLiteral(4)),
          BodyAttribute("c", NULLLiteral)),
      Row(BodyAttribute("a", IntegerLiteral(3)),
          BodyAttribute("b", IntegerLiteral(3)),
          BodyAttribute("c", IntegerLiteral(3))),
      Row(BodyAttribute("a", NULLLiteral),
          BodyAttribute("b", NULLLiteral),
          BodyAttribute("c", IntegerLiteral(4))),
      Row(BodyAttribute("a", NULLLiteral),
          BodyAttribute("b", NULLLiteral),
          BodyAttribute("c", IntegerLiteral(5))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }
}
