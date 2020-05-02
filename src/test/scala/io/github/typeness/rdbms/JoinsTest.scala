package io.github.typeness.rdbms

import org.scalatest.funsuite.AnyFunSuite

class JoinsTest extends AnyFunSuite {

  import Relations._

  test("CROSS JOIN on 2 relations") {
    val join = Select(
      List(Var(col"a"), Var(col"b")),
      Some(rel"RelationA"),
      List(CrossJoin(rel"RelationB")),
      None,
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute(col"a", IntegerLiteral(1)), BodyAttribute(col"b", IntegerLiteral(2))),
      Row(BodyAttribute(col"a", IntegerLiteral(1)), BodyAttribute(col"b", IntegerLiteral(3))),
      Row(BodyAttribute(col"a", IntegerLiteral(1)), BodyAttribute(col"b", IntegerLiteral(4))),
      Row(BodyAttribute(col"a", IntegerLiteral(2)), BodyAttribute(col"b", IntegerLiteral(2))),
      Row(BodyAttribute(col"a", IntegerLiteral(2)), BodyAttribute(col"b", IntegerLiteral(3))),
      Row(BodyAttribute(col"a", IntegerLiteral(2)), BodyAttribute(col"b", IntegerLiteral(4))),
      Row(BodyAttribute(col"a", IntegerLiteral(3)), BodyAttribute(col"b", IntegerLiteral(2))),
      Row(BodyAttribute(col"a", IntegerLiteral(3)), BodyAttribute(col"b", IntegerLiteral(3))),
      Row(BodyAttribute(col"a", IntegerLiteral(3)), BodyAttribute(col"b", IntegerLiteral(4)))
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("CROSS JOIN on 3 relations") {
    val join = Select(
      List(Var(col"a"), Var(col"b"), Var(col"c")),
      Some(rel"RelationA"),
      List(CrossJoin(rel"RelationB"), CrossJoin(rel"RelationC")),
      None,
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute(col"a", IntegerLiteral(1)),
          BodyAttribute(col"b", IntegerLiteral(2)),
          BodyAttribute(col"c", IntegerLiteral(3))),
      Row(BodyAttribute(col"a", IntegerLiteral(1)),
          BodyAttribute(col"b", IntegerLiteral(2)),
          BodyAttribute(col"c", IntegerLiteral(4))),
      Row(BodyAttribute(col"a", IntegerLiteral(1)),
          BodyAttribute(col"b", IntegerLiteral(2)),
          BodyAttribute(col"c", IntegerLiteral(5))),
      Row(BodyAttribute(col"a", IntegerLiteral(1)),
          BodyAttribute(col"b", IntegerLiteral(3)),
          BodyAttribute(col"c", IntegerLiteral(3))),
      Row(BodyAttribute(col"a", IntegerLiteral(1)),
          BodyAttribute(col"b", IntegerLiteral(3)),
          BodyAttribute(col"c", IntegerLiteral(4))),
      Row(BodyAttribute(col"a", IntegerLiteral(1)),
          BodyAttribute(col"b", IntegerLiteral(3)),
          BodyAttribute(col"c", IntegerLiteral(5))),
      Row(BodyAttribute(col"a", IntegerLiteral(1)),
          BodyAttribute(col"b", IntegerLiteral(4)),
          BodyAttribute(col"c", IntegerLiteral(3))),
      Row(BodyAttribute(col"a", IntegerLiteral(1)),
          BodyAttribute(col"b", IntegerLiteral(4)),
          BodyAttribute(col"c", IntegerLiteral(4))),
      Row(BodyAttribute(col"a", IntegerLiteral(1)),
          BodyAttribute(col"b", IntegerLiteral(4)),
          BodyAttribute(col"c", IntegerLiteral(5))),
      Row(BodyAttribute(col"a", IntegerLiteral(2)),
          BodyAttribute(col"b", IntegerLiteral(2)),
          BodyAttribute(col"c", IntegerLiteral(3))),
      Row(BodyAttribute(col"a", IntegerLiteral(2)),
          BodyAttribute(col"b", IntegerLiteral(2)),
          BodyAttribute(col"c", IntegerLiteral(4))),
      Row(BodyAttribute(col"a", IntegerLiteral(2)),
          BodyAttribute(col"b", IntegerLiteral(2)),
          BodyAttribute(col"c", IntegerLiteral(5))),
      Row(BodyAttribute(col"a", IntegerLiteral(2)),
          BodyAttribute(col"b", IntegerLiteral(3)),
          BodyAttribute(col"c", IntegerLiteral(3))),
      Row(BodyAttribute(col"a", IntegerLiteral(2)),
          BodyAttribute(col"b", IntegerLiteral(3)),
          BodyAttribute(col"c", IntegerLiteral(4))),
      Row(BodyAttribute(col"a", IntegerLiteral(2)),
          BodyAttribute(col"b", IntegerLiteral(3)),
          BodyAttribute(col"c", IntegerLiteral(5))),
      Row(BodyAttribute(col"a", IntegerLiteral(2)),
          BodyAttribute(col"b", IntegerLiteral(4)),
          BodyAttribute(col"c", IntegerLiteral(3))),
      Row(BodyAttribute(col"a", IntegerLiteral(2)),
          BodyAttribute(col"b", IntegerLiteral(4)),
          BodyAttribute(col"c", IntegerLiteral(4))),
      Row(BodyAttribute(col"a", IntegerLiteral(2)),
          BodyAttribute(col"b", IntegerLiteral(4)),
          BodyAttribute(col"c", IntegerLiteral(5))),
      Row(BodyAttribute(col"a", IntegerLiteral(3)),
          BodyAttribute(col"b", IntegerLiteral(2)),
          BodyAttribute(col"c", IntegerLiteral(3))),
      Row(BodyAttribute(col"a", IntegerLiteral(3)),
          BodyAttribute(col"b", IntegerLiteral(2)),
          BodyAttribute(col"c", IntegerLiteral(4))),
      Row(BodyAttribute(col"a", IntegerLiteral(3)),
          BodyAttribute(col"b", IntegerLiteral(2)),
          BodyAttribute(col"c", IntegerLiteral(5))),
      Row(BodyAttribute(col"a", IntegerLiteral(3)),
          BodyAttribute(col"b", IntegerLiteral(3)),
          BodyAttribute(col"c", IntegerLiteral(3))),
      Row(BodyAttribute(col"a", IntegerLiteral(3)),
          BodyAttribute(col"b", IntegerLiteral(3)),
          BodyAttribute(col"c", IntegerLiteral(4))),
      Row(BodyAttribute(col"a", IntegerLiteral(3)),
          BodyAttribute(col"b", IntegerLiteral(3)),
          BodyAttribute(col"c", IntegerLiteral(5))),
      Row(BodyAttribute(col"a", IntegerLiteral(3)),
          BodyAttribute(col"b", IntegerLiteral(4)),
          BodyAttribute(col"c", IntegerLiteral(3))),
      Row(BodyAttribute(col"a", IntegerLiteral(3)),
          BodyAttribute(col"b", IntegerLiteral(4)),
          BodyAttribute(col"c", IntegerLiteral(4))),
      Row(BodyAttribute(col"a", IntegerLiteral(3)),
          BodyAttribute(col"b", IntegerLiteral(4)),
          BodyAttribute(col"c", IntegerLiteral(5))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("INNER JOIN on 2 relations") {
    val join = Select(
      List(Var(col"a"), Var(col"b")),
      Some(rel"RelationA"),
      List(InnerJoin(rel"RelationB", Equals(Var(col"a"), Var(col"b")))),
      None,
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute(col"a", IntegerLiteral(2)), BodyAttribute(col"b", IntegerLiteral(2))),
      Row(BodyAttribute(col"a", IntegerLiteral(3)), BodyAttribute(col"b", IntegerLiteral(3))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("INNER JOIN on 3 relations") {
    val join = Select(
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
    val expected = List(
      Row(BodyAttribute(col"a", IntegerLiteral(3)),
          BodyAttribute(col"b", IntegerLiteral(3)),
          BodyAttribute(col"c", IntegerLiteral(3))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("LEFT OUTER JOIN on 2 relations") {
    val join = Select(
      List(Var(col"a"), Var(col"b")),
      Some(rel"RelationA"),
      List(LeftOuterJoin(rel"RelationB", Equals(Var(col"a"), Var(col"b")))),
      None,
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute(col"a", IntegerLiteral(1)), BodyAttribute(col"b", NULLLiteral)),
      Row(BodyAttribute(col"a", IntegerLiteral(2)), BodyAttribute(col"b", IntegerLiteral(2))),
      Row(BodyAttribute(col"a", IntegerLiteral(3)), BodyAttribute(col"b", IntegerLiteral(3))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("LEFT OUTER JOIN on 3 relations") {
    val join = Select(
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
    val expected = List(
      Row(BodyAttribute(col"a", IntegerLiteral(1)),
          BodyAttribute(col"b", NULLLiteral),
          BodyAttribute(col"c", NULLLiteral)),
      Row(BodyAttribute(col"a", IntegerLiteral(2)),
          BodyAttribute(col"b", IntegerLiteral(2)),
          BodyAttribute(col"c", NULLLiteral)),
      Row(BodyAttribute(col"a", IntegerLiteral(3)),
          BodyAttribute(col"b", IntegerLiteral(3)),
          BodyAttribute(col"c", IntegerLiteral(3))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("RIGHT OUTER JOIN on 2 relations") {
    val join = Select(
      List(Var(col"a"), Var(col"b")),
      Some(rel"RelationA"),
      List(RightOuterJoin(rel"RelationB", Equals(Var(col"a"), Var(col"b")))),
      None,
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute(col"a", IntegerLiteral(2)), BodyAttribute(col"b", IntegerLiteral(2))),
      Row(BodyAttribute(col"a", IntegerLiteral(3)), BodyAttribute(col"b", IntegerLiteral(3))),
      Row(BodyAttribute(col"a", NULLLiteral), BodyAttribute(col"b", IntegerLiteral(4))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("RIGHT OUTER JOIN on 3 relations") {
    val join = Select(
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
    val expected = List(
      Row(BodyAttribute(col"a", IntegerLiteral(3)),
          BodyAttribute(col"b", IntegerLiteral(3)),
          BodyAttribute(col"c", IntegerLiteral(3))),
      Row(BodyAttribute(col"a", NULLLiteral),
          BodyAttribute(col"b", NULLLiteral),
          BodyAttribute(col"c", IntegerLiteral(4))),
      Row(BodyAttribute(col"a", NULLLiteral),
          BodyAttribute(col"b", NULLLiteral),
          BodyAttribute(col"c", IntegerLiteral(5))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("FULL OUTER JOIN on 2 relations") {
    val join = Select(
      List(Var(col"a"), Var(col"b")),
      Some(rel"RelationA"),
      List(FullOuterJoin(rel"RelationB", Equals(Var(col"a"), Var(col"b")))),
      None,
      Nil,
      None,
      Nil
    )
    val expected = List(
      Row(BodyAttribute(col"a", IntegerLiteral(1)), BodyAttribute(col"b", NULLLiteral)),
      Row(BodyAttribute(col"a", IntegerLiteral(2)), BodyAttribute(col"b", IntegerLiteral(2))),
      Row(BodyAttribute(col"a", IntegerLiteral(3)), BodyAttribute(col"b", IntegerLiteral(3))),
      Row(BodyAttribute(col"a", NULLLiteral), BodyAttribute(col"b", IntegerLiteral(4))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }

  test("FULL OUTER JOIN on 3 relations") {
    val join = Select(
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
    val expected = List(
      Row(BodyAttribute(col"a", IntegerLiteral(1)),
          BodyAttribute(col"b", NULLLiteral),
          BodyAttribute(col"c", NULLLiteral)),
      Row(BodyAttribute(col"a", IntegerLiteral(2)),
          BodyAttribute(col"b", IntegerLiteral(2)),
          BodyAttribute(col"c", NULLLiteral)),
      Row(BodyAttribute(col"a", NULLLiteral),
          BodyAttribute(col"b", IntegerLiteral(4)),
          BodyAttribute(col"c", NULLLiteral)),
      Row(BodyAttribute(col"a", IntegerLiteral(3)),
          BodyAttribute(col"b", IntegerLiteral(3)),
          BodyAttribute(col"c", IntegerLiteral(3))),
      Row(BodyAttribute(col"a", NULLLiteral),
          BodyAttribute(col"b", NULLLiteral),
          BodyAttribute(col"c", IntegerLiteral(4))),
      Row(BodyAttribute(col"a", NULLLiteral),
          BodyAttribute(col"b", NULLLiteral),
          BodyAttribute(col"c", IntegerLiteral(5))),
    )
    val result = QueryBuilder.run(join, schemaABC)
    assert(result == Right(expected))
  }
}
