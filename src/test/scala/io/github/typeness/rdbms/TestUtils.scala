package io.github.typeness.rdbms

import scala.util.Using

object TestUtils extends App {
  def readFromSQLFile(source: String): Either[SQLError, Schema] =
    Using(scala.io.Source.fromResource(source)) { source =>
      SchemaFormatSQL.deserialize(source.mkString)
    }.get
}
