package io.github.typeness

package object rdbms extends Common {
  implicit class RelationStringContext(val sc: StringContext) extends AnyVal {
    def rel(args: String*): RelationName = {println(args); RelationName(sc.parts.mkString)}
  }
  implicit class ColumnStringContext(val sc: StringContext) extends AnyVal {
    def col(args: String*): AttributeName = {println(args); AttributeName(sc.parts.mkString)}
  }
}