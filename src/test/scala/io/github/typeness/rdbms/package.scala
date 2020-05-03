package io.github.typeness

package object rdbms extends Common {
  implicit class RelationStringContext(val sc: StringContext) extends AnyVal {
    def rel(args: String*): RelationName = RelationName(sc.parts.mkString)
  }
  implicit class ColumnStringContext(val sc: StringContext) extends AnyVal {
    def col(args: String*): AttributeName = AttributeName(sc.parts.mkString)
  }
}