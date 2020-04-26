package io.github.typeness

package object rdbms {
  implicit class RelationStringContext(val sc: StringContext) extends AnyVal {
    def rel(args: Any*): RelationName = RelationName(sc.parts.mkString)
  }
}