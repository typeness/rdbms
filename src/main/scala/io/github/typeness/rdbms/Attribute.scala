package io.github.typeness.rdbms

case class HeadingAttribute(name: AttributeName, domain: AnyType, constraints: List[ColumnConstraint])

case class BodyAttribute(name: AttributeName, literal: Literal)

case class AttributeName(value: String) extends AnyVal