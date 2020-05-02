package io.github.typeness.rdbms

sealed trait Attribute {
  def name: AttributeName
}
case class HeadingAttribute(name: AttributeName, domain: AnyType, constraints: List[ColumnConstraint])
    extends Attribute
case class BodyAttribute(name: AttributeName, literal: Literal) extends Attribute

case class AttributeName(value: String) extends AnyVal