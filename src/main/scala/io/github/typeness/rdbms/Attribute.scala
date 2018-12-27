package io.github.typeness.rdbms

sealed trait Attribute {
  def name: String
}
case class HeadingAttribute(name: String, domain: AnyType, constraints: List[Constraint])
    extends Attribute
case class BodyAttribute(name: String, literal: Literal) extends Attribute
