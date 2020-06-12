package io.github.typeness.rdbms

import upickle.default._

case class HeadingAttribute(name: AttributeName, domain: AnyType, constraints: List[ColumnConstraint])

object HeadingAttribute {
  implicit val headingAttributeReadWriter: ReadWriter[HeadingAttribute] = macroRW
}

case class BodyAttribute(name: AttributeName, literal: Literal)

object BodyAttribute {
  implicit val bodyAttributeReadWriter: ReadWriter[BodyAttribute] = macroRW
}

case class AttributeName(value: String) extends AnyVal

object AttributeName {
  implicit val attributeNameReadWriter: ReadWriter[AttributeName] = macroRW
}