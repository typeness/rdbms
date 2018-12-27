package io.github.typeness.rdbms

sealed trait AnyType

case object IntegerType extends AnyType
case object DateType extends AnyType
case class NVarCharType(size: Int) extends AnyType
case class CharType(size: Int) extends AnyType
case object NullType extends AnyType
case object MoneyType extends AnyType
case object BitType extends AnyType
case class DecimalType(precision: Int, scale: Int) extends AnyType
case object TinyIntType extends AnyType
