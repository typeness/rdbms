package io.github.typeness.rdbms

sealed trait AnyType

case object IntegerType extends AnyType
case object DateType extends AnyType
case object StringType extends AnyType
case object NullType extends AnyType
case object MoneyType extends AnyType
case object BitType extends AnyType
