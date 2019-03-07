package io.github.typeness.rdbms

sealed trait AnyType

case object IntegerType extends AnyType
case object RealType extends AnyType
case object DateType extends AnyType
case class NVarCharType(size: Int) extends AnyType {
  def canEqual(other: Any): Boolean = other.isInstanceOf[NVarCharType]
  override def equals(other: Any): Boolean = other match {
    case that: NVarCharType =>
      that canEqual this
    case _ => false
  }
}
case class CharType(size: Int) extends AnyType
case object NullType extends AnyType
case object MoneyType extends AnyType
case object BitType extends AnyType
case class DecimalType(precision: Int, scale: Int) extends AnyType
case object TinyIntType extends AnyType
case object ImageType extends AnyType
case object NTextType extends AnyType
