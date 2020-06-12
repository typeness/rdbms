package io.github.typeness.rdbms

import upickle.default._

sealed trait AnyType {
  def show: String
}

object AnyType {
  implicit val anyTypeReadWriter: ReadWriter[AnyType] = ReadWriter.merge(
    macroRW[IntegerType.type],
    macroRW[RealType.type],
    macroRW[DateType.type],
    macroRW[NVarCharType],
    macroRW[CharType],
    macroRW[NullType.type],
    macroRW[MoneyType.type],
    macroRW[BitType.type],
    macroRW[DecimalType],
    macroRW[TinyIntType.type],
    macroRW[ImageType.type],
    macroRW[NTextType.type],
  )
}

case object IntegerType extends AnyType {
  override def show: String = "INT"
}
case object RealType extends AnyType {
  override def show: String = "REAL"
}
case object DateType extends AnyType {
  override def show: String = "DATE"
}
case class NVarCharType(size: Int) extends AnyType {
  def canEqual(other: Any): Boolean = other.isInstanceOf[NVarCharType]
  override def equals(other: Any): Boolean = other match {
    case that: NVarCharType =>
      that canEqual this
    case _ => false
  }

  override def show: String = str"NVARCHAR(${size.toString})"
}
case class CharType(size: Int) extends AnyType {
  override def show: String = str"CHAR(${size.toString})"
}
case object NullType extends AnyType {
  override def show: String = "NULL"
}
case object MoneyType extends AnyType {
  override def show: String = "MONEY"
}
case object BitType extends AnyType {
  override def show: String = "BIT"
}
case class DecimalType(precision: Int, scale: Int) extends AnyType {
  override def show: String = str"DECIMAL(${precision.toString}, ${scale.toString})"
}
case object TinyIntType extends AnyType {
  override def show: String = "TINYINT"
}
case object ImageType extends AnyType {
  override def show: String = "IMAGE"
}
case object NTextType extends AnyType {
  override def show: String = "NTEXT"
}
