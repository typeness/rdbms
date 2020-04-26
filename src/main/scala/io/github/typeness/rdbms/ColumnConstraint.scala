package io.github.typeness.rdbms

sealed trait ColumnConstraint extends Product with Serializable {
  def name: Option[String]
  def show: String
}
case class AttributeIdentity(name: Option[String], current: Int, step: Int)
    extends ColumnConstraint {
  def toIdentity(attributeName: String): Identity = Identity(attributeName, current, step)

  override def show: String = s"IDENTITY($current,$step)"
}
case class Unique(name: Option[String] = None) extends ColumnConstraint {
  override def show: String = "UNIQUE"
}
case class NotNULL(name: Option[String] = None) extends ColumnConstraint {
  override def show: String = "NOT NULL"
}
case class NULL(name: Option[String] = None) extends ColumnConstraint {
  override def show: String = "NULL"
}
case class PrimaryKey(name: Option[String] = None) extends ColumnConstraint {
  override def show: String = "PRIMARY KEY"
}
case class Default(value: Literal, name: Option[String] = None) extends ColumnConstraint {
  override def show: String = s"DEFAULT(${value.show})"
}

case class Check(bool: Bool, name: Option[String] = None) extends ColumnConstraint {
  override def show: String = s"CHECK(${bool.show})"
}
case class ForeignKey(primaryKeyName: String,
                      pKeyRelationName: RelationName,
                      onUpdate: PrimaryKeyTrigger,
                      onDelete: PrimaryKeyTrigger,
                      name: Option[String] = None)
    extends ColumnConstraint {
  override def show: String =
    s"FOREIGN KEY REFERENCES $pKeyRelationName($primaryKeyName)"
}

sealed trait PrimaryKeyTrigger {
}
case object NoAction extends PrimaryKeyTrigger
case object Cascade extends PrimaryKeyTrigger
case object SetNULL extends PrimaryKeyTrigger
case object SetDefault extends PrimaryKeyTrigger

sealed trait RelationConstraint {
  def names: List[String]
  def constraintName: Option[String]
  def toColumnConstraint: ColumnConstraint
}
case class PKeyRelationConstraint(names: List[String], constraintName: Option[String] = None)
    extends RelationConstraint {
  override def toColumnConstraint: ColumnConstraint = PrimaryKey(constraintName)
}
case class FKeyRelationConstraint(names: List[String],
                                  pKeyRelationName: RelationName,
                                  pKeyColumnName: String,
                                  onDelete: PrimaryKeyTrigger,
                                  onUpdate: PrimaryKeyTrigger,
                                  constraintName: Option[String] = None)
    extends RelationConstraint {
  override def toColumnConstraint: ColumnConstraint =
    ForeignKey(pKeyColumnName, pKeyRelationName, onUpdate, onDelete, constraintName)
}
case class DefaultRelationConstraint(name: String, value: Literal, constraintName: Option[String])
    extends RelationConstraint {
  override def names: List[String] = List(name)
  override def toColumnConstraint: ColumnConstraint = Default(value, constraintName)
}

case class CheckRelationConstraint(condition: Bool, constraintName: Option[String])
  extends RelationConstraint {
  override def names: List[String] = Nil
  override def toColumnConstraint: ColumnConstraint = Check(condition, constraintName)
}
