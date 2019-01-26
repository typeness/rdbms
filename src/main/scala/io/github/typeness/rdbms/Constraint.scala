package io.github.typeness.rdbms

sealed trait Constraint extends Product with Serializable {
  def name: Option[String]
}
case class Unique(name: Option[String] = None) extends Constraint
case class NotNULL(name: Option[String] = None) extends Constraint
case class NULL(name: Option[String] = None) extends Constraint
case class PrimaryKey(name: Option[String] = None) extends Constraint
case class Default(value: Literal, name: Option[String] = None) extends Constraint
case class Check(bool: Bool, name: Option[String] = None) extends Constraint
case class ForeignKey(primaryKeyName: String,
                      pKeyRelationName: String,
                      onUpdate: PrimaryKeyTrigger,
                      onDelete: PrimaryKeyTrigger,
                      name: Option[String] = None)
    extends Constraint

sealed trait PrimaryKeyTrigger
case object NoAction extends PrimaryKeyTrigger
case object Cascade extends PrimaryKeyTrigger
case object SetNULL extends PrimaryKeyTrigger
case object SetDefault extends PrimaryKeyTrigger

sealed trait RelationConstraint {
  def names: List[String]
  def name: Option[String]
}
case class PKeyRelationConstraint(names: List[String], name: Option[String] = None)
    extends RelationConstraint
case class FKeyRelationConstraint(names: List[String],
                                  pKeyRelationName: String,
                                  pKeyColumnName: String,
                                  onDelete: PrimaryKeyTrigger,
                                  onUpdate: PrimaryKeyTrigger,
                                  name: Option[String] = None)
    extends RelationConstraint
