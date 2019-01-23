package io.github.typeness.rdbms

sealed trait Constraint
case object Unique extends Constraint
case object NotNULL extends Constraint
case object NULL extends Constraint
case object PrimaryKey extends Constraint
case class Default(value: Literal) extends Constraint
case class Check(bool: Bool) extends Constraint
case class ForeignKey(primaryKeyName: String,
                      pKeyRelationName: String,
                      onUpdate: PrimaryKeyTrigger,
                      onDelete: PrimaryKeyTrigger)
    extends Constraint

sealed trait PrimaryKeyTrigger
case object NoAction extends PrimaryKeyTrigger
case object Cascade extends PrimaryKeyTrigger
case object SetNULL extends PrimaryKeyTrigger
case object SetDefault extends PrimaryKeyTrigger

sealed trait RelationConstraint {
  def names: List[String]
}
case class PKeyRelationConstraint(names: List[String]) extends RelationConstraint
case class FKeyRelationConstraint(names: List[String],
                                pKeyRelationName: String,
                                pKeyColumnName: String,
                                onDelete: PrimaryKeyTrigger,
                                onUpdate: PrimaryKeyTrigger)
    extends RelationConstraint
