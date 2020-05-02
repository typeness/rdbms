package io.github.typeness.rdbms

trait BuilderUtils {
  def checkUndefinedNames(referenced: List[AttributeName],
                          defined: List[AttributeName]): Either[ColumnDoesNotExists, Unit] =
    referenced.filterNot(defined.contains) match {
      case invalid :: _ => Left(ColumnDoesNotExists(invalid))
      case _            => Right(())
    }

  def checkNonUniqueNames(names: List[AttributeName]): Either[MultipleColumnNames, Unit] = {
    val nonUnique = names.diff(names.distinct)
    nonUnique match {
      case Nil            => Right(())
      case duplicate :: _ => Left(MultipleColumnNames(duplicate))
    }
  }
}
