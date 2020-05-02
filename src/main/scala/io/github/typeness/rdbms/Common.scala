package io.github.typeness.rdbms

trait Common {
  implicit class StrStringContext(val sc: StringContext)  {
    def str(args: String*): String =
      sc.parts.zipAll(args, "", "").map {
        case (a, b) => StringContext.processEscapes(a) + b
      }.mkString("")
  }
  implicit class ListOps[T](list: List[T]) {
    def has(t: T): Boolean = list.contains(t)
  }
  implicit class EitherOps[T, S](either: Either[T, S]) {
    def has(b: S): Boolean = either.contains(b)
  }
  implicit class OptionOps[T](option: Option[T]) {
    def has(t: T): Boolean = option.contains(t)
  }
  implicit class MapOps[T, S](map: Map[T, S]) {
    def has(t: T): Boolean = map.contains(t)
  }
}
