package io.github.typeness.rdbms

trait StrInterpolator {
  implicit class StrStringContext(val sc: StringContext)  {
    def str(args: String*): String =
      sc.parts.zipAll(args, "", "").map {
        case (a, b) => StringContext.processEscapes(a) + b
      }.mkString("")
  }
}
