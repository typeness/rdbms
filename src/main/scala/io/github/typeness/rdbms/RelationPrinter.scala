package io.github.typeness.rdbms

object RelationPrinter {
  def makeString(rows: List[Row]): String = rows match {
    case Nil =>
      ""
    case head :: _ =>
      val sizes = getColumnsSize(head.getNames, rows)
      val names = head.getNames.map(name => rightPad(name, sizes(name), ' '))
      val header = names.map(str => str.map(_ => '-')).mkString(" ")
      val body = rows.map(showRow(_, sizes)).mkString("\n")
      s"${names.mkString(" ")}\n$header\n$body"
  }

  private def rightPad(text: String, size: Int, pad: Char): String =
    text + pad.toString * (size - text.length)

  private def getColumnsSize(names: List[String], rows: List[Row]): Map[String, Int] =
    names
      .map(name =>
        name -> (name.length :: rows.flatMap(_.projectOption(name)).map(_.literal.show.length)).max)
      .toMap

  private def showRow(row: Row, sizes: Map[String, Int]): String = {
    row.attributes
      .map(attrib => rightPad(attrib.literal.show, sizes(attrib.name), ' '))
      .mkString(" ")
  }
}
