import scala.sys.process._

/*
case class processes(process_name: String, columnNames: String) {
  val buffer = new StringBuffer()
  val cmd = "cmd /K wmic process where name=\"" + process_name + "\" get " + columnNames + "/format:csv"
  val res = (cmd lines_! ProcessLogger(buffer append _)).dropRight(1).filter(_ != "").map(_.split(","))
  val columns = res.head
  def columnGetter(name: String) = (items: Array[String]) => items(columns.indexOf(name))
  def data = res.tail.toList
}
*/
val buffer = new StringBuffer()
("cmd /K wmic process where name=\"java.exe\" get ProcessId,CommandLine /format:csv" lines_! ProcessLogger(buffer append _)).filter(_.indexOf("CompileServer")>=0).toList match {
  case str :: t => 
    val pid = str.substring(str.lastIndexOf(",")+1)
    (s"cmd /K wmic process $pid delete" lines_! ProcessLogger(buffer append _)).mkString
  case _ =>
    "CompileServer not found."
}
