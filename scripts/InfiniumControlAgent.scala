import java.nio.file.{Paths, Files}
import analytics.tiger.ETL._
import analytics.tiger._
import scalikejdbc._

case class Gtc(buff: Array[Byte]) {

  val controls = Array(
    ("DNP(High)", "Staining"),
    ("DNP(Bgnd)", "Staining"),
    ("Biotin(High)", "Staining"),
    ("Biotin(Bgnd)", "Staining"),
    ("Extension(A)", "Extension"),
    ("Extension(T)", "Extension"),
    ("Extension(C)", "Extension"),
    ("Extension(G)", "Extension"),
    ("TargetRemoval", "TargetRemoval"),
    ("Hyb(High)", "Hybridization"),
    ("Hyb(Medium)", "Hybridization"),
    ("Hyb(Low)", "Hybridization"),
    ("String(PM)", "Stringency"),
    ("String(MM)", "Stringency"),
    ("NSB(Bgnd)Red", "Non-SpecificBinding"),
    ("NSB(Bgnd)Purple", "Non-SpecificBinding"),
    ("NSB(Bgnd)Blue", "Non-SpecificBinding"),
    ("NSB(Bgnd)Green", "Non-SpecificBinding"),
    ("NP(A)", "Non-Polymorphic"),
    ("NP(T)", "Non-Polymorphic"),
    ("NP(C)", "Non-Polymorphic"),
    ("NP(G)", "Non-Polymorphic"),
    ("Restore", "Restoration")
  )

  def readChar    (offset: Int) = (buff(offset)+0x100)%0x100
  def readString  (offset: Int) = new String(buff.drop(offset+1).take(readChar(offset)))
  def readInteger4(offset: Int) = 0x01*readChar(offset) + 0x100*readChar(offset+1) + 0x10000*readChar(offset+2) + 0x1000000*readChar(offset+3)
  def readInteger2(offset: Int) = 0x01*readChar(offset) + 0x100*readChar(offset+1)

  lazy val nEntries        = readInteger4(4)
  lazy val gtcIndex        = 0.until(nEntries) map { n => readInteger2(8+6*n) -> readInteger4(10+6*n) } toMap
  lazy val numSnps         = gtcIndex(1)
  lazy val sampleName      = readString(gtcIndex(10))
  lazy val samplePlate     = readString(gtcIndex(11))
  lazy val sampleWell      = readString(gtcIndex(12))
  lazy val clusterFile     = readString(gtcIndex(100))
  lazy val manifest        = readString(gtcIndex(101))
  lazy val imagingDate     = readString(gtcIndex(200))
  lazy val autocallDate    = readString(gtcIndex(201))
  lazy val autocallVersion = readString(gtcIndex(300))
  lazy val xRawControlOffset = gtcIndex(500)
  lazy val yRawControlOffset = gtcIndex(501)
  lazy val xArrayLen       = readInteger4(xRawControlOffset)
  lazy val yArrayLen       = readInteger4(yRawControlOffset)
  lazy val xRawControl     = 0.until(xArrayLen) map { n => readInteger2(xRawControlOffset+4+2*n)}
  lazy val yRawControl     = 0.until(yArrayLen) map { n => readInteger2(yRawControlOffset+4+2*n)}

  lazy val xyRawControl = {
    val groupSize = xRawControl.size/controls.size
    0.until(controls.size) map { n => List(controls(n)._1, controls(n)._2, xRawControl(n*groupSize), yRawControl(n*groupSize)) }
  }

}

object Gtc {
  def apply(filename: String) = new Gtc(Files.readAllBytes(Paths.get(filename)))
  def fromChipBarcode(barcode: String) = apply(s"""\\\\neon-cifs\\humgen_illumina_data\\${barcode.split("_").head}\\$barcode.gtc""")
}

def timeToChipBarcodeConvertor(a: MillisDelta)(session: DBSession): DiscreteDelta[String] = {
  implicit val sess = session
  val (d1, d2) = a.unpack
  val res = sql"""SELECT chip_barcode FROM cognos.GAP_AUTOCALL_DATA WHERE first_seen_date>=?""".bind(d1).map(_.string(1)).list.apply()
  DiscreteDelta(res.toSet)
}

val InfiniumControlAgent: ETL.etlType[DiscreteDelta[String]] = delta => session => {
  val chip_barcode = delta.elements.head
  try {
    val gtc = Gtc.fromChipBarcode(chip_barcode)
    sql"DELETE FROM cognos.infinium_controls WHERE chip_barcode=?".bind(chip_barcode).update().apply()(session)
    val prep_stmt = SQL("INSERT INTO cognos.infinium_controls VALUES(?,?,?,?,?)")
    gtc.xyRawControl map { it => prep_stmt.bind(chip_barcode :: it: _*).update().apply()(session) }
    Seq((delta, Right("OK")))
  } catch {
    case e: java.nio.file.FileSystemException => Seq((delta, Right(etlMessage(e.getClass.getCanonicalName + ": " + e.getMessage))))
    case e: Exception => Seq((delta, Right(etlMessage(e.getClass.getCanonicalName + ": " + e.getMessage))))
  }
}

val agentName = "analytics.tiger.InfiniumControlAgent"
val etlPlan = for (
  delta <- MillisDelta.loadFromDb(agentName) ;
  plan <- prepareEtl(agentName, delta, InfiniumControlAgent)(chunkSize=1, convertor = timeToChipBarcodeConvertor)
) yield plan

val res = utils.CognosDB.apply(etlPlan)
defaultErrorEmailer(agentName)(res)
println(res)
