package analytics.tiger.ext.mercury

import java.sql.{SQLIntegrityConstraintViolationException, Timestamp}
import analytics.tiger.ETL._
import analytics.tiger._
import analytics.tiger.utils.ConnProvider
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scalikejdbc._
import spray.json._

/**
 * Created by atanas on 3/26/2015.
 */
case class MolecularIndexingScheme(name: String, sequences: List[Map[String, String]])

class Library(
  val library: String,
  val project: Option[String],
  val initiative: Option[String],
  val analysisType: Option[String],
  val referenceSequence: Option[String],
  val species: Option[String],
  val gssrBarcodes: Option[List[String]],
  val lcSet: Option[String],
  val rootSample: Option[String],
  val productOrderKey: Option[String],
  val productOrderSample: Option[String],
  val labWorkflow: Option[String],
  val researchProjectId: Option[String],
  val researchProjectName: Option[String],
  val product: Option[String],
  val productFamily: Option[String],
  val sampleId: Option[String],
  val sampleType: Option[String],
  val collaboratorSampleId: Option[String],
  val collaboratorParticipantId: Option[String],
  val materialType: Option[String],
  val productPartNumber: Option[String],
  val molecularIndexingScheme: Option[MolecularIndexingScheme],
  val lsid: String,
  val workRequestId: Option[Int],
  val workRequestType: Option[String],
  val workRequestDomain: Option[String],
  val libraryCreationDate: String,
  val dataType: Option[String],
  val baitSetName: Option[String],
  val positiveControl: Option[Boolean],
  val negativeControl: Option[Boolean]
)

case class Lane(
  libraries: List[Library],
//  primer: Option[String],
  name: String,
  sequencedLibrary: String,
  sequencedLibraryCreationTime: String,
  loadingConcentration: Option[Double]
)

case class Run(
  error: Option[String],
  flowcellBarcode: Option[String],
  sequencer: Option[String],
  sequencerModel: Option[String],
  //reads: List[Map[String, String]],
  actualReadStructure: Option[String],
  setupReadStructure: Option[String],
  imagedAreaPerLaneMM2: Option[Double],
  name: Option[String],
  barcode: Option[String],
  //pairedRun: Boolean,
  runDateString: Option[String],
  lanes: Option[List[Lane]],
  systemOfRecord: Option[String]
)

object MyJsonProtocol extends DefaultJsonProtocol {

  // https://gist.github.com/agemooij/f2e4075a193d166355f8
  // https://github.com/spray/spray-json/blob/master/src/main/scala/spray/json/CollectionFormats.scala
  implicit object libraryFormat extends RootJsonFormat[Library] {

    def write(item: Library) = JsObject(
      Seq(
        "library" -> item.library.toJson,
        "project" -> item.project.toJson,
        "initiative" -> item.library.toJson
      ): _*
    )

    val convertors = Seq[Map[String, JsValue] => Object](
      _("library").convertTo[String],
      _("project").convertTo[Option[String]],
      _("initiative").convertTo[Option[String]],
      _("analysisType").convertTo[Option[String]],
      _("referenceSequence").convertTo[Option[String]],
      _("species").convertTo[Option[String]],
      _("gssrBarcodes").convertTo[Option[List[String]]],
      _("lcSet").convertTo[Option[String]],
      _("rootSample").convertTo[Option[String]],
      _("productOrderKey").convertTo[Option[String]],
      _("productOrderSample").convertTo[Option[String]],
      _("labWorkflow").convertTo[Option[String]],
      _("researchProjectId").convertTo[Option[String]],
      _("researchProjectName").convertTo[Option[String]],
      _("product").convertTo[Option[String]],
      _("productFamily").convertTo[Option[String]],
      _("sampleId").convertTo[Option[String]],
      _("sampleType").convertTo[Option[String]],
      _("collaboratorSampleId").convertTo[Option[String]],
      _("collaboratorParticipantId").convertTo[Option[String]],
      _("materialType").convertTo[Option[String]],
      _("productPartNumber").convertTo[Option[String]],
      _("molecularIndexingScheme").convertTo[Option[MolecularIndexingScheme]],
      _("lsid").convertTo[String],
      _("workRequestId").convertTo[Option[Int]].asInstanceOf[Object],
      _("workRequestType").convertTo[Option[String]],
      _("workRequestDomain").convertTo[Option[String]],
      _("libraryCreationDate").convertTo[String],
      _("dataType").convertTo[Option[String]],
      _("baitSetName").convertTo[Option[String]],
      _("positiveControl").convertTo[Option[Boolean]].asInstanceOf[Object],
      _("negativeControl").convertTo[Option[Boolean]].asInstanceOf[Object]
    ).zipWithIndex
    
    def read(json: JsValue) = {
      val jsObject = json.asJsObject
      val fields = jsObject.fields
      val constructor = classOf[Library].getConstructors()(0)

      try {
        val params = convertors.map(it => {
          try Right(it._1.apply(fields))
          catch { case e: Exception => Left(s"index: ${it._2}, ${e.getMessage}") }
      })
        if (params.exists(_.isLeft)) throw new RuntimeException(params.collect{case Left(s) => s}.mkString("\n"))
        constructor.newInstance(params.map{case Right(p) => p} : _*).asInstanceOf[Library]
      } catch {
        case t: Throwable => deserializationError("Cannot deserialize Library: invalid input. Raw input: " + t + "\n" + jsObject)
      }
    }

  }

  implicit val molecularIndexingSchemeFormat = jsonFormat2(MolecularIndexingScheme)
  implicit val listOfLibraries = listFormat[Library]
  implicit val laneFormat = jsonFormat5(Lane)
  implicit val listOfLanes = listFormat[Lane]
  implicit val runFormat = jsonFormat12(Run)
}

object BspSpecies {

  // The injection method (optional)
  def apply(parts: String*): String = parts.mkString(":")

  // The extraction method (mandatory)
  def unapplySeq(whole: String): Option[Seq[String]] = Some(whole.split("\\:"))

  def parse(s: String) = {
    val (ignoreGenus, sciName, strain) =
      try s match {
          case BspSpecies(a,b,c) => (a     ,b  ,c)
          case BspSpecies(a,b)   => (a     ,b  ,None)
          case BspSpecies(b)     => (None  ,b  ,None)
          case BspSpecies(a,b,c,_) => (a   ,b  ,c)
          case BspSpecies(a,b,c,_,_) => (a   ,b  ,c)
      } catch {
        case e:scala.MatchError => throw new RuntimeException(s"Can't parse BspSpecies:$s")
      }
    (sciName.trim, if (strain==None) "" else strain.toString.trim)
  }

}

object sequencingRun {

  case class seqRun(rowid: String, flowcell_barcode: String, run_name: String, source: String)

  type seqRunDelta = DiscreteDelta[seqRun]

  def loadDeltaCore(whereClause: String) = Reader((session: DBSession) => {
    implicit val sess = session
    val res = SQL(s"SELECT rowid, flowcell_barcode, run_name, source FROM COGNOS.slxre_rghqs_targets WHERE $whereClause").map(it => seqRun(it.string(1), it.string(2), it.string(3), it.string(4))).list.apply()
    new seqRunDelta(res.toSet)
  })

  val loadDeltaPending = loadDeltaCore("nvl(rgmd_timestamp,'1-jan-2000')<request_timestamp AND blacklist_timestamp IS NULL")

  def loadDeltaByFcBarcode(fc_barcodes: Seq[String]) = loadDeltaCore(s"flowcell_barcode IN (${fc_barcodes.map(it => s"'$it'").mkString(",")})")

  def PdoSamples_TO_FcBarcodes(pdo_samples: Seq[String]) = Reader((session: DBSession) => {
    implicit val sess = session
    SQL(s"SELECT DISTINCT a.flowcell_barcode FROM COGNOS.slxre_readgroup_metadata a WHERE a.product_order_sample IN (${pdo_samples.map(it => s"'$it'").mkString(",")})").map(_.string(1)).list.apply()
  })

  def loadDeltaByPdoSamples(pdo_samples: Seq[String]) = PdoSamples_TO_FcBarcodes(pdo_samples) flatMap (barcodes => Reader(session => loadDeltaByFcBarcode(barcodes)(session)))

  lazy val subs = utils.CognosDB.apply(
    Reader((session: DBSession) => {
    implicit val sess = session
    sql"""
      SELECT subs.product_part_number, p.part_number, p.product_name, p.product_family_name
      FROM COGNOS.PDO_STAR_PRODUCT_SUBSTITUTION subs
      JOIN mercurydw.product p ON p.part_number=subs.product_part_number_substitute"""
    .map(it => it.string(1) -> (Some(it.string(2)), Some(it.string(3)), Some(it.string(4)))).list().apply().toMap
  }))

  val RunMercuryAgent: ETL.etlType[seqRunDelta] = delta => session => {
    import analytics.tiger.ext.mercury.MyJsonProtocol._
    import spray.json.DefaultJsonProtocol._

    if (delta.elements.size >1) throw new RuntimeException("chunkSize=1 must be enabled when calling prepareETL.")
    val runItem = delta.unpack.head
    val run: Run = scala.io.Source.fromURL(s"https://mercury.broadinstitute.org:8443/Mercury/rest/IlluminaRun/query?runName=${runItem.run_name}").mkString.parseJson.convertTo[Run]
    implicit val sess = session

    val steps = List(
      ("pipeline API",
      () => {
        run.error match {
          case Some(err) =>
            val ind = err.indexOf("doesn't appear to have been registered yet")
            if (ind >= 0) throw new Exception(err.substring(ind))
            else throw new Exception(err)
          case None =>
            val fmt = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm")
            val fmt2 = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss")
            SQL(s"DELETE FROM COGNOS.SLXRE_READGROUP_METADATA WHERE run_name=?").bind(runItem.run_name).executeUpdate().apply()(session)
            val prep_stmt = SQL(s"INSERT INTO COGNOS.SLXRE_READGROUP_METADATA VALUES(${1.to(52).map(_ => "?").mkString(",")})")
            val res: Seq[Either[etlMessage, Any]] = for (lane <- run.lanes.getOrElse(Seq()); lib <- lane.libraries) yield {
              val (sciName, strain) =
                if (lib.species == None) ("", "")
                else analytics.tiger.ext.mercury.BspSpecies.parse(lib.species.get)
              val (part_number, product_name, product_family_name) = subs.getOrElse(lib.productPartNumber.getOrElse(""), (lib.productPartNumber, lib.product, lib.productFamily))
              val params = Array(
                // PK
                run.flowcellBarcode,
                lane.name,
                try lib.molecularIndexingScheme.get.name catch { case _ => "NULL" },
                //
                run.barcode,
                run.name,
                run.sequencer,
                run.sequencerModel,
                run.actualReadStructure,
                run.setupReadStructure,
                run.imagedAreaPerLaneMM2,
                new Timestamp(DateTime.parse(run.runDateString.getOrElse(""), fmt).getMillis),
                lib.library,
                lib.project,
                lib.initiative,
                lib.analysisType,
                lib.referenceSequence,
                lib.species,
                lib.lcSet,
                lane.sequencedLibrary,
                new Timestamp(DateTime.parse(lane.sequencedLibraryCreationTime, fmt2).getMillis),
                lib.productOrderKey,
                lib.productOrderSample,
                lib.labWorkflow,
                lib.researchProjectName,
                lib.researchProjectId,
                  product_name,
                  product_family_name,
                lib.sampleId,
                lib.sampleType,
                lib.collaboratorSampleId,
                lib.collaboratorParticipantId,
                lib.materialType,
                  part_number,
                lib.lsid,
                lib.workRequestId.getOrElse(null),
                lib.baitSetName,
                None, // is_greedy
                run.systemOfRecord,
                if (lib.positiveControl.getOrElse(false) || lib.negativeControl.getOrElse(false)) 1 else 0,
                None, // run_fraction
                sciName,
                strain,
                lib.workRequestType,
                lib.workRequestDomain,
                lib.gssrBarcodes.getOrElse(List()).mkString(","),
                new Timestamp(DateTime.parse(lib.libraryCreationDate, fmt).getMillis),
                lane.loadingConcentration,
                lib.dataType,
                None, // rgmd_id
                runItem.source,
                lib.rootSample,
                None  // is_pool_test
              )
              try {
                prep_stmt.bind(params: _*).executeUpdate().apply()(session)
                Right("ok")
              } catch {
                case e: SQLIntegrityConstraintViolationException => Right(etlMessage(s"PK violation detected: (lane:${params(1)},index:${params(2)}"))
              }
            }
            ETL.aggregateResults("", res.distinct)
        }
      }),

      ("compute IsGreedyFlag",
      () => SQL("""
          |BEGIN
          |    FOR rec IN (
          |        WITH
          |        core AS (SELECT * FROM COGNOS.slxre_readgroup_metadata a WHERE a.flowcell_barcode=?),
          |
          |        flowcell_lanes AS
          |        (SELECT
          |            -- PK
          |            flowcell_barcode,
          |            --
          |            COUNT(DISTINCT lane) lanes
          |        FROM core a
          |        GROUP BY flowcell_barcode
          |        ),
          |
          |        lane_barcodes AS
          |        (SELECT
          |            -- PK
          |            flowcell_barcode,
          |            lane,
          |            --
          |            COUNT(*) barcodes_total
          |        FROM core a
          |        GROUP BY flowcell_barcode,lane
          |        )
          |
          |        SELECT
          |            -- PK--
          |            core.flowcell_barcode,
          |            core.lane,
          |            core.molecular_indexing_scheme,
          |            --
          |            1/(flowcell_lanes.lanes*lane_barcodes.barcodes_total) run_fraction,
          |            decode(lane_barcodes.barcodes_total, 1, 1, 0) is_greedy
          |        FROM core
          |        LEFT JOIN COGNOS.flowcell_lanes ON flowcell_lanes.flowcell_barcode=core.flowcell_barcode
          |        LEFT JOIN COGNOS.lane_barcodes ON core.flowcell_barcode =lane_barcodes.flowcell_barcode AND core.lane=lane_barcodes.lane
          |    ) LOOP
          |        UPDATE COGNOS.slxre_readgroup_metadata a SET run_fraction=rec.run_fraction, is_greedy=rec.is_greedy --, timestamp=sysdate
          |        WHERE a.flowcell_barcode=rec.flowcell_barcode AND a.lane=rec.lane AND a.molecular_indexing_scheme=rec.molecular_indexing_scheme ;
          |    END LOOP ;
          |END ;
        """.stripMargin).bind(runItem.flowcell_barcode).execute().apply()(session)),

      ("correct LCSET from Squid",
      () => SQL("""
            |MERGE INTO COGNOS.slxre_readgroup_metadata a
            |USING (
            |SELECT fcl.flowcell_barcode, fcl.lane_name, fcl.fcl_comment_lcset, fcl.is_pool_test, fcl.comments
            |FROM COGNOS.SLXRE_FCLANE fcl
            |WHERE fcl.flowcell_barcode=?
            |) delta
            |ON (a.flowcell_barcode = delta.flowcell_barcode AND to_char(a.lane) = delta.lane_name)
            |
            |WHEN MATCHED THEN
            |UPDATE
            |SET
            |    a.lcset = CASE WHEN  REGEXP_INSTR(lower(delta.comments), 'top[ -_]?off')>0 THEN delta.fcl_comment_lcset ELSE a.lcset END,
            |    a.is_pool_test = delta.is_pool_test
            |WHERE
            |a.system_of_record='SQUID'
          """.stripMargin).bind(runItem.flowcell_barcode).execute().apply()(session)),

      ("set Pool-test flag from Mercury",
      () => SQL("""
          |MERGE INTO COGNOS.slxre_readgroup_metadata a
          |USING (
          |SELECT d.flowcell_barcode, d.lane,
          |    decode(d.is_pool_test , 'Y', 1, 0) is_pool_test
          |FROM mercurydw.flowcell_designation d
          |WHERE d.flowcell_barcode=?
          |) delta
          |ON (a.flowcell_barcode = delta.flowcell_barcode AND to_char(a.lane) = delta.lane)
          |
          |WHEN MATCHED THEN
          |UPDATE
          |SET
          |    a.is_pool_test = delta.is_pool_test
          |WHERE
          |a.system_of_record='MERCURY'
        """.stripMargin).bind(runItem.flowcell_barcode).execute().apply()(session)),

      ("Update OrganicRun: read_type, length, index_length",
      () => run.actualReadStructure.map(ars => SQL(
        """DECLARE
          |    v_type varchar2(100);
          |    v_length NUMBER ;
          |    v_index_length NUMBER ;
          |BEGIN
          |    solexa_2.read_info(?, ?, v_type, v_length, v_index_length) ;
          |    UPDATE COGNOS.slxre2_organic_run NOLOGGING
          |    SET read_type=v_type, read_length=v_length, index_length=v_index_length
          |    WHERE run_name=? ;
          |END ;
        """.stripMargin).bind(run.setupReadStructure, ars, runItem.run_name).execute().apply()(session))),

      ("stamp RGMD complete",
      () => SQL(s"UPDATE COGNOS.slxre_rghqs_targets SET rgmd_timestamp=sysdate WHERE rowid=?").bind(runItem.rowid).execute().apply()(session))

    ).map{ case (name, step) => () =>
      try {
        (step.apply() match {
          case e: Either[etlMessage, Any] => e
          case _ => Right("ok")
        }).asInstanceOf[Either[etlMessage, Any]]
      } catch { case e: Exception => Left(etlMessage(s"$name: ${e.getMessage}")) }
    }
    val res = ETL.aggregateResults("", ETL.executeSteps(steps))
    Seq((delta, Right(res match {
      case Left (e:etlMessage)  => e
      case Right(e:etlMessage)  => e
      case _ => "OK"
    })))
  }

}


