    import analytics.tiger.ETL._
    import analytics.tiger._
    import analytics.tiger.metrics._
    import analytics.tiger.metrics.metricUtils._
    import scalikejdbc._

    import scala.util.Try

    case class sampleAggItem(project: String, sample: String, dataType: String)


    val getFolderFunc = (groupId: Seq[Extractor], session: DBSession) => {
      implicit val sess = session
      val scala.util.Success(saggProject: String) = groupId.head.func(Seq())
      val scala.util.Success(saggSample: String) = groupId.tail.head.func(Seq())
      scala.reflect.io.File.apply(scala.tools.nsc.io.File(utils.config.getString("crsp_agg_folder")) / saggProject / "Exome" / saggSample).toDirectory
    }

    val mytasks = Seq(
      metricTask(
        "crsp_genotyping",
        Seq(
          ("VARIANT_TYPE", metricType.String),
          ("TRUTH_SAMPLE", metricType.String),
          ("CALL_SAMPLE", metricType.String),
          ("HET_SENSITIVITY", metricType.Double),
          ("HET_PPV", metricType.Double),
          ("HET_SPECIFICITY", metricType.Double),
          ("HOMVAR_SENSITIVITY", metricType.Double),
          ("HOMVAR_PPV", metricType.Double),
          ("HOMVAR_SPECIFICITY", metricType.Double),
          ("VAR_SENSITIVITY", metricType.Double),
          ("VAR_PPV", metricType.Double),
          ("VAR_SPECIFICITY", metricType.Double)
        ),
        6,
        "\t",
        "AGG_GENOTYPE_CONCORDANCE_TEST",
        """(.*).NA12878.NIST.usingPaddedIntervalsList""".r,
        getFolderFunc
      )
    )

    def crspGenotypingETL: ETL.etlType[DiscreteDelta[sampleAggItem]] = delta => session => {
      val sampleAgg = delta.unpack.head // make sure 'chunkSize=1' is specified when calling prepareETL
      val handlers = mytasks.map(taskToHandler(_)) // ++ Seq(Q30Handler, storedFunctionHandler("COGNOS.WALKUP_LANE"))
      val groupId = Seq(Extractor("PROJECT", _ => Try(sampleAgg.project)), Extractor("SAMPLE", _ => Try(sampleAgg.sample)), Extractor("DATA_TYPE", _ => Try(sampleAgg.dataType)))
      val res = handlers.map(_(groupId, session))
      Seq((delta, ETL.aggregateResults("", res)))
    }

    def millisToCrspSampleAggConvertor(a: MillisDelta)(session: DBSession): DiscreteDelta[sampleAggItem] = {
      implicit val sess = session
      val (d1, d2) = a.unpack
      val res = sql"SELECT a.project, a.SAMPLE, a.data_type FROM metrics.aggregation@CRSPREPORTING a WHERE  nvl(a.modified_at, a.workflow_end_date) >= ${d1} AND nvl(a.modified_at, a.workflow_end_date) < ${d2} AND a.LIBRARY IS NULL AND a.is_latest =1".map(it => sampleAggItem(it.string(1), it.string(2), it.string(3))).list.apply()
      DiscreteDelta(res.toSet)
    }

	

val agentName = "analytics.tiger.CrspGenotypingAgent.TEST"
val dbName = "CognosDatabaseProd"

val etlPlan = for (
  delta <- MillisDelta.loadFromDb(agentName);
  plan <- prepareEtl(agentName, delta, crspGenotypingETL)(chunkSize = 1, convertor = millisToCrspSampleAggConvertor)
  ) yield plan
val res = utils.CognosDB.apply(etlPlan)

// Manual discrete Delta
//val delta = new discreteDelta(Set(sampleAggItem("RP-805","SM-74NDL", "Exome")))
//val etlPlan = prepareEtl(agentName, delta, crspGenotypingETL)(chunkSize=1)
