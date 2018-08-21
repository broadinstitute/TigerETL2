import analytics.tiger.{ETL, utils}
import analytics.tiger.ETL._

utils.CognosDB.apply(prepareEtl("analytics.tiger.JiraTransitions", dummyDelta,
{ _ => session =>
  session.execute(
    s"""
       |BEGIN
       |  JIRADWH.etl.generate_transitions@LOJ_LINK.SEQBLDR(jiradwh.etl.labopsjira_schema@LOJ_LINK.SEQBLDR) ;
       |  JIRADWH.etl.generate_transitions@LOJ_LINK.SEQBLDR(jiradwh.etl.prodinfojira_schema@LOJ_LINK.SEQBLDR) ;
       |END;
     """.stripMargin)
  Seq((dummyDelta, Right("OK")))
}: ETL.etlType[DummyDelta])())
