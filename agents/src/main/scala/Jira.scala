package analytics.tiger.agents

/**
  * Created by atanas on 5/30/2017.
  */

import analytics.tiger.ETL._
import analytics.tiger.{ETL, utils}

object Jira {

  def main(args: Array[String]) {
    utils.CognosDB.apply(prepareEtl("analytics.tiger.agents.JiraTransitions", dummyDelta, { _ =>
      session =>
        session.execute(
          s"""
           |BEGIN
           |  JIRADWH.etl.generate_transitions@LOJ_LINK.SEQBLDR(jiradwh.etl.labopsjira_schema@LOJ_LINK.SEQBLDR) ;
           |  JIRADWH.etl.generate_transitions@LOJ_LINK.SEQBLDR(jiradwh.etl.prodinfojira_schema@LOJ_LINK.SEQBLDR) ;
           |END;
          """.stripMargin)
      Seq((dummyDelta, Right("OK")))
    }: ETL.etlType[DummyDelta])())
  }

}
