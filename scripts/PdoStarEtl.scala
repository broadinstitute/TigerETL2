import analytics.tiger.ETL._
import analytics.tiger._
import org.joda.time.DateTime

val agentName = "analytics.tiger.PdoStar5Agent"

def pdoPushLeft(cond: => Boolean, size: Long = 365L)(delta: MillisDelta) = {
  if (cond) new MillisDelta(delta.start.minus(size*24*60*60*1000), delta.size + size*24*60*60*1000)
  else delta
}

val argsMap = utils.argsToMap(args)

val etlPlan = for (
  delta <- MillisDelta.loadFromDb(agentName) map pdoPushLeft(
    argsMap.get("1y-refresh").getOrElse("FALSE").toBoolean ||
    (DateTime.now.dayOfWeek().getAsText == "Friday" && DateTime.now.hourOfDay().get < 2)
  );
  plan <- prepareEtl(agentName, delta, sqlScript.etl(relativeFile("resources/pdo_star_etl.sql")))()
) yield plan
val res = utils.CognosDB.apply(etlPlan)
defaultErrorEmailer(agentName)(res)
println(res)
