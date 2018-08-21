import java.security.cert.X509Certificate
import javax.net.ssl._

import analytics.tiger.ETL._
import analytics.tiger._
import scalikejdbc._
import spray.json._

// Bypasses both client and server validation.
object TrustAll extends X509TrustManager {
  val getAcceptedIssuers = null

  def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String) = {}

  def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String) = {}
}

// Verifies all host names by simply returning true.
object VerifiesAllHostNames extends HostnameVerifier {
  def verify(s: String, sslSession: SSLSession) = true
}


case class OrspProject(
  key: String,
  label: String,
  projectType: String,
  status: String,
  description: Option[String],
  url: String
)
object OrspProjectJsonProtocol extends DefaultJsonProtocol {
  implicit val orspProjectFormat = jsonFormat(OrspProject, "key", "label", "type", "status", "description", "url")
  implicit val listOfOrspProjectsFormat = listFormat[OrspProject]
}
import OrspProjectJsonProtocol._

case class OrspProjectCollection(
  sampleCollection: String,
  project: String,
  projectUrl: String,
  consent: String,
  consentUrl: String
)
object OrspProjectCollectionJsonProtocol extends DefaultJsonProtocol {
  implicit val orspProjectCollectionFormat = jsonFormat(OrspProjectCollection, "sampleCollection", "project", "projectUrl", "consent", "consentUrl")
  implicit val listOfOrspProjectCollectionsFormat = listFormat[OrspProjectCollection]
}
import OrspProjectCollectionJsonProtocol._

val OrspProjectAgent: etlType[DummyDelta] = delta => session => {
  implicit val sess = session

  // SSL Context initialization and configuration
  val sslContext = SSLContext.getInstance("SSL")
  sslContext.init(null, Array(TrustAll), new java.security.SecureRandom())
  HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory)
  HttpsURLConnection.setDefaultHostnameVerifier(VerifiesAllHostNames)

  val projects: List[OrspProject] = scala.io.Source.fromURL(s"https://orsp.broadinstitute.org/api/projects").mkString.parseJson.convertTo[List[OrspProject]]
  val projectCollections: List[OrspProjectCollection] = scala.io.Source.fromURL(s"https://orsp.broadinstitute.org/api/samples").mkString.parseJson.convertTo[List[OrspProjectCollection]]

  val projectInsertSql = SQL(s"INSERT INTO orsp_project(project_key, label, project_type, status, description, url) VALUES(?, ?, ?, ?, ?, ?)")

  SQL("DELETE FROM orsp_project_consent").executeUpdate().apply()
  SQL("DELETE FROM orsp_consent").executeUpdate().apply()
  SQL("DELETE FROM orsp_project").executeUpdate().apply()

  projects.foreach(p => projectInsertSql.bind(p.key, p.label, p.projectType, p.status, p.description, p.url).executeUpdate().apply())

  projectCollections.foreach(c => {
      // Use MERGE INTO to insert a row only if it doesn't already exist
      sql"""MERGE INTO orsp_consent a
            USING (SELECT ${c.consent} AS consent_key,
                          ${c.consentUrl} AS consent_url
                   FROM dual) b
            ON (a.consent_key = b.consent_key)
            WHEN NOT MATCHED THEN
              INSERT (a.consent_key, a.consent_url)
              VALUES (b.consent_key, b.consent_url)""".update().apply()
      sql"""MERGE INTO orsp_project_consent a
            USING (SELECT ${c.project} AS project_key,
                          ${c.consent} AS consent_key,
                          ${c.sampleCollection} AS sample_collection
                   FROM dual) b
            ON (a.project_key = b.project_key
              AND a.consent_key = b.consent_key
              AND a.sample_collection = b.sample_collection)
            WHEN NOT MATCHED THEN
              INSERT (a.project_key, a.consent_key, a.sample_collection)
              VALUES (b.project_key, b.consent_key, b.sample_collection)""".update().apply()
  })

  Seq((delta, Right(s"Imported ${projects.size} ORSP Projects and ${projectCollections.size} consent/collection mappings")))
}

val agentName: String = "analytics.tiger.OrspProjectAgent"

// Run silently (for testing)
//val result = utils.CognosDB.apply(Reader((session: DBSession) => OrspProjectAgent(dummyDelta)(session)))
//val result = utils.CognosDB.apply((session: DBSession) => OrspProjectAgent(dummyDelta)(session))
val etlPlan = prepareEtl(agentName, dummyDelta, OrspProjectAgent)()
val result = utils.CognosDB.apply(etlPlan)
defaultErrorEmailer(agentName)(result)

println(result)
