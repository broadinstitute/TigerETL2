package analytics.tiger.api

import analytics.tiger.utils
import spray.routing.authentication.{UserPass, BasicAuth}
import spray.routing.directives.AuthMagnet
import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Failure, Success, Try}

// https://www.bcrypt-generator.com/
// http://www.tecnoguru.com/blog/2014/07/07/implementing-http-basic-authentication-with-spray/
// http://blog.knoldus.com/2015/01/14/spray-authenticate-directive-a-decoupled-way-to-authenticate-your-api/

case class AuthInfo(val user: String, val permissions: Set[String]) {
  def hasPermission(permission: String) = permissions.exists(_ == permission)
}

trait Authenticator {
  def basicUserAuthenticator(implicit ec: ExecutionContext): AuthMagnet[AuthInfo] = {

    def validateUser(userPass: Option[UserPass]): Option[AuthInfo] = {
      import dispatch._
      import collection.JavaConversions._

      val res = for (
        up <- Try(userPass.get);
        crowd <- Try {
          val req = url(utils.config.getString("crowdURL"))
          .addQueryParameter("username", up.user)
          .addHeader("Content-Type", "application/xml")
          .as_!(utils.config.getString("crowdAppName"), utils.config.getString("crowdAppPassword")) << (s"""<?xml version="1.0" encoding="UTF-8"?><password><value>${up.pass}</value></password>""")
          Http(req OK as.xml.Elem).apply()
        }
      ) yield new AuthInfo(up.user, utils.config.getStringList(s"users.${up.user}.permissions").toSet)
      res match {
        case Success(ai) => Some(ai)
        case Failure(f) => println(f) ; None
      }
    }

    BasicAuth((userPass: Option[UserPass]) => Future{ validateUser(userPass) }, realm = "Private API")
  }
}
