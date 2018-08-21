package analytics.tiger.api

import javax.net.ssl.{HostnameVerifier, SSLSession}

import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import akka.io.IO
import akka.routing.RoundRobinPool
import analytics.tiger.utils
import spray.can.Http

object Boot extends App {

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("spray-api-service")
  val log = Logging(system, getClass)

  // create and start our service actor
  val service = system.actorOf(RoundRobinPool(utils.config.getInt("concurrentActors")).props(Props[TigerActor]), "router")

  // disable ssl host verification
  javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
    override def verify(hostname: String, sslSession: SSLSession): Boolean = true
  })

  // http://spray.io/documentation/1.2.4/spray-can/configuration/
  // start a new HTTP server on port 8080 with our service actor as the handler
  IO(Http) ! Http.Bind(service, interface = "0.0.0.0", port = utils.config.getInt("port"))

  // https://github.com/typesafehub/config#using-the-library
  scalikejdbc.config.DBs.setupAll()
}