package analytics.tiger.api

import analytics.tiger.utils
import cats.data.Reader
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scalikejdbc.{DBSession, SQL}

import scala.xml.{Node, NodeSeq}
import dispatch._
import Defaults._
import com.ning.http.client.{Response}

import scala.util.matching.Regex

// http://stackoverflow.com/questions/14657705/match-value-with-function-based-on-type
// https://medium.com/@sinisalouc/overcoming-type-erasure-in-scala-8f2422070d20#.w0ww0her0
object jira {

  private val jiradtf = DateTimeFormat.forPattern("EEE, d MMM yyyy HH:mm:ss Z")

  def operationR(operation: String, defaultParam: String = ",") = new {
    def unapply(s: String) = s"""$operation(\\((.*)\\))?""".r.unapplySeq(s).flatMap {
      case Seq(_, null) => Some(defaultParam)
      case Seq(_, x) => Some(x)
      case _ => None
    }
  }

  object customField {
    def unapply(s: String) = """customfield_(\d*)""".r.unapplySeq(s).map(_.head)
  }

  object fieldName {
    def unapply(s: String) = """([^/(]*)(\((.*)\))?""".r.unapplySeq(s).map(l => l(0) :: l(2) :: Nil)
  }

  val toStringR       = operationR("toString", ",")
  val toListR         = operationR("toList", ",")
  val splitR         = operationR("split", ",")
  val concatR        = operationR("concat", ",")
  val regexTableR     = operationR("regexTable")
  val toBitsR         = operationR("toBits")
  val parseHtmlTableR = operationR("parseHtmlTable")
  val stripHtmlR = operationR("stripHtml")
  val select     = operationR("select")
  //val toIntR     = operationWithColumnNumbersR("toInt")
  //val toDoubleR  = operationWithColumnNumbersR("toDouble")
  //val toBooleanR = operationWithColumnNumbersR("toBoolean")

  object paramsR {
    def unapply(s: String) = Some(s.split(";").toList.filter(_ != ""))
  }

  object paramsIntR {
    def unapply(s: String) = paramsR.unapply(s).map{_.map(_.toInt-1)}
  }

  object regexParamsR {
    def unapply(s: String) = paramsR.unapply(s) match {
      case Some(Nil) => None
      case Some(re :: Nil) =>
        Some(re.r, 0.until(re.count(_ == '(') - """(\\\()""".r.findAllMatchIn(re).size).map(it => (it, it.toString)))
      case Some(re :: t) =>
        val cols = t.zipWithIndex
        Some(re.r, cols.filter(_._2 % 2 == 0).map(_._1.toInt) zip cols.filter(_._2 % 2 != 0).map(_._1))
    }
  }

  abstract class Extractor[+A] extends (Node => Option[A])

  case class stringExtractor(f: Node => Option[String]) extends Extractor[String] {
    def apply(n: Node) = f(n)
  }

  case class intExtractor(f: Node => Option[Int]) extends Extractor[Int] {
    def apply(n: Node) = f(n)
  }

  case class doubleExtractor(f: Node => Option[Double]) extends Extractor[Double] {
    def apply(n: Node) = f(n)
  }

  case class dateExtractor(f: Node => Option[DateTime]) extends Extractor[DateTime] {
    def apply(n: Node) = f(n)
  }

  case class booleanExtractor(f: Node => Option[Boolean]) extends Extractor[Boolean] {
    def apply(n: Node) = f(n)
  }

  case class cfNodeExtractor(f: Node => Option[Node]) extends Extractor[Node] {
    def apply(n: Node) = f(n)
  }

  //class listExtractor[B](f: Node => Option[List[B]]) extends Extractor[List[B]] { def apply(n: Node) = f(n) }
  class tableExtractor[+A](val f: Node => Option[List[A]], val columns: Seq[(String, Extractor[Any])]) extends Extractor[List[A]] {
    override def apply(n: Node) = f(n)
  }

  object dummy {
    object stringExtractor extends stringExtractor(n => ???)
    object intExtractor extends intExtractor(n => ???)
    object doubleExtractor extends doubleExtractor(n => ???)
    object booleanExtractor extends booleanExtractor(n => ???)
    object dateExtractor extends dateExtractor(n => ???)
  }

  case class listExtractor(native_f: Node => Option[List[String]]) extends tableExtractor(n => native_f(n).map(_.map(Seq(_))), Seq(("", dummy.stringExtractor)))
  def regularExtractor(fieldName: String) = stringExtractor(n => (n \ fieldName headOption) map (_.text.trim))
  def customExtractor(fieldName: String) = cfNodeExtractor(n => (n \ "customfields" \ "customfield").find(cf => (cf \ "@id" text) == fieldName))

  val stringToBoolean = (s: String) => s.toLowerCase match {
      case "true" | "yes" | "on" => Some(true)
      case "false" | "no" | "off" => Some(false)
      case _ => None
    }

  val toDouble = (ext: stringExtractor) => doubleExtractor(n => ext(n).map(_.toDouble))
  val toBoolean = (ext: stringExtractor) => booleanExtractor(n => ext(n).flatMap(it =>
    it.toLowerCase match {
      case "true" | "yes" | "on" => Some(true)
      case "false" | "no" | "off" => Some(false)
      case _ => None
    }
  ))
  val toDate = (ext: stringExtractor) => dateExtractor(n => ext(n).map(jiradtf.parseDateTime))

  val cfToList = (ext: cfNodeExtractor) => listExtractor(n => ext(n).map(_ \ "customfieldvalues" head).map(_.child.map(_.text.trim).toList.filter(_ != "")))
  def stringToList(dlm: String) = {
     dlm.r  // try the regex
    (ext: stringExtractor) => listExtractor(n => ext(n).map(_.split(dlm).toList.filter { _ != ""}))
  }
  def listToString(dlm: String) = (ext: listExtractor) => stringExtractor(n => ext(n).flatMap { it => (it.map(_.mkString)).mkString(dlm) match {
    case "" => None
    case it => Some(it)
  }})

  def tableToList(dlm: String) = (ext: tableExtractor[Seq[String]]) => listExtractor(n => ext(n).map(_.map(_.mkString(dlm))))

  def tableSelect(columns: Seq[Int]) = (ext: tableExtractor[Seq[Any]]) => {
    if (columns.exists(it => it <0 || it >= ext.columns.size)) throw new RuntimeException(s"Invalid Index for select.")
    new tableExtractor(
      n => ext(n).map(_.map(line => columns.map(col => line(col)))),
      columns.map(col => ext.columns(col))
    )
  }

  def tableToTable(columns: Seq[Int], f: Any => Any, dExtractor: Extractor[Any]) = (ext: tableExtractor[Seq[Any]]) => {
    if (columns.exists(it => it <0 || it >= ext.columns.size)) throw new RuntimeException(s"Invalid Index for select.")
    val columnsInd = ext.columns.zipWithIndex
    new tableExtractor(
      n => ext(n).map(_.map(line => columnsInd.map { case (column, ind) =>
        val value = line(ind)
        if (columns.exists(_ == ind)) f(value) else value
      })),
      columnsInd.map{ case (column, ind) => if (columns.exists(_ == ind)) (column._1, dExtractor) else column }
    )
  }

  def parseHtmlTable(columns: Seq[String]) = (ext: stringExtractor) => new tableExtractor(
    n => ext(n).map{ it => (utils.loadXML(it) \\ "table" \\ "tr").tail.map(line => (line \ "td").map(_.text)).toList},
    columns.map((_, dummy.stringExtractor))
  )

  def regexTable(expr: Regex, columns: Seq[(Int, String)]) = (ext: stringExtractor) => new tableExtractor(
    n => ext(n).map{ data =>
    expr.findAllMatchIn(data).map{m => columns.map{col => Option(m.group(col._1)).getOrElse("") }}.toList},
    columns.map(it => (it._2, dummy.stringExtractor))
  )

  val stringToInt = (ext: stringExtractor) => intExtractor(n => ext(n).map(_.toInt))
  val booleanToInt = (ext: booleanExtractor) => intExtractor(n => ext(n).map(if (_) 1 else 0))
  val stringStripHtml = (ext: stringExtractor) => stringExtractor(n => ext(n).map(utils.stripHtml))
  val listStripHtml = (ext: listExtractor) => listExtractor(n => ext(n).map(_.map(it => utils.stripHtml(it.head))))
  val doubleToInt = (ext: doubleExtractor) => intExtractor(n => ext(n).map(_.toInt))
  val stringExpose = (ext: stringExtractor) => stringExtractor(n => ext(n).map(utils.ascEncode))

  // specialized extractors
  def timestampExtractor(d: DateTime) = dateExtractor(_ => Option(d))

  def flatListExtractor(levels: String*) = listExtractor(n => Option((levels.foldLeft(n.asInstanceOf[NodeSeq]){ case (acc, it) => acc \ it }).map(_.text).toList))
  val labelsExtractor   = flatListExtractor("labels", "label")
  val subtasksExtractor = flatListExtractor("subtasks", "subtask")
  val componentsExtractor = flatListExtractor("component")

  val linksExtractor = new tableExtractor(n => Option {
      val key = (n \ "key").text
      for (
        issuelinktype <- (n \ "issuelinks" \ "issuelinktype") toList;
        issuelinktypeName <- List(issuelinktype \ "name" text);
        direction <- List("inwardlinks", "outwardlinks");
        directedLink <- (issuelinktype \ direction) toList;
        description <- List(directedLink \ "@description" text);
        issuelink <- (directedLink \ "issuelink") toList;
        link <- List(issuelink.text.trim)
      ) yield Seq(direction match {
        case "outwardlinks" => key + " " + description + " " + link
        case "inwardlinks"  => link + " " + description + " " + key
      }, issuelinktypeName, direction, description, link)
    },
    Seq(("narrative", dummy.stringExtractor), ("type", dummy.stringExtractor), ("direction", dummy.stringExtractor), ("description", dummy.stringExtractor), ("key", dummy.stringExtractor))
  )

  val commentsExtractor =  new tableExtractor(n => Option {
    (n \ "comments" \ "comment").map(c => Seq(
      jiradtf.parseDateTime(c \ "@created" text),
      c \ "@author" text,
      (c text).trim,
      (c \ "@id" text)
    )) toList
    },
    Seq(("created", dummy.dateExtractor), ("author", dummy.stringExtractor), ("text", dummy.stringExtractor), ("id", dummy.stringExtractor))
  )

  case class metadata(fieldName: String, friendlyFieldName: String, extractor: Extractor[Any])

  def generateRawMetadata(fields: Seq[String]): Seq[metadata] = {

    abstract class placeHolderExtractor extends Extractor { override def apply(n: Node) = ??? }
    case class bitsExtractor(columns: Seq[String], le: listExtractor) extends placeHolderExtractor
    case class intBitsExtractor(be: bitsExtractor) extends placeHolderExtractor

    fields.map { it =>
      val fieldName(List(name, suggestedName)) :: ops = it.split(":").toList
      val friendlyFieldName = Option(suggestedName).getOrElse(name)
      val initialExtractor = name match {
        case "timestamp" => timestampExtractor(DateTime.now)
        case "labels" => labelsExtractor
        case "issuelinks" => linksExtractor
        case "subtasks" => subtasksExtractor
        case "components" => componentsExtractor
        case "comments" => commentsExtractor
        case customField(id) => cfToList(customExtractor(name))
        case _ => regularExtractor(name)
      }

      val aggext = ops.foldLeft(initialExtractor.asInstanceOf[Extractor[Any]]) {
        case (acc: stringExtractor, "toInt") => stringToInt(acc)
        case (acc: booleanExtractor, "toInt") => booleanToInt(acc)
        case (acc: stringExtractor, "toDouble") => toDouble(acc)
        case (acc: stringExtractor, "toDate") => toDate(acc)
        case (acc: stringExtractor, "toBoolean") => toBoolean(acc)
        case (acc: stringExtractor, "expose") => stringExpose(acc)
        case (acc: stringExtractor, "stripHtml") => stringStripHtml(acc)
        case (acc: stringExtractor, parseHtmlTableR(paramsR(l))) if (!l.isEmpty) => parseHtmlTable(l)(acc)
        case (acc: stringExtractor, regexTableR(regexParamsR(expr, columns))) if (!columns.isEmpty) => regexTable(expr, columns)(acc)
        case (acc: listExtractor  , "stripHtml") => listStripHtml(acc)

        case (acc: stringExtractor,  splitR(dlm)) => stringToList(dlm)(acc)

        case (acc: tableExtractor[Seq[String]], toListR(dlm)) => tableToList(dlm)(acc)
        case (acc: tableExtractor[Seq[String]], select(paramsIntR(columns))) if (!columns.isEmpty)  => tableSelect(columns)(acc)

        case (acc: listExtractor,   concatR(dlm)) => listToString(dlm)(acc)

        case (acc: doubleExtractor, "toInt") => doubleToInt(acc)
        case (acc: listExtractor, "toDouble") => (listToString("") andThen toDouble) (acc)
        case (acc: listExtractor, "toDate") => (listToString("") andThen toDate) (acc)

        case (acc: listExtractor, toBitsR(paramsR(l))) if (!l.isEmpty) => bitsExtractor(l, acc)
        case (acc: bitsExtractor, "toInt") => intBitsExtractor(acc)

        case (acc: tableExtractor[Seq[Any]], stripHtmlR(paramsIntR(columns))) => tableToTable(columns, it => utils.stripHtml(it.asInstanceOf[String]), dummy.stringExtractor)(acc)

        case (acc, tran) => throw new RuntimeException(s"Invalid transformation(${acc.getClass.getName}) $friendlyFieldName:$tran")
      }
      metadata(name, friendlyFieldName, aggext)

    }.flatMap{
      case metadata(name, friendlyFieldName, bitsExtractor(columns, le)) =>
        columns.map(col => metadata(
          name,
          s"$friendlyFieldName[$col]",
          booleanExtractor(n => le.native_f.apply(n).map(_.exists(_ == col)))
        ))
      case metadata(name, friendlyFieldName, intBitsExtractor(be)) =>
        be.columns.map(col => metadata(
          name,
          s"$friendlyFieldName[$col]",
          intExtractor(n => be.le.native_f.apply(n).map(it => if (it.exists(_ == col)) 1 else 0))
        ))
      case m: metadata => Seq(m)
    }
  }

  def getStreamer(mtd: Seq[metadata]): (List[Node] => Stream[Seq[Option[Any]]], Seq[metadata], Option[String]) = {

    val (nodeExtractor, flatMetadata, exploderField) = mtd.filter(_.extractor.isInstanceOf[tableExtractor[Seq[Any]]]).toList match {
      case exploder :: Nil =>
        val left = mtd.takeWhile(_ != exploder)
        val right = mtd.dropWhile(_ != exploder).tail
        val mle = exploder.extractor.asInstanceOf[tableExtractor[Seq[Any]]]
        val empty = 0.until(mle.columns.size).map(_ => None: Option[String])

        ((node: Node) => {
          val l = left.map(_.extractor(node))
          val r = right.map(_.extractor(node))
          mle.apply(node) match {
            case Some(list) if (!list.isEmpty) => list map { expl => l ++ 0.until(mle.columns.size).map(ind => Option(expl(ind))) ++ r } toStream
            case Some(list) if (list.isEmpty) => Stream(l ++ empty ++ r)
            case None => Stream(l ++ empty ++ r)
          }
        },
        left ++
          mle.columns.map{ col => metadata(
            exploder.fieldName,
            s"${exploder.friendlyFieldName}${if (mle.columns.size == 1) "" else "[" + col._1 + "]"}" ,
            col._2
          )}
          ++ right,
        Some(exploder.friendlyFieldName)
        )

      case Nil =>
        val extractors = mtd map (_.extractor)
        ((node: Node) => Stream(extractors map (_(node))), mtd, None)

      case exploder :: more => throw new RuntimeException("Cartesian product detected. No more than 1 table/list is allowed : " + (exploder :: more).map(_.friendlyFieldName).mkString(", "))
    }

    def getStreamCore(items0: List[Node]): Stream[Seq[Option[Any]]] = items0 match {
      case Nil  => Stream.empty
      case head :: tail => nodeExtractor(head) #::: getStreamCore(tail)
    }

    ((items: List[Node]) => getStreamCore(items), flatMetadata, exploderField)
  }


  case class request(domain: String, jqlQuery: String, fields: String, username: String, password: String)
  case class response(fields: Seq[jira.metadata], stream: Stream[Seq[Option[Any]]])

  class superExtractor(rq: request, tempMax: Int = 1000) {
    val request = rq
    private val mtd = generateRawMetadata(rq.fields.split(",").filterNot(_==""))
    private val conf = utils.config.getConfig(s"jira.${rq.domain}")
    lazy val (streamer, metadata, exploderField) = getStreamer(mtd)
    lazy val req = dispatch.url(conf.getString("url")).as_!(rq.username, rq.password) <<? ("tempMax" -> tempMax.toString) :: ("jqlQuery" -> rq.jqlQuery) :: mtd.map(_.fieldName).distinct.map("field" -> _).toList
  }

  // https://confluence.atlassian.com/jira064/displaying-search-results-in-xml-720416695.html
  // http://stackoverflow.com/questions/14941462/how-do-i-get-the-value-of-a-failed-request-in-dispatch

  def apiCall(req: Req) = {
    implicit class MyRequestHandlerTupleBuilder(req: Req) {
      def OKWithBody[T](f: Response => T) = (req.toRequest, new utils.OkWithBodyHandler(f, utils.loadXML(_) \ "body" \\ "u" text))
    }
    Http(req OKWithBody as.String).either()
  }

  val process = Reader((rq: request) => {
    val sExecutor = new superExtractor(rq)
    println(sExecutor.req.url)

    val doc = apiCall(sExecutor.req) match {
      case Right(s) => utils.loadXML(s)
      case Left(e) => throw e
    }
    val items = doc \ "channel" \ "item" toList
    val total = (doc \ "channel" \ "issue" \ "@total" text) toInt

    val totalStream = 1.to((total-1)/1000).foldLeft(sExecutor.streamer(items)){ (acc, start) => {
      val doc0 = (Http(sExecutor.req.addQueryParameter("pager/start", start*1000 toString) OK as.xml.Elem)).apply()
      val items0 = (doc0 \ "channel" \ "item").toList
      acc #::: sExecutor.streamer(items0)
    }}
    response(sExecutor.metadata, totalStream)
  })


  //import analytics.tiger._
  //val etlPlan = jira.process.map(jira.toETL("mytable")).apply(jira.request("labopsjira", "filter=13334", "status,reporter,created:toDate,customfield_10010","analytics", "analytics"))
  //utils.CognosDB.apply(etlPlan)

  def toETL(tableName: String) = (response: jira.response) => Reader(
    (session: DBSession) => {
      implicit val sess = session
      val stmt = SQL(s"INSERT INTO $tableName VALUES(${response.fields.map{ _ => "?"}.mkString(",")})")
      response.stream.foreach{ stmt.bind(_: _*).executeUpdate().apply() }
    })

  //import analytics.tiger._
  //val res = jira.process.map(jira.toDDL)(jira.request("labopsjira","filter=13334","key,labels,links:toList( ):toString(),customfield_13968:toDouble,status,reporter,created:toDate,customfield_10010:toList(<br/>)", "analytics","analytics"))
  val toDDL = (rsp: response) => rsp.fields.zipWithIndex.map{ case (metadata(_,fieldName, extractor), index) =>
    "\"" + fieldName + "\" " + (extractor match {
      case _: jira.stringExtractor =>
        val size = rsp.stream.foldLeft(0)( (acc, line) => line(index).asInstanceOf[Option[String]] match {
          case Some(str) => scala.math.max(acc, str.size)
          case None => acc
        })
        s"VARCHAR2($size BYTE)"
      case _: jira.intExtractor    => "NUMBER(10,0)"
      case _: jira.doubleExtractor => "NUMBER(15,5)"
      case _: jira.booleanExtractor=> "NUMBER(1,0)"
      case _: jira.dateExtractor   => "TIMESTAMP"
    })
  }.mkString("CREATE TABLE ??? (\n",",\n",")")

  implicit def toReader(r: cats.data.Reader[DBSession,_]) = Reader((s:DBSession) => r.apply(s))

  case class FieldMetadata(name: String, id: String, `type`: Option[String], custom: Option[String])
  def getFieldsMetadata(domain: String) = {
    val conf = utils.config.getConfig(s"jira.$domain")
    val req = dispatch.url(conf.getString("fieldsUrl")).as_!(conf.getString("username"), conf.getString("password"))
    val doc = (Http(req OK as.String)).apply()
    import spray.json._
    import DefaultJsonProtocol._
    case class Schema(`type`: String, custom: Option[String])
    case class Field(id: String, name: String, schema: Option[Schema])
    implicit val schemaFormat = jsonFormat(Schema, "type", "custom")
    implicit val fieldFormat = jsonFormat(Field, "id", "name", "schema")
    val fields = doc.parseJson.convertTo[List[Field]]
    fields.map(it => FieldMetadata(it.name, it.id, it.schema.map(_.`type`), it.schema.flatMap(_.custom)))
  }


  case class issueApi(domain: String, key: String, items: List[(String, String, String)] = Nil) {
    val conf = utils.config.getConfig(s"jira.$domain")
    val req = dispatch.url(s"""${conf.getString("issueUrl")}/$key""").as_!(conf.getString("username"), conf.getString("password"))

    def set   (field: String, value: String) = issueApi(domain, key, ("set"   , field, value) :: items)
    def add   (field: String, value: String) = issueApi(domain, key, ("add"   , field, value) :: items)
    def remove(field: String, value: String) = issueApi(domain, key, ("remove", field, value) :: items)

    def update(): Unit = {
      import dispatch._
      val json =  items.groupBy(_._2).map{ case (field, verbs) =>
        verbs.map{ case(verb, _, value) =>
          verb match {
            case "set" => s"""{ "set": "$value" }"""
            case "add" => s"""{ "add": "$value" }"""
            case "remove" => s"""{ "remove": { "id": "$value" }"""
      }}.mkString(s""""$field": [""", "," , "]")}.mkString(s"""{ "update": {""", "," , "}}")
      val req2 = req.setContentType("application/json", "UTF-8") << json
      val res = Http(req2.PUT).apply().getStatusCode
      if (res != 204) throw new RuntimeException(s"Jira Status Code: $res")
    }

    import spray.json._
    def get = Http(req.GET OK as.String).apply().parseJson

  }


}