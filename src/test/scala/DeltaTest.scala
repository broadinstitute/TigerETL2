//import org.scalacheck.Prop._
//import org.scalacheck.{Prop, Properties}

/**
 * Created by atanas on 12/4/2014.
 */

import analytics.tiger._
import org.scalatest._
import Matchers._
import org.scalatest.FlatSpec

class MySpec extends FlatSpec {

/*
  def _1ofN_failure_etl(n: Int)(di:daysDelta) = {
    val (d1, d2) = di.unpack
    val str = "[" + intervalDelta.daysFormatter.print(d1) + ", " + intervalDelta.daysFormatter.print(d2) + "]"
    if (di.start%n == 0) Left(etlError("Failed " + str))
    else Right("Processed " + str)
  }

  "DaysItem" should "span over adjacent intervals" in {

    DeltaFactory.getDaysDelta(Seq(
      new daysItem("1980-Jan-01", 2),
      new daysItem("1980-Jan-03", 3))
    ).toString should equal (
      DeltaFactory.getDaysDelta(Seq(new daysItem("1980-Jan-01", 5))).toString
    )

  }

  val delta = DeltaFactory.getDaysDelta[String](Seq(new daysItem("1980-Jan-01", 10)), 3)
  it should "processing " in {
    //DeltaFactory.aggregateResults(delta.apply(_1ofN_failure_etl(5)))
    //DeltaFactory.aggregateResults(delta.apply(_1ofN_failure_etl(Int.MaxValue)))
  }

  it should "merge overlapping intervals" in {
    DeltaFactory.getDaysDelta(Seq(
      new daysItem("1980-Jan-01", 10),
      new daysItem("1980-Jan-05", 20))
    ).toString should equal (
      DeltaFactory.getDaysDelta(Seq(new daysItem("1980-Jan-01", 24))).toString
    )

  }

  it should "preserve total size of non-overlapping intervals" in {
    DeltaFactory.getDays(Seq(
      new daysItem("1980-Jan-01", 10),
      new daysItem("1980-Feb-05", 20))
    ).foldLeft(0L)((acc, ii) => acc+ii.size) should equal (30)
  }
*/


  "An empty Set" should "have size 0" in {
    assert(Set.empty.size == 0)
    assertResult(2) { 5-3 }
  }

  it should "produce NoSuchElementException when head is invoked" in {
    intercept[NoSuchElementException] {
      Set.empty.head
    }
  }
}

/*

object StringSpecification extends Properties("String") {
  import Prop.forAll

  property("startsWith") = forAll { (a: String, b: String) =>
    (a+b).startsWith(a)
  }

  property("concatenate") = forAll { (a: String, b: String) =>
    (a+b).length > a.length && (a+b).length > b.length
  }

  property("substring") = forAll { (a: String, b: String, c: String) =>
    (a+b+c).substring(a.length, a.length+b.length) == b
  }

}
*/