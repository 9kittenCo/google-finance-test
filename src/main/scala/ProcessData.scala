import Helpers._
import fs2._

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.io.BufferedSource
import scala.util.Try

case class Row(date: String, close: Double) {
  override def toString = s"""Row("$date", $close)"""
}

class ProcessData {
  implicit lazy val strategy: Strategy = Strategy.fromExecutionContext(
    scala.concurrent.ExecutionContext.Implicits.global)

  def pricesURL(ticker: String, startdate: String, enddate: String): String = {
    s"http://www.google.com/finance/historical?q=NASDAQ:${ticker}&histperiod=daily&startdate=${startdate}&enddate=${enddate}&output=csv"
  }

  def getData(ticker: String, startdate: String, enddate: String): Result[String] = {
    Try {
      val source: BufferedSource =
        scala.io.Source.fromURL(pricesURL(ticker, startdate, enddate),"ISO-8859-1") // "ISO-8859-1"

      val s: Stream[Task, String] = Stream
        .unfold(source)((reader: BufferedSource) =>
          if (reader.hasNext) Some((reader.next(), reader)) else None)
        .fold("")(_ + _)
        .through(text.lines)
      s
    }.getOrElse(Stream.fail[Task](new Exception("Ticker not found")))
  }

  def query(ticker: String, startdate: String, enddate: String): Result[Row] = {

    getData(ticker, enddate, enddate)
      .drop(1) // skip headers
      .map(s => s.split(",").toList)
      .collect {
        case date :: _ :: _ :: _ :: close :: _ :: Nil =>
          Row(date, close.toDouble)
      }
  }

  def closePrices(q: Result[Row]): Result[Double] =
    q.map(_.close)

  def returns(q: Result[Row]): Result[Double] = {
    val data: Result[Double] = closePrices(q)
    data.zip(data.drop(1)).collect {
      case (today, yesterday) => (today - yesterday) / yesterday
    }
  }

  def meanClosePrice(q: Result[Row]): Result[Double] = {
    closePrices(q).zipWithIndex.fold(0d) {
      case (cma, (pn, prevIndex)) =>
        val n1 = prevIndex + 1
        (pn + prevIndex * cma) / n1
    }
  }

  def meanReturn(q: Result[Row]): Result[Double] = {
    returns(q).zipWithIndex.fold(0d) {
      case (cma, (pn, prevIndex)) =>
        val n1 = prevIndex + 1
        (pn + prevIndex * cma) / n1
    }
  }
}

object Main extends App {

  override def main(args: Array[String]): Unit = {
    val ticker = if (args.length == 1) args.head else "GOOG"

    val cal = Calendar.getInstance()
    val minuteFormat = new SimpleDateFormat("d-MMM-yy")
    val enddate = cal.getTime
    cal.add(Calendar.YEAR, -1)

    val startdate = cal.getTime
    val endAsString = minuteFormat.format(enddate)
    val startAsString = minuteFormat.format(startdate)

    val processData = new ProcessData
    val query = processData.query(ticker, startAsString, endAsString).get // to cache the fetched data
    val qStream = Stream.emits(query)

    println(">>> 1. close prices for " + ticker)
    //  1. Close price list for the past year
    println(processData.closePrices(qStream).get)
    //  2. Returns list for the past year
    println(">>> 2. returns for " + ticker)
    println(processData.returns(qStream).get)
    //  3. Mean price for the past year
    println(">>> 3. mean daily price for " + ticker)
    println(processData.meanClosePrice(qStream).get.head)
    // 4. Mean return for the past year
    println(">>> 4. mean daily return for " + ticker)
    println(processData.meanReturn(qStream).get.head)
  }
}
