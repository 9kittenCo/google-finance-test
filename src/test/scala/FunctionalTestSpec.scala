import org.scalatest.{FunSpecLike, MustMatchers}

import Helpers._

class TestProcessData extends ProcessData {
  override def query(ticker: String, startdate: String, enddate: String) = {
    fs2.Stream(Row("20-Nov-17", 308.74), Row("17-Nov-17", 315.05), Row("16-Nov-17", 312.5), Row("15-Nov-17", 311.3), Row("14-Nov-17", 308.7), Row("13-Nov-17", 315.4), Row("10-Nov-17", 302.99), Row("9-Nov-17", 302.99), Row("8-Nov-17", 304.39), Row("7-Nov-17", 306.05), Row("6-Nov-17", 302.78), Row("3-Nov-17", 306.09), Row("2-Nov-17", 299.26), Row("1-Nov-17", 321.08), Row("31-Oct-17", 331.53), Row("30-Oct-17", 320.08), Row("27-Oct-17", 320.87), Row("26-Oct-17", 326.17), Row("25-Oct-17", 325.84), Row("24-Oct-17", 337.34), Row("23-Oct-17", 337.02), Row("20-Oct-17", 345.1), Row("19-Oct-17", 351.81), Row("18-Oct-17", 359.65), Row("17-Oct-17", 355.75), Row("16-Oct-17", 350.6), Row("13-Oct-17", 355.57), Row("12-Oct-17", 355.68), Row("11-Oct-17", 354.6), Row("10-Oct-17", 355.59), Row("9-Oct-17", 342.94), Row("6-Oct-17", 356.88), Row("5-Oct-17", 355.33), Row("4-Oct-17", 355.01), Row("3-Oct-17", 348.14), Row("2-Oct-17", 341.53), Row("29-Sep-17", 341.1), Row("28-Sep-17", 339.6), Row("27-Sep-17", 340.97), Row("26-Sep-17", 345.25), Row("25-Sep-17", 344.99), Row("22-Sep-17", 351.09), Row("21-Sep-17", 366.48), Row("20-Sep-17", 373.91), Row("19-Sep-17", 375.1), Row("18-Sep-17", 385.0), Row("15-Sep-17", 379.81), Row("14-Sep-17", 377.64), Row("13-Sep-17", 366.23), Row("12-Sep-17", 362.75), Row("11-Sep-17", 363.69), Row("8-Sep-17", 343.4), Row("7-Sep-17", 350.61), Row("6-Sep-17", 344.53), Row("5-Sep-17", 349.59), Row("1-Sep-17", 355.4), Row("31-Aug-17", 355.9), Row("30-Aug-17", 353.18), Row("29-Aug-17", 347.36), Row("28-Aug-17", 345.66), Row("25-Aug-17", 348.05), Row("24-Aug-17", 352.93), Row("23-Aug-17", 352.77), Row("22-Aug-17", 341.35), Row("21-Aug-17", 337.86), Row("18-Aug-17", 347.46), Row("17-Aug-17", 351.92), Row("16-Aug-17", 362.91), Row("15-Aug-17", 362.33), Row("14-Aug-17", 363.8), Row("11-Aug-17", 357.87), Row("10-Aug-17", 355.4), Row("9-Aug-17", 363.53), Row("8-Aug-17", 365.22), Row("7-Aug-17", 355.17), Row("4-Aug-17", 356.91), Row("3-Aug-17", 347.09), Row("2-Aug-17", 325.89), Row("1-Aug-17", 319.57), Row("31-Jul-17", 323.47), Row("28-Jul-17", 335.07), Row("27-Jul-17", 334.46), Row("26-Jul-17", 343.85), Row("25-Jul-17", 339.6), Row("24-Jul-17", 342.52), Row("21-Jul-17", 328.4), Row("20-Jul-17", 329.92), Row("19-Jul-17", 325.26), Row("18-Jul-17", 328.24), Row("17-Jul-17", 319.57), Row("14-Jul-17", 327.78), Row("13-Jul-17", 323.41), Row("12-Jul-17", 329.52), Row("11-Jul-17", 327.22), Row("10-Jul-17", 316.05), Row("7-Jul-17", 313.22), Row("6-Jul-17", 308.83), Row("5-Jul-17", 327.09), Row("3-Jul-17", 352.62), Row("30-Jun-17", 361.61), Row("29-Jun-17", 360.75), Row("28-Jun-17", 371.24), Row("27-Jun-17", 362.37), Row("26-Jun-17", 377.49), Row("23-Jun-17", 383.45), Row("22-Jun-17", 382.61), Row("21-Jun-17", 376.4), Row("20-Jun-17", 372.24), Row("19-Jun-17", 369.8), Row("16-Jun-17", 371.4), Row("15-Jun-17", 375.34), Row("14-Jun-17", 380.66), Row("13-Jun-17", 375.95), Row("12-Jun-17", 359.01), Row("9-Jun-17", 357.32), Row("8-Jun-17", 370.0), Row("7-Jun-17", 359.65), Row("6-Jun-17", 352.85), Row("5-Jun-17", 347.32), Row("2-Jun-17", 339.85), Row("1-Jun-17", 340.37), Row("31-May-17", 341.01), Row("30-May-17", 335.1), Row("26-May-17", 325.14), Row("25-May-17", 316.83), Row("24-May-17", 310.22), Row("23-May-17", 303.86), Row("22-May-17", 310.35), Row("19-May-17", 310.83), Row("18-May-17", 313.06), Row("17-May-17", 306.11), Row("16-May-17", 317.01), Row("15-May-17", 315.88), Row("12-May-17", 324.81), Row("11-May-17", 323.1), Row("10-May-17", 325.22), Row("9-May-17", 321.26), Row("8-May-17", 307.19), Row("5-May-17", 308.35), Row("4-May-17", 295.46), Row("3-May-17", 311.02), Row("2-May-17", 318.89), Row("1-May-17", 322.83), Row("28-Apr-17", 314.07), Row("27-Apr-17", 308.63), Row("26-Apr-17", 310.17), Row("25-Apr-17", 313.79), Row("24-Apr-17", 308.03), Row("21-Apr-17", 305.6), Row("20-Apr-17", 302.51), Row("19-Apr-17", 305.52), Row("18-Apr-17", 300.25), Row("17-Apr-17", 301.44), Row("13-Apr-17", 304.0), Row("12-Apr-17", 296.84), Row("11-Apr-17", 308.71), Row("10-Apr-17", 312.39), Row("7-Apr-17", 302.54), Row("6-Apr-17", 298.7), Row("5-Apr-17", 295.0), Row("4-Apr-17", 303.7), Row("3-Apr-17", 298.52), Row("31-Mar-17", 278.3), Row("30-Mar-17", 277.92), Row("29-Mar-17", 277.38), Row("28-Mar-17", 277.45), Row("27-Mar-17", 270.22), Row("24-Mar-17", 263.16), Row("23-Mar-17", 254.78), Row("22-Mar-17", 255.01), Row("21-Mar-17", 250.68), Row("20-Mar-17", 261.92), Row("17-Mar-17", 261.5), Row("16-Mar-17", 262.05), Row("15-Mar-17", 255.73), Row("14-Mar-17", 258.0), Row("13-Mar-17", 246.17), Row("10-Mar-17", 243.69), Row("9-Mar-17", 244.9), Row("8-Mar-17", 246.87), Row("7-Mar-17", 248.59), Row("6-Mar-17", 251.21), Row("3-Mar-17", 251.57), Row("2-Mar-17", 250.48), Row("1-Mar-17", 250.02), Row("28-Feb-17", 249.99), Row("27-Feb-17", 246.23), Row("24-Feb-17", 257.0), Row("23-Feb-17", 255.99), Row("22-Feb-17", 273.51), Row("21-Feb-17", 277.39), Row("17-Feb-17", 272.23), Row("16-Feb-17", 268.95), Row("15-Feb-17", 279.76), Row("14-Feb-17", 280.98), Row("13-Feb-17", 280.6), Row("10-Feb-17", 269.23), Row("9-Feb-17", 269.2), Row("8-Feb-17", 262.08), Row("7-Feb-17", 257.48), Row("6-Feb-17", 257.77), Row("3-Feb-17", 251.33), Row("2-Feb-17", 251.55), Row("1-Feb-17", 249.24), Row("31-Jan-17", 251.93), Row("30-Jan-17", 250.63), Row("27-Jan-17", 252.95), Row("26-Jan-17", 252.51), Row("25-Jan-17", 254.47), Row("24-Jan-17", 254.61), Row("23-Jan-17", 248.92), Row("20-Jan-17", 244.73), Row("19-Jan-17", 243.76), Row("18-Jan-17", 238.36), Row("17-Jan-17", 235.58), Row("13-Jan-17", 237.75), Row("12-Jan-17", 229.59), Row("11-Jan-17", 229.73), Row("10-Jan-17", 229.87), Row("9-Jan-17", 231.28), Row("6-Jan-17", 229.01), Row("5-Jan-17", 226.75), Row("4-Jan-17", 226.99), Row("3-Jan-17", 216.99), Row("30-Dec-16", 213.69), Row("29-Dec-16", 214.68), Row("28-Dec-16", 219.74), Row("27-Dec-16", 219.53), Row("23-Dec-16", 213.34), Row("22-Dec-16", 208.45), Row("21-Dec-16", 207.7), Row("20-Dec-16", 208.79), Row("19-Dec-16", 202.73), Row("16-Dec-16", 202.49), Row("15-Dec-16", 197.58), Row("14-Dec-16", 198.69), Row("13-Dec-16", 198.15), Row("12-Dec-16", 192.43), Row("9-Dec-16", 192.18), Row("8-Dec-16", 192.29), Row("7-Dec-16", 193.15), Row("6-Dec-16", 185.85), Row("5-Dec-16", 186.8), Row("2-Dec-16", 181.47), Row("1-Dec-16", 181.88), Row("30-Nov-16", 189.4), Row("29-Nov-16", 189.57), Row("28-Nov-16", 196.12), Row("25-Nov-16", 196.65), Row("23-Nov-16", 193.14))
  }

  val tickers = query("", "23-Nov-16", "20-Nov-17")
}

class TickerSpec extends FunSpecLike with MustMatchers {
  it("should parse csv") {
    new TestProcessData with TestData {
      private val csv = getData("", "23-Nov-16", "20-Nov-17").get

      csv.head must ===("Date,Open,High,Low,Close,Volume")
      csv(1) must ===("20-Nov-17, 313.79, 315.50, 304.75, 308.74, 8247650")
      csv(2) must ===("17-Nov-17, 325.67, 326.67, 313.15, 315.05, 13735139")
    }
  }

  it("should count daily prices") {
    new TestProcessData {
      val r = closePrices(tickers).get

      r.size must ===(250)
      r.head must ===(308.74)
      r.last must ===(193.14)
    }
  }

  it("should calculate returs") {
    new TestProcessData {
      val r = returns(tickers).get

      r.size must ===(249)
      r.head must ===(-0.020028566894143795)
      r.drop(1).head must ===(0.008160000000000037)
      r.last must ===(0.018173345759552758)
    }
  }

  it("should calculate mean close price correctly") {
    new TestProcessData {
      override def closePrices(q: Result[Row]) = fs2.Stream.emits(Vector(1, 2, 6))

      meanClosePrice(tickers).get.head must ===(3.0)
    }
  }

  it("should calculate mean close price") {
    new TestProcessData {
      meanClosePrice(tickers).get.head must ===(301.6653999999998)
    }
  }

  it("should calculate mean returns correctly") {
    new TestProcessData {
      override def returns(q: Result[Row]) = fs2.Stream.emits(Vector(1, 2, 6))

      meanReturn(tickers).get.head must ===(3.0)
    }
  }

  it("should calculate mean returns") {
    new TestProcessData {
      meanReturn(tickers).get.head must ===(0.0021349696884398325)
    }
  }

}