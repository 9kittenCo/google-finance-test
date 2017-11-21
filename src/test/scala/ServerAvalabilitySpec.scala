import org.scalatest.{FunSpecLike, MustMatchers, Tag}
import Helpers._
import java.text.SimpleDateFormat
import java.util.Calendar

class ServerAvalabilitySpec extends FunSpecLike with MustMatchers{
  it("should get data from google finance about GOOG") { pending
    val t = new ProcessData().getData("GOOG", "23-Nov-16", "20-Nov-17").get

    t.nonEmpty must be (true)
  }
}