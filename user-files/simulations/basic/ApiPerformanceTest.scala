package basic
import scala.util.Random
import scala.concurrent.duration._
import com.typesafe.config.{Config, ConfigFactory}
import io.gatling.app.Gatling
import io.gatling.core.Predef._
import io.gatling.core.config.GatlingPropertiesBuilder
import io.gatling.http.Predef._

class ApiPerformanceTest extends Simulation {

  val Appuid = Random.alphanumeric.take(10).mkString
  val Creative_category = Random.alphanumeric.take(10).mkString
  val Creative_id = Random.alphanumeric.take(10).mkString
  val Domain = Random.alphanumeric.take(10).mkString
  val Campaign_item_id = Random.nextInt(3)
  val duration = Integer.valueOf(System.getProperty("duration","60"))
  val httpProtocol = http.baseUrl(System.getProperty("baseUrl"))
    .acceptHeader("application/json, text/plain, */*")
    .acceptEncodingHeader("gzip, deflate")
    .acceptLanguageHeader("en-us")
    .userAgentHeader("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8")


  val scn = scenario("fast-data-solution")
    .exec(_.set("Txid", Random.alphanumeric.take(10).mkString))
    .exec(
      http("Bid request")
        .post("/demostagev2/bid")
        .body(StringBody(s"""{"appuid": "${Appuid}", "campaign_item_id": ${Campaign_item_id}, "creative_category": "${Creative_category}", "creative_id":"${Creative_id}", "txid":"${Txid}", "domain":"${Domain}", "type":"bid"}""")).asJson
        .check(status.is(200))
    )
    .exec(
      http("Impression request")
        .post("/demostagev2/impression")
        .body(StringBody(s"""{"txid":"${Txid}", "type":"click"}""")).asJson
        .check(status.is(200))
    )
    .exec(
      http("Click request")
        .post("/demostagev2/click")
        .body(StringBody(s"""{"txid":"${Txid}", "win_price": 10, "type":"imp" }""")).asJson
        .check(status.is(200))
    )
  setUp(scn.inject(rampUsersPerSec(1) to(2) during(duration)).protocols(httpProtocol))
}