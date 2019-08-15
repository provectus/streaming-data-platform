package basic
import scala.util.Random
import scala.concurrent.duration._
import com.typesafe.config.{Config, ConfigFactory}
import io.gatling.app.Gatling
import io.gatling.core.Predef._
import io.gatling.core.config.GatlingPropertiesBuilder
import io.gatling.http.Predef._
import java.util.concurrent.ThreadLocalRandom
import java.util.UUID.randomUUID

class ApiPerformanceTest extends Simulation {

  val rnd = ThreadLocalRandom.current
  val Appuid = Random.alphanumeric.take(10).mkString
  val Creative_category = Random.alphanumeric.take(10).mkString
  val Creative_id = Random.alphanumeric.take(10).mkString
  val Domain = Random.alphanumeric.take(10).mkString
  val Campaign_item_id = rnd.nextInt(1000000, 2000000)
  val duration = Integer.valueOf(System.getProperty("duration","60"))
  val httpProtocol = http.baseUrl(System.getProperty("baseUrl"))
    .acceptHeader("application/json, text/plain, */*")
    .acceptEncodingHeader("gzip, deflate")
    .acceptLanguageHeader("en-us")
    .userAgentHeader("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8")



  val scn = scenario("fast-data-solution")
    .exec(_.setAll(
      ("Appuid" -> Appuid),
      ("Campaign_item_id" -> Campaign_item_id),
      ("Creative_category"-> Creative_category),
      ("Creative_id"-> Creative_id),
      ("Domain"-> Domain),
      ("Txid", randomUUID().toString)
    ))
    .exec(
      http("Bid request")
        .post("/bid")
        .body(StringBody("""{"win_price": 0, "app_uid": "${Appuid}", "campaign_item_id": ${Campaign_item_id}, "creative_category": "${Creative_category}", "creative_id":"${Creative_id}", "tx_id":"${Txid}", "domain":"${Domain}"}""")).asJson
        .check(status.is(200))
    )
    .exec(
        http("Impression request")
          .post("/impression")
          .body(StringBody("""{"tx_id":"${Txid}", "win_price": 10 }""")).asJson
          .check(status.is(200))
    )
    .exec(
      http("Click request")
        .post("/click")
        .body(StringBody("""{"tx_id":"${Txid}"}""")).asJson
        .check(status.is(200))
    )
  setUp(scn.inject(rampUsersPerSec(1) to(10) during(duration)).protocols(httpProtocol))
}
