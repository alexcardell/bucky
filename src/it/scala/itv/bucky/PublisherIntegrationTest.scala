package itv.bucky

import com.rabbitmq.client.MessageProperties
import itv.bucky.TestUtils._
import itv.contentdelivery.testutilities.json.JsonResult
import itv.httpyroraptor._
import itv.utils.Blob
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class PublisherIntegrationTest extends FunSuite with ScalaFutures {

  val testQueueName = "bucky-publisher-test"
  val routingKey = RoutingKey(testQueueName)
  val exchange = Exchange("")
  lazy val (testQueue, amqpClientConfig, rmqAdminHhttp) = IntegrationUtils.setUp(testQueueName)

  ignore("Can publish messages to a (pre-existing) queue") {
    testQueue.head.purge()

    for {
      amqpClient <- amqpClientConfig
      publish <- amqpClient.rawPublisher()
    } {
      val body = Blob.from("Hello World!")
      publish(PublishCommand(Exchange(""), routingKey, MessageProperties.MINIMAL_PERSISTENT_BASIC, body)).asTry.futureValue shouldBe 'success

      testQueue.head.getNextMessage().payload shouldBe body
    }

  }

  ignore("Publisher can recover from connection failure") {
    testQueue.head.purge()

    for {
      amqpClient <- amqpClientConfig.copy(networkRecoveryInterval = Some(500.millis))
      publish <- amqpClient.rawPublisher()
    } {
      // Publish before failure
      publish(PublishCommand(exchange, routingKey, MessageProperties.MINIMAL_PERSISTENT_BASIC, Blob.from("Before"))).asTry.futureValue shouldBe 'success

      killRabbitConnection()

      // Publish fails until connection is re-established
      publish(PublishCommand(exchange, routingKey, MessageProperties.MINIMAL_PERSISTENT_BASIC, Blob.from("Immediately after"))).asTry.futureValue shouldBe 'failure

      // Publish succeeds once connection is re-established
      Thread.sleep(600L)
      publish(PublishCommand(exchange, routingKey, MessageProperties.MINIMAL_PERSISTENT_BASIC, Blob.from("A while after"))).asTry.futureValue shouldBe 'success

      testQueue.head.consumeAllMessages() should have size 2
    }

  }

  private def killRabbitConnection(): Unit = {
    val jsonResult = rmqAdminHhttp.handle(GET("/api/connections")).body.to[JsonResult]
    for {
      connection <- jsonResult.array if connection("user").string == amqpClientConfig.username
    } {
      val connectionName = connection("name").string
      println(s"Killing connection $connectionName")
      rmqAdminHhttp.handle(DELETE(UriBuilder / "api" / "connections" / connectionName)) shouldBe 'successful
    }
  }


}
