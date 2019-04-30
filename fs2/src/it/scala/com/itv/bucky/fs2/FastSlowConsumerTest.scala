package com.itv.bucky.fs2

import java.util.UUID

import cats.effect.{ContextShift, IO, Timer}
import cats.effect.concurrent.Ref
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.decl.{Exchange, Queue}
import com.itv.bucky.{Ack, AmqpClient, AmqpClientConfig, ExchangeName, Handler, PayloadMarshaller, PayloadUnmarshaller, PublishCommandBuilder, QueueName, RoutingKey}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{FunSuite}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.Matchers._
import fs2.Stream

class FastSlowConsumerTest extends FunSuite with StrictLogging with Eventually with IntegrationPatience {

  test("A fast and slow consumer should be able to work concurrently") {
    val rawConfig = ConfigFactory.load("bucky")
    val config =
      AmqpClientConfig(
        rawConfig.getString("rmq.host"),
        rawConfig.getInt("rmq.port"),
        rawConfig.getString("rmq.username"),
        rawConfig.getString("rmq.password"))

    val exchange = ExchangeName("anexchange")
    val slowQueue = QueueName("aqueue1" + UUID.randomUUID().toString)
    val slowRk = RoutingKey("ark1")
    val fastQueue = QueueName("aqueue2" + UUID.randomUUID().toString)
    val fastRk = RoutingKey("ark2")


    implicit val contextShift: ContextShift[IO] = IO.contextShift(implicitly)
    implicit val timer: Timer[IO] = IO.timer(implicitly)

    lazy val buffer: Ref[IO, List[String]] = Ref.of[IO, List[String]](List.empty[String]).unsafeRunSync()

    val declarations =
      List(Queue(slowQueue), Queue(fastQueue), Exchange(exchange).binding(slowRk -> slowQueue).binding(fastRk -> fastQueue))

    IOAmqpClient.use(config, declarations) { client =>
      val fastPcb =
        PublishCommandBuilder.publishCommandBuilder(StringPayloadMarshaller).using(exchange).using(fastRk)
      val slowPcb =
        PublishCommandBuilder.publishCommandBuilder(StringPayloadMarshaller).using(exchange).using(slowRk)

      val fastPublisher = client.publisherOf(fastPcb)
      val slowPublisher = client.publisherOf(slowPcb)

      val fastHandler: Handler[IO, String] = (v1: String) => {
        for {
          _ <- IO.delay(logger.error("banana fast invoked"))
          _ <- buffer.update { current =>
            logger.error("banana fast updating, current: " + current)
            current :+ "fast"
          }
        }
          yield {
            logger.error("fast yielding")
            Ack
          }
      }
      val slowHandler: Handler[IO, String] = (v1: String) => {
        for {
          _ <- IO.delay(logger.error("banana slow invoked"))
          _ <- IO.sleep(10.second)
          _ <- IO.delay(logger.error("banana slow done sleeping"))
          _ <- buffer.update { current =>
            logger.error("banana slow updating, current: " + current)
            current :+ "slow"
          }
        }
          yield {
            logger.error("slow yielding")
            Ack
          }
      }

      client.consumer(slowQueue, AmqpClient.handlerOf[IO, String](slowHandler, StringPayloadUnmarshaller)).concurrently(
        client.consumer(fastQueue, AmqpClient.handlerOf[IO, String](fastHandler, StringPayloadUnmarshaller))
      ).concurrently(Stream.eval {
        IO {
          Thread.sleep(1000)
          logger.error("publishing slow")
          slowPublisher("slow message").unsafeRunSync()
          Thread.sleep(1000)
          logger.error("publishing fast")
          fastPublisher("fast message").unsafeRunSync()
        }
      })
    }
      .compile
      .drain
      .unsafeRunAsync(cb => logger.error("BANANA " + cb))

    eventually {
      val messages = buffer.get.unsafeRunSync()
      messages should have size 2
      messages shouldBe List("fast", "slow")
    }
  }

}