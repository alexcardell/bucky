package com.itv.bucky.example.instrumentation
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import cats.effect.{ContextShift, IO, Timer}
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.decl.{Exchange, Queue}
import com.itv.bucky._
import com.itv.bucky.fs2.IOAmqpClient
import com.typesafe.scalalogging.StrictLogging
import kamon.{Kamon, SpanReporter}
import kamon.jaeger.JaegerReporter
import com.itv.bucky.instrumentation.InstrumentedAmqpClient

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import com.itv.bucky.PublishCommandBuilder._
import kamon.executors.util.ContextAwareExecutorService
import kamon.executors.{Executors => KamonExecutor}


object InstrumentationDemo extends App with StrictLogging {
  val executorService       = Executors.newWorkStealingPool(10)
  val contextAwareExService = ContextAwareExecutorService(executorService)
  implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(contextAwareExService)
  val reg                   = KamonExecutor.register("fork-join-pool", KamonExecutor.instrument(executorService))


  object IntPayloadUnmarshaller extends Unmarshaller[Payload, Int] {
    import UnmarshalResult._

    override def unmarshal(thing: Payload): UnmarshalResult[Int] =
      Try(new String(thing.value, "UTF-8").toInt) match {
        case util.Success(value) =>
          value.unmarshalSuccess
        case util.Failure(_) =>
          s"${thing.value} is not an integer".unmarshalFailure
      }
  }

  Kamon.addReporter(new JaegerReporter)

  import fs2.ioMonadError

  implicit val timer: Timer[IO] = IO.timer(ec)

  object Declarations {
    val sleepCommand = Queue(QueueName("sleep.command"))
    val alarmEvent = Queue(QueueName("alarm.event"))

    val exchange = Exchange(ExchangeName("foo")).binding(RoutingKey("bar") -> alarmEvent.name)
    val all   = List(sleepCommand, alarmEvent, exchange)
  }

  val amqpClientConfig =
    AmqpClientConfig("172.17.0.2", 5672, "guest", "guest")
      .copy(sharedExecutor = Some(contextAwareExService))

  def alarmClockHandler(coffeeMachineTrigger: Publisher[IO, Unit]) =
    Handler[IO, String] { alarm =>
      for {
        _ <- IO(logger.info("ALERT!!!!!!!!!!!!!!!!! " + alarm))
        _ <- coffeeMachineTrigger(())
      }
        yield Ack
    }

  def sleepHandler(alarmClock: Publisher[IO, String]) =
    Handler[IO, Int] { sleepFor: Int =>
      for {
        _ <- IO(logger.info(s"sleeping for ${sleepFor}ms"))
        _ <- IO { Thread.sleep(sleepFor) }
        _ <- alarmClock("beep beep")
      }
        yield Ack
    }

  val alarmCommandBuilder =
    PublishCommandBuilder.publishCommandBuilder[String](PayloadMarshaller.StringPayloadMarshaller)
      .using(ExchangeName("foo"))
      .using(RoutingKey("bar"))

  val coffeCommandBuilder =
    PublishCommandBuilder.publishCommandBuilder[Unit]((v1: Unit) => Payload.from("Plz make me a coffee"))
        .using(ExchangeName("foo"))
        .using(RoutingKey("covfefe"))

  IOAmqpClient
    .use(amqpClientConfig, Declarations.all) { amqpClient =>
      val wrapped = InstrumentedAmqpClient(amqpClient)
      val alarmClock = wrapped.publisherOf(alarmCommandBuilder)
      val coffeeMachineTrigger = wrapped.publisherOf(coffeCommandBuilder)

      val a = wrapped.consumer(Declarations.sleepCommand.name, AmqpClient.handlerOf(sleepHandler(alarmClock), IntPayloadUnmarshaller))
      val b = wrapped.consumer(Declarations.alarmEvent.name, AmqpClient.handlerOf(alarmClockHandler(coffeeMachineTrigger), StringPayloadUnmarshaller))

      implicit val cs: ContextShift[IO] = IO.contextShift(ec)
      a.concurrently(b)
    }
    .compile
    .drain
    .unsafeRunSync()

}
