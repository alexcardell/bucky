package com.itv.bucky.example.argonaut

import com.itv.lifecycle.Lifecycle
import com.typesafe.scalalogging.StrictLogging
import com.itv.bucky._
import com.itv.bucky.decl._
import com.itv.bucky.lifecycle._
import com.itv.bucky.future._
import com.itv.bucky.example.argonaut.Shared.Person

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/*
  The only difference between this and itv.bucky.example.marshalling.UnmarshalledConsumer
  is the way the PayloadUnmarshaller is defined in itv.bucky.example.argonaut.Shared!
 */
object ArgonautUnmarshalledConsumer extends App with StrictLogging {

  object Declarations {
    val queue = Queue(QueueName("queue.people.argonaut"))
    val all = List(queue)
  }

  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig("33.33.33.11", 5672, "guest", "guest")

  val personHandler =
    Handler[Future, Person] { message: Person =>
      Future {
        logger.info(s"${message.name} is ${message.age} years old")
        Ack
      }
    }

  /**
    * A lifecycle is a monadic try/finally statement.
    * More detailed information is available here https://github.com/ITV/lifecycle
    */
  val lifecycle: Lifecycle[Unit] =
    for {
      amqpClient <- AmqpClientLifecycle(amqpClientConfig)
      _ <- DeclarationLifecycle(Declarations.all, amqpClient)
      _ <- amqpClient.consumer(Declarations.queue.name, AmqpClient.handlerOf(personHandler, Shared.personUnmarshaller))
    }
      yield ()

  lifecycle.runUntilJvmShutdown()

}
