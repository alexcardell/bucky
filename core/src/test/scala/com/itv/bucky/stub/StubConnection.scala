package com.itv.bucky.stub

import com.google.common.util.concurrent.MoreExecutors
import com.rabbitmq.client.impl.AMQImpl.Basic.ConsumeOk
import com.rabbitmq.client.impl.AMQImpl.Confirm.SelectOk
import com.rabbitmq.client.impl._
import com.rabbitmq.client.{AMQP, Method, TrafficListener, MessageProperties => RMessageProperties}
import org.scalatest.Matchers
import org.scalatest.mockito.MockitoSugar

import scala.collection.mutable.ListBuffer

private[this] object StubConnection extends MockitoSugar with Matchers {
  import org.mockito.Mockito.when
  val stubConnection: AMQConnection = mock[com.rabbitmq.client.impl.AMQConnection]
  when(stubConnection.getChannelRpcTimeout).thenReturn(1)
  when(stubConnection.getTrafficListener).thenReturn(TrafficListener.NO_OP)

}

class StubChannel
    extends ChannelN(StubConnection.stubConnection,
                     0,
                     new ConsumerWorkService(MoreExecutors.newDirectExecutorService(), null, 1)) {

  val transmittedCommands: ListBuffer[Method]   = ListBuffer.empty
  val consumers: ListBuffer[AMQP.Basic.Consume] = ListBuffer.empty
  var setPrefetchCount                          = 0

  override def quiescingTransmit(c: AMQCommand): Unit = {
    val method = c.getMethod
    method match {
      case _: AMQP.Confirm.Select =>
        replyWith(new SelectOk())
      case c: AMQP.Basic.Consume =>
        consumers += c
        replyWith(new ConsumeOk(c.getConsumerTag))
      case _: AMQP.Basic.Publish =>
        ()
      case _: AMQP.Basic.Ack =>
        ()
      case _: AMQP.Basic.Nack =>
        ()
      case other =>
        throw new IllegalStateException("StubChannel does not know how to handle " + other)
    }
    transmittedCommands += method
  }

  override def basicQos(prefetchCount: Int): Unit =
    setPrefetchCount = prefetchCount

  def replyWith(method: Method): Unit =
    handleCompleteInboundCommand(new AMQCommand(method))

  def deliver(delivery: AMQP.Basic.Deliver,
              body: Payload,
              properties: AMQP.BasicProperties = RMessageProperties.BASIC): Unit =
    handleCompleteInboundCommand(new AMQCommand(delivery, properties, body.value))
}
