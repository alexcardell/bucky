package com.itv.bucky

import cats.effect._
import cats.effect.implicits._
import cats.effect.std.Dispatcher
import cats.implicits._
import com.itv.bucky.consume._
import com.itv.bucky.publish._
import com.itv.bucky.decl.Declaration
import com.itv.bucky.publish.PendingConfirmListener
import com.typesafe.scalalogging.StrictLogging

import scala.collection.immutable.TreeMap
import scala.language.higherKinds
import scala.util.Try
import cats.effect.{Deferred, Ref, Temporal}

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

private[bucky] case class AmqpClientConnectionManager[F[_]](
    amqpConfig: AmqpClientConfig,
    publishChannel: Channel[F],
    pendingConfirmListener: PendingConfirmListener[F],
    dispatcher: Dispatcher[F],
    executionContext: ExecutionContext
)(implicit F: Async[F], t: Temporal[F])
    extends StrictLogging {

  private def runWithChannelSync[T](action: F[T]): F[T] =
    publishChannel.synchroniseIfNeeded {
      F.fromTry(Try {
        dispatcher.unsafeRunSync(action)
      })
    }

  def publish(cmd: PublishCommand): F[Unit] =
    for {
      deliveryTag <- Ref.of[F, Option[Long]](None)
      _ <- (for {
        signal <- Deferred[F, Boolean]
        _ <- runWithChannelSync {
          for {
            nextPublishSeq <- publishChannel.getNextPublishSeqNo
            _              <- deliveryTag.set(Some(nextPublishSeq))
            _              <- pendingConfirmListener.pendingConfirmations.update(_ + (nextPublishSeq -> signal))
            _              <- publishChannel.publish(nextPublishSeq, cmd)
          } yield ()
        }
        _ <- signal.get.ifM(F.unit, F.raiseError[Unit](new RuntimeException(s"Failed to publish msg: ${cmd}")))
      } yield ())
        .timeout(amqpConfig.publishingTimeout)
        .recoverWith {
          case e =>
            runWithChannelSync {
              for {
                dl          <- deliveryTag.get
                deliveryTag <- F.fromOption(dl, new RuntimeException("Timeout occurred before a delivery tag could be obtained.", e))
                _           <- pendingConfirmListener.pop(deliveryTag, multiple = false)
                _           <- F.raiseError[Unit](e)
              } yield ()
            }
        }
    } yield ()

  def registerConsumer(channel: Channel[F],
                       queueName: QueueName,
                       handler: Handler[F, Delivery],
                       onHandlerException: ConsumeAction,
                       prefetchCount: Int): F[Unit] =
    (for {
      consumerTag <- F.delay(ConsumerTag.create(queueName))
      _           <- F.delay(logger.debug("Registering consumer for queue: {} with tag {}.", queueName.value, consumerTag.value))
      _           <- channel.basicQos(prefetchCount)
      _           <- channel.registerConsumer(handler, onHandlerException, queueName, consumerTag)
      _           <- F.delay(logger.debug("Consumer for queue: {} with tag {} was successfully registered.", queueName.value, consumerTag.value))
      _           <- F.delay(logger.debug("Successfully registered consumer for queue: {} with tag.", queueName.value), consumerTag.value)
    } yield ()).evalOn(executionContext)

  def declare(declarations: Iterable[Declaration]): F[Unit] = publishChannel.runDeclarations(declarations)
}

private[bucky] object AmqpClientConnectionManager extends StrictLogging {

  def apply[F[_]](config: AmqpClientConfig, publishChannel: Channel[F], dispatcher: Dispatcher[F], executionContext: ExecutionContext)(implicit
      F: Async[F],
      t: Temporal[F]
  ): F[AmqpClientConnectionManager[F]] =
    for {
      pendingConfirmations <- Ref.of[F, TreeMap[Long, Deferred[F, Boolean]]](TreeMap.empty)
      pendingReturn        <- Ref.of[F, Boolean](false)
      _                    <- publishChannel.confirmSelect
      confirmListener      <- F.blocking(publish.PendingConfirmListener(pendingConfirmations, pendingReturn, dispatcher))
      _                    <- publishChannel.addConfirmListener(confirmListener)
      _                    <- publishChannel.addReturnListener(confirmListener)
    } yield AmqpClientConnectionManager(config, publishChannel, confirmListener, dispatcher, executionContext)
}
