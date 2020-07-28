package com.itv.bucky

import cats.effect._
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.implicits._
import cats.implicits._
import com.itv.bucky.consume._
import com.itv.bucky.publish._
import com.itv.bucky.decl.Declaration
import com.itv.bucky.publish.PendingConfirmListener
import com.typesafe.scalalogging.StrictLogging

import scala.collection.immutable.TreeMap
import scala.language.higherKinds
import scala.util.Try

private[bucky] case class AmqpClientConnectionManager[F[_]](
                                                             amqpConfig: AmqpClientConfig,
                                                             publishChannelRef: Ref[F, (Channel[F], F[Unit])],
                                                             pendingConfirmListener: PendingConfirmListener[F])(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F])
    extends StrictLogging {

  private def runWithChannelSync[T](action: Channel[F] => F[T]): F[T] =
    publishChannelRef.get.flatMap(channel => channel._1.synchroniseIfNeeded {
      F.fromTry(Try {
        action(channel._1).toIO.unsafeRunSync()
      })
    })

  private def liftConfirmationResult(result: ConfirmationResult): F[Unit] =
    result match {
      case ConfirmationAck => F.unit
      case ConfirmationNack =>
        F.raiseError(new RuntimeException("Failed to publish due to nack from server"))
      case ConfirmationAborted(exception) =>
        F.raiseError(new RuntimeException("Failed to publish due to a channel exception", exception))
    }

  def publish(cmd: PublishCommand): F[Unit] =
    for {
      _ <- cs.shift
      deliveryTag <- Ref.of[F, Option[Long]](None)
      _ <- (for {
        signal <- Deferred[F, ConfirmationResult]
        _ <- runWithChannelSync { publishChannel =>
          for {
            nextPublishSeq <- publishChannel.getNextPublishSeqNo
            _              <- deliveryTag.set(Some(nextPublishSeq))
            _              <- pendingConfirmListener.pendingConfirmations.update(_ + (nextPublishSeq -> signal))
            _              <- publishChannel.publish(nextPublishSeq, cmd)
          } yield ()
        }
        _ <- signal.get.flatMap(liftConfirmationResult)
      } yield ())
        .timeout(amqpConfig.publishingTimeout)
        .recoverWith {
          case e =>
            runWithChannelSync { _ =>
              for {
                dl          <- deliveryTag.get
                deliveryTag <- F.fromOption(dl, new RuntimeException("Timeout occurred before a delivery tag could be obtained.", e))
                _           <- pendingConfirmListener.pop(deliveryTag, multiple = false)
                _           <- F.raiseError[Unit](e)
              } yield ()
            }
        }
    } yield ()

  def registerConsumer(channel: Channel[F], queueName: QueueName, handler: Handler[F, Delivery], onHandlerException: ConsumeAction, prefetchCount: Int): F[Unit] =
    for {
      _ <- cs.shift
      consumerTag <- F.delay(ConsumerTag.create(queueName))
      _ <- F.delay(logger.debug("Registering consumer for queue: {} with tag {}.", queueName.value, consumerTag.value))
      _ <- channel.basicQos(prefetchCount)
      _ <- channel.registerConsumer(handler, onHandlerException, queueName, consumerTag, cs)
      _ <- F.delay(logger.debug("Consumer for queue: {} with tag {} was successfully registered.", queueName.value, consumerTag.value))
      _ <- F.delay(logger.debug("Successfully registered consumer for queue: {} with tag.", queueName.value), consumerTag.value)
    } yield ()

  def declare(declarations: Iterable[Declaration]): F[Unit] = publishChannelRef.get.flatMap(_._1.runDeclarations(declarations))
}

private[bucky] object AmqpClientConnectionManager extends StrictLogging {

  def apply[F[_]](config: AmqpClientConfig,
                  buildChannel: () => Resource[F, Channel[F]])(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F]): F[AmqpClientConnectionManager[F]] =
    for {
      publishChannelManager <- PublishChannelManager(buildChannel())
    } yield AmqpClientConnectionManager(config, publishChannelManager.publishChannelRef, publishChannelManager.pendingConfirmListener)
}
