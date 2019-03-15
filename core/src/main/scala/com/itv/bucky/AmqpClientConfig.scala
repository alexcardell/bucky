package com.itv.bucky

import java.util.concurrent.{ExecutorService, ScheduledExecutorService, ThreadFactory}

import scala.concurrent.duration._

/**
  * AmqpClient configuration.
  */
case class AmqpClientConfig(host: String,
                            port: Int,
                            username: String,
                            password: String,
                            networkRecoveryInterval: Option[FiniteDuration] = Some(5.seconds),
                            networkRecoveryIntervalOnStart: Option[NetworkRecoveryOnStart] = Some(
                              NetworkRecoveryOnStart()),
                            virtualHost: Option[String] = None,
                            heartbeatExecutorService: Option[ScheduledExecutorService] = None,
                            sharedExecutor: Option[ExecutorService] = None,
                            topologyRecoveryExecutor: Option[ExecutorService] = None,
                            shutdownExecutor: Option[ExecutorService] = None,
                            threadFactory: Option[ThreadFactory] = None
                           )

case class NetworkRecoveryOnStart(interval: FiniteDuration = 30.seconds, max: FiniteDuration = 15.minutes) {
  val numberOfRetries = max.toMillis / interval.toMillis
}
