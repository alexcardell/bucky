package com.itv.bucky.future

import org.scalatest.FunSuite

import scala.concurrent.Future
import FutureExt._
import com.itv.bucky.template.EstimatedMessageCountTest

class FutureEstimatedMessageCountTest extends FunSuite with EstimatedMessageCountTest[Future] with FuturePublisherTest {

  override def runAll(list: Seq[Future[Unit]]): Unit = Future.sequence(list)
}
