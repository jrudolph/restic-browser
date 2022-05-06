package net.virtualvoid.restic

import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }

import scala.concurrent.duration._

class Stats[T](name: String, userUnitName: String, userUnits: T => Long, reportInterval: FiniteDuration = 5.seconds) extends GraphStage[FlowShape[T, T]] {
  val in = Inlet[T]("stats.in")
  val out = Outlet[T]("stats.out")

  override def shape: FlowShape[T, T] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      setHandlers(in, out, this)

      var waitingOnPullMicros: Long = 0
      var waitingOnPushMicros: Long = 0
      var processed: Long = 0
      var userUnitsProcessed: Long = 0
      var latestTimestamp: Long = System.nanoTime()
      var nextMessageAt: Deadline = Deadline.now + reportInterval
      var lastProcessed: Long = 0
      var lastUserUnitsProcessed: Long = 0
      var lastReportedNanos = latestTimestamp

      override def onPush(): Unit = {
        val element = grab(in)

        val now = System.nanoTime()
        waitingOnPushMicros += (now - latestTimestamp) / 1000
        latestTimestamp = now
        processed += 1
        userUnitsProcessed += userUnits(element)
        if (nextMessageAt.isOverdue()) report(now)

        push(out, element)
      }
      override def onPull(): Unit = {
        val now = System.nanoTime()
        waitingOnPullMicros += (now - latestTimestamp) / 1000
        latestTimestamp = now
        if (nextMessageAt.isOverdue()) report(now)

        pull(in)
      }
      def report(now: Long): Unit = {
        val elementsPerSecond = (processed - lastProcessed).toFloat * 1000000000 / (now - lastReportedNanos)
        val userUnitsPerSecond = (userUnitsProcessed - lastUserUnitsProcessed).toFloat * 1000000000 / (now - lastReportedNanos)
        println(f"[$name%20s] processed: $processed%10d tpt: ${elementsPerSecond}%9.3f elements/s | processed (in total): ${userUnitsProcessed}%10d $userUnitName%-10s tpt: ${userUnitsPerSecond}%10.3f $userUnitName%10s/s | waiting rate: ${waitingOnPushMicros.toFloat / waitingOnPullMicros}%6.2f")

        nextMessageAt = Deadline.now + reportInterval
        lastProcessed = processed
        lastUserUnitsProcessed = userUnitsProcessed
        lastReportedNanos = now
      }
    }
}