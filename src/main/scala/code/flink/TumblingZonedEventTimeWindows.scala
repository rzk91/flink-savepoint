package code.flink

import com.typesafe.scalalogging.LazyLogging
import io.findify.flink.api.StreamExecutionEnvironment
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaEnv}
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, PurgingTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import java.time.{Instant, ZoneId}
import java.util
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

class TumblingZonedEventTimeWindows[A] private (
  zoneIdExtractor: A => ZoneId,
  duration: Duration,
  logIdExtractor: A => String
) extends WindowAssigner[A, TimeWindow]
    with LazyLogging {

  override def assignWindows(
    element: A,
    timestamp: Long,
    context: WindowAssigner.WindowAssignerContext
  ): util.Collection[TimeWindow] =
    if (timestamp > Long.MinValue) {
      val from = timestamp - timestamp % duration.toMillis
      val start = Instant.ofEpochMilli(from).atZone(zoneIdExtractor(element))
      val timeWindow = new TimeWindow(
        start.toInstant.toEpochMilli,
        start.plus(duration.length, duration.unit.toChronoUnit).toInstant.toEpochMilli
      )

      logger.trace(s"[${logIdExtractor(element)}] Assigning to $timeWindow: $element")

      List(timeWindow).asJava
    } else {
      throw new RuntimeException(
        "Record has Long.MinValue timestamp (= no timestamp marker). " +
        "Is the time characteristic set to 'ProcessingTime', " +
        "or did you forget to call '.withTimestampAssigner(...)' on your data stream?"
      )
    }

  override def getDefaultTrigger(env: JavaEnv): Trigger[A, TimeWindow] =
    PurgingTrigger.of(EventTimeTrigger.create.asInstanceOf[Trigger[A, TimeWindow]])

  override def getWindowSerializer(executionConfig: ExecutionConfig): TypeSerializer[TimeWindow] =
    new TimeWindow.Serializer

  override def isEventTime: Boolean = true

  override def toString: String = s"TumblingEventTimeWindows($zoneIdExtractor, $duration)"
}

object TumblingZonedEventTimeWindows {

  def apply[A, K](zoneIdExtractor: A => ZoneId, duration: Duration, logIdExtractor: A => K)(
    implicit
    env: StreamExecutionEnvironment
  ): TumblingZonedEventTimeWindows[A] = new TumblingZonedEventTimeWindows(
    env.getJavaEnv.clean(zoneIdExtractor),
    duration,
    env.getJavaEnv.clean(logIdExtractor).andThen(_.toString)
  )

  def apply[A, K](duration: Duration, logIdExtractor: A => K)(
    implicit
    env: StreamExecutionEnvironment
  ): TumblingZonedEventTimeWindows[A] =
    new TumblingZonedEventTimeWindows(
      _ => ZoneId.of("UTC"),
      duration,
      env.getJavaEnv.clean(logIdExtractor).andThen(_.toString)
    )
}
