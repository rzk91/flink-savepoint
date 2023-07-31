package code

import code.FlinkOps.RichDataStream
import io.circe.generic.auto._
import io.findify.flink.api.{DataStream, StreamExecutionEnvironment}
import io.findify.flinkadt.api._

object AnotherBot extends AnalyticsBot[KeyValue[Int], KeyValue[Int]] {

  override def kafkaTopic: String = ""

  def main(args: Array[String]): Unit = analyze()

  override protected def analyzeAllEvents(
    eventStream: DataStream[KeyValue[Int]]
  ): DataStream[KeyValue[Int]] =
    eventStream
      .debug(logger = logger.debug(_))
      .name("Debugger")
      .uid("debugger")

  override protected def events(env: StreamExecutionEnvironment): DataStream[KeyValue[Int]] =
    env
      .addSource(new RandomEventSource)
      .name("Event Source")
      .uid("event-source")
}
