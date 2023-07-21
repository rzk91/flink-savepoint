package code

import code.FlinkOps.RichDataStream
import io.circe.generic.auto._
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object AnotherBot extends AnalyticsBot[KeyValue[Int], KeyValue[Int]] {

  override def kafkaTopic: String = ""

  def main(args: Array[String]): Unit = analyze()

  override protected def analyzeAllEvents(
    eventStream: DataStream[KeyValue[Int]]
  ): DataStream[KeyValue[Int]] = {
    eventStream
      .debug(logger = logger.debug(_))
      .name("Debugger")
      .uid("debugger")
  }

  override protected def events(env: StreamExecutionEnvironment): DataStream[KeyValue[Int]] =
    env
      .addSource(new RandomEventSource)
      .name("Event Source")
      .uid("event-source")
}
