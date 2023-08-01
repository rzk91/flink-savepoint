package code.bots

import code._
import code.flink.{CountAggregateFunction, TumblingZonedEventTimeWindows}
import code.util.extensionmethods.RichDataStream
import io.circe.generic.auto._
import io.findify.flink.api.{DataStream, StreamExecutionEnvironment}
import io.findify.flinkadt.api._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.concurrent.duration._

object WindowTestBot extends AnalyticsBot[GenericKVStringInt, GenericKVStringDouble] {

  override val kafkaTopic: String = "flink117-test"

  implicit val typeInfoKeyString: TypeInformation[KVString] = TypeInformation.of(classOf[KVString])

  def main(args: Array[String]): Unit = analyze()

  override protected def analyzeAllEvents(
    eventStream: DataStream[GenericKVStringInt]
  )(implicit env: StreamExecutionEnvironment): DataStream[GenericKVStringDouble] =
    eventStream
      .debug(logger = logger.debug(_))
      .uid("debugger")
      .keyBy(_.key)
      .window(TumblingZonedEventTimeWindows(20.seconds, (gkv: GenericKVStringInt) => gkv.key))
      .aggregate(
        new CountAggregateFunction[GenericKVStringInt, GenericKVStringDouble](
          acc => acc.value.copy(value = acc.value.value / acc.count.toDouble)
        ),
        (
          _: KVString,
          w: TimeWindow,
          input: Iterable[GenericKVStringDouble],
          output: Collector[GenericKVStringDouble]
        ) => output.collect(input.head.copy(timestamp = w.getEnd))
      )
      .uid("aggregate")
}
