package code.bots

import code._
import code.util.FlinkStateHelper
import code.util.extensionmethods.RichDataStream
import io.circe.generic.auto._
import io.findify.flink.api.{DataStream, StreamExecutionEnvironment}
import io.findify.flinkadt.api._
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

object TestBot extends AnalyticsBot[GenericKVStringInt, GenericKVStringDouble] {

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
      .process(new CustomAggregationFunction)
      .uid("aggregate")

  class CustomAggregationFunction
      extends KeyedProcessFunction[KVString, GenericKVStringInt, GenericKVStringDouble]
      with FlinkStateHelper {

    lazy val sum: ValueState[Int] = valueState("sum")
    lazy val count: ValueState[Int] = valueState("count")

    override def processElement(
      gkv: GenericKVStringInt,
      ctx: KeyedProcessFunction[KVString, GenericKVStringInt, GenericKVStringDouble]#Context,
      out: Collector[GenericKVStringDouble]
    ): Unit = {
      logger.debug(
        s"[${ctx.getCurrentKey}] Current state values: sum = ${sum.value}, count = ${count.value}"
      )

      count.update(count.value + 1)
      sum.update(sum.value + gkv.value)

      out.collect(gkv.copy(key = ctx.getCurrentKey, value = sum.value / count.value.toDouble))
    }
  }
}
