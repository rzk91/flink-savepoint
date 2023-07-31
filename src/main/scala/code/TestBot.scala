package code

import code.FlinkOps.RichDataStream
import io.circe.generic.auto._
import io.findify.flink.api.DataStream
import io.findify.flinkadt.api._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

object TestBot
    extends AnalyticsBot[
      GenericKeyValue[KeyValue[String], Int],
      GenericKeyValue[KeyValue[String], Double]
    ] {

  override val kafkaTopic: String = "another-test"

  implicit val typeInfoKeyString: TypeInformation[KeyValue[String]] =
    TypeInformation.of(classOf[KeyValue[String]])

  def main(args: Array[String]): Unit = analyze()

  override protected def analyzeAllEvents(
    eventStream: DataStream[GenericKeyValue[KeyValue[String], Int]]
  ): DataStream[GenericKeyValue[KeyValue[String], Double]] =
    eventStream
      .debug(logger = logger.debug(_))
      .uid("debugger")
      .keyBy(_.key)
      .process(new CustomAggregationFunction)
      .uid("aggregate")

  class CustomAggregationFunction
      extends KeyedProcessFunction[KeyValue[String], GenericKeyValue[KeyValue[String], Int], GenericKeyValue[
        KeyValue[String],
        Double
      ]] {

    lazy val sum: ValueState[Int] = getRuntimeContext.getState(
      new ValueStateDescriptor[Int]("sum", implicitly[TypeInformation[Int]])
    )

    lazy val count: ValueState[Int] = getRuntimeContext.getState(
      new ValueStateDescriptor[Int]("count", implicitly[TypeInformation[Int]])
    )

    override def processElement(
      gkv: GenericKeyValue[KeyValue[String], Int],
      ctx: KeyedProcessFunction[KeyValue[String], GenericKeyValue[KeyValue[String], Int], GenericKeyValue[
        KeyValue[String],
        Double
      ]]#Context,
      out: Collector[GenericKeyValue[KeyValue[String], Double]]
    ): Unit = {
      logger.debug(
        s"[${ctx.getCurrentKey}] Current state values: sum = ${sum.value}, count = ${count.value}"
      )

      count.update(count.value + 1)
      sum.update(sum.value + gkv.value)

      out.collect(GenericKeyValue(ctx.getCurrentKey, sum.value / count.value.toDouble))
    }
  }
}
