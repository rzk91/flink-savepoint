package code.savepoint

import code._
import code.bots.WindowTestBot.typeInfoKeyString
import code.common.{GenericKeyValue, KeyValue, ValueCount}
import code.flink.{CountAggregateFunction, TumblingZonedEventTimeWindows}
import code.util.extensionmethods._
import com.typesafe.scalalogging.LazyLogging
import io.findify.flink.api.StreamExecutionEnvironment
import io.findify.flinkadt.api._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.state.api.functions.WindowReaderFunction
import org.apache.flink.state.api.{SavepointReader, SavepointWriter}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import scala.concurrent.duration._
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter

object WindowTestBotStateReader extends SavepointManager with LazyLogging {

  override val checkpointDir: String = "648c102ca2ee82f6a62cedda0e47c77a/chk-27"

  def main(args: Array[String]): Unit = analyze()

  override def processSavepoint(
    savepoint: SavepointReader
  )(implicit env: StreamExecutionEnvironment): Option[SavepointWriter] = {
    savepoint
      .window(
        TumblingZonedEventTimeWindows(
          20.seconds,
          (gkv: GenericKeyValue[KeyValue[String], Int]) => gkv.key
        )
      )
      .aggregate(
        "aggregate",
        new CountAggregateFunction[GenericKVStringInt, GenericKVStringDouble](
          acc => acc.value.copy(value = acc.value.value / acc.count.toDouble)
        ),
        new WindowStateReader,
        implicitly[TypeInformation[KVString]],
        implicitly[TypeInformation[ValueCount[GenericKVStringInt]]],
        implicitly[TypeInformation[GenericKVStringDouble]]
      )
      .toScalaStream
      .uid("state-reader")
      .debug(logger = logger.debug(_))
      .uid("debugger")

    None
  }

  class WindowStateReader
      extends WindowReaderFunction[
        GenericKVStringDouble,
        GenericKVStringDouble,
        KVString,
        TimeWindow
      ] {

    override def readWindow(
      key: KVString,
      ctx: WindowReaderFunction.Context[TimeWindow],
      elements: lang.Iterable[GenericKVStringDouble],
      out: Collector[GenericKVStringDouble]
    ): Unit = {
      logger.debug(
        s"[$key] Current window in state: ${ctx.window}; " +
        s"Current elements in window: ${elements.asScala}; " +
        s"Current timers: ${ctx.registeredEventTimeTimers.asScala}"
      )

      out.collect(elements.asScala.last)
    }
  }
}
