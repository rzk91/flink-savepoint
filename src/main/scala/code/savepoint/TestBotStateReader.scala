package code.savepoint

import code.common.KeyValue
import code.util.extensionmethods.RichDataStream
import com.typesafe.scalalogging.LazyLogging
import io.findify.flink.api.{DataStream, StreamExecutionEnvironment}
import io.findify.flinkadt.api._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.state.api.{OperatorIdentifier, SavepointReader, SavepointWriter}
import org.apache.flink.util.Collector

object TestBotStateReader extends SavepointManager with LazyLogging {

  override val checkpointDir: String = "cbab1558931def8a458a4ea89ce6134c/chk-9"
  def main(args: Array[String]): Unit = analyze()

  override def processSavepoint(
    savepoint: SavepointReader
  )(implicit env: StreamExecutionEnvironment): Option[SavepointWriter] = {
    new DataStream(
      savepoint
        .readKeyedState(OperatorIdentifier.forUid("aggregate"), new ReaderFunction)
    ).uid("state-source")
      .debug(logger = logger.debug(_))
      .uid("debugger")

    None
  }

  class ReaderFunction extends KeyedStateReaderFunction[KeyValue[String], KeyValue[String]] {

    var sum: ValueState[Int] = _
    var count: ValueState[Int] = _

    override def open(parameters: Configuration): Unit = {
      sum = getRuntimeContext.getState(
        new ValueStateDescriptor[Int]("sum", implicitly[TypeInformation[Int]])
      )
      count = getRuntimeContext.getState(
        new ValueStateDescriptor[Int]("count", implicitly[TypeInformation[Int]])
      )
    }

    override def readKey(
      key: KeyValue[String],
      ctx: KeyedStateReaderFunction.Context,
      out: Collector[KeyValue[String]]
    ): Unit = {
      logger.debug(
        s"[$key] State variables: " +
        s"Sum state: ${sum.value}; " +
        s"Count state: ${count.value}; "
      )

      out.collect(key.copy(value = "State successfully read."))
    }
  }
}
