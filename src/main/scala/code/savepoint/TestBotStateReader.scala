package code.savepoint

import code._
import code.util.FlinkStateHelper
import code.util.extensionmethods._
import com.typesafe.scalalogging.LazyLogging
import io.findify.flink.api.StreamExecutionEnvironment
import io.findify.flinkadt.api._
import org.apache.flink.api.common.state.ValueState
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
    savepoint
      .readKeyedState(OperatorIdentifier.forUid("aggregate"), new ReaderFunction)
      .toScalaStream
      .uid("state-source")
      .debug(logger = logger.debug(_))
      .uid("debugger")

    None
  }

  class ReaderFunction extends KeyedStateReaderFunction[KVString, KVString] with FlinkStateHelper {

    var sum: ValueState[Int] = _
    var count: ValueState[Int] = _

    override def open(parameters: Configuration): Unit = {
      sum = valueState("sum")
      count = valueState("count")
    }

    override def readKey(
      key: KVString,
      ctx: KeyedStateReaderFunction.Context,
      out: Collector[KVString]
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
