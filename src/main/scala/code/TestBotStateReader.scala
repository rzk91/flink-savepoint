package code

import code.FlinkOps.RichDataStream
import com.typesafe.scalalogging.LazyLogging
import io.findify.flink.api.DataStream
import io.findify.flinkadt.api._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.state.api.{OperatorIdentifier, SavepointReader}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object TestBotStateReader extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val checkpointDir =
      "file:///Users/rzk91/Documents/Work/Git/flink-checkpoints-test/checkpoints/0405b46983d94f92da8e5524e3b52138/chk-1"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val backend: StateBackend = new HashMapStateBackend()

    val savepoint = SavepointReader.read(env, checkpointDir, backend)

    new DataStream(
      savepoint
        .readKeyedState(OperatorIdentifier.forUid("aggregate"), new ReaderFunction)
    ).debug(logger = logger.debug(_))
      .uid("debugger")

    env.execute()
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
