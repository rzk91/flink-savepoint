package code

import code.FlinkOps.RichDataSet
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.state.api.Savepoint
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.util.Collector

object TestBotStateReader extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val checkpointDir =
      "file:///Users/rzk91/Documents/Work/Git/flink-checkpoints-test/checkpoints/e7a62ab2d130fa62622ada891d00809c/chk-1"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val backend: StateBackend = new MemoryStateBackend()

    val savepoint = Savepoint.load(env, checkpointDir, backend)

    savepoint
      .readKeyedState("aggregate", new ReaderFunction)
      .debug(logger = logger.debug(_))
      .void()

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
