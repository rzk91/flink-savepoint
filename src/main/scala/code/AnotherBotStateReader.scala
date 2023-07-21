package code

import code.FlinkOps.RichDataSet
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.state.api.Savepoint

object AnotherBotStateReader extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val checkpointDir =
      "file:///Users/rzk91/Documents/Work/Git/flink-checkpoints-test/checkpoints/83a2af1520c2eab7ac48971393867aeb/chk-3"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val backend: StateBackend = new MemoryStateBackend()

    val savepoint = Savepoint.load(env, checkpointDir, backend)

    savepoint
      .readListState("event-source", "state", implicitly[TypeInformation[List[Int]]])
      .debug(logger = logger.debug(_))
      .void()

    env.execute()
  }
}
