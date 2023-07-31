package code

import code.FlinkOps.RichDataStream
import com.typesafe.scalalogging.LazyLogging
import io.findify.flink.api.DataStream
import io.findify.flinkadt.api._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.state.api.{OperatorIdentifier, SavepointReader}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object AnotherBotStateReader extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val checkpointDir =
      "file:///Users/rzk91/Documents/Work/Git/flink-checkpoints-test/checkpoints/83a2af1520c2eab7ac48971393867aeb/chk-3"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val backend: StateBackend = new HashMapStateBackend()

    val savepoint = SavepointReader.read(env, checkpointDir, backend)

    new DataStream(
      savepoint
        .readListState(
          OperatorIdentifier.forUid("event-source"),
          "state",
          implicitly[TypeInformation[List[Int]]]
        )
    ).debug(logger = logger.debug(_))
      .uid("debugger")

    env.execute()
  }
}
