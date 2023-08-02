package code.savepoint

import code.util.extensionmethods._
import com.typesafe.scalalogging.LazyLogging
import io.findify.flink.api.StreamExecutionEnvironment
import io.findify.flinkadt.api._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.state.api.{OperatorIdentifier, SavepointReader, SavepointWriter}

object AnotherBotStateReader extends SavepointManager with LazyLogging {

  override val checkpointDir: String = "83a2af1520c2eab7ac48971393867aeb/chk-3"

  def main(args: Array[String]): Unit = analyze()

  override def processSavepoint(
    savepoint: SavepointReader
  )(implicit env: StreamExecutionEnvironment): Option[SavepointWriter] = {
    savepoint
      .readListState(
        OperatorIdentifier.forUid("event-source"),
        "state",
        implicitly[TypeInformation[List[Int]]]
      )
      .toScalaStream
      .uid("state-source")
      .debug(logger = logger.debug(_))
      .uid("debugger")

    None
  }
}
