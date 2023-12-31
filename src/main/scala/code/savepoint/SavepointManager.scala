package code.savepoint

import io.findify.flink.api.StreamExecutionEnvironment
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.state.api.{SavepointReader, SavepointWriter}

trait SavepointManager {

  def checkpointDir: String
  def savepointDir: Option[String] = None

  def processSavepoint(savepoint: SavepointReader)(implicit env: StreamExecutionEnvironment): Option[SavepointWriter]

  def analyze(): Unit = {
    val mainDir = "/Users/rzk91/Documents/Work/Git/flink-checkpoints-test/checkpoints"
    val checkpointFile = s"file://$mainDir/$checkpointDir"
    val savepointFile = savepointDir.map(dir => s"file://$savepointDir/$dir")

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableAutoGeneratedUIDs()

    val backend: StateBackend = new HashMapStateBackend()

    val savepoint = SavepointReader.read(env.getJavaEnv, checkpointFile, backend)

    for {
      sp   <- processSavepoint(savepoint)
      file <- savepointFile
    } yield sp.write(file)

    env.execute()
  }
}
