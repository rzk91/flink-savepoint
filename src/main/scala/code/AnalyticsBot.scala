package code

import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import io.findify.flink.api.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.{CheckpointingOptions, Configuration, StateBackendOptions}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings
import org.apache.flink.runtime.minicluster.{MiniCluster, MiniClusterConfiguration}
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.nio.file.{Files, Path}

abstract class AnalyticsBot[IN: Decoder: TypeInformation, OUT: TypeInformation]
    extends LazyLogging
    with Serializable {

  def kafkaTopic: String

  protected def analyzeAllEvents(eventStream: DataStream[IN]): DataStream[OUT]

  final def analyze(): Unit = {
    val checkpointDir = "/Users/rzk91/Documents/Work/Git/flink-checkpoints-test/checkpoints"
    val savepointDir = Option(Path.of(s"$checkpointDir/83a2af1520c2eab7ac48971393867aeb/chk-3"))
      .filter(Files.exists(_))

    val flinkConfig: Configuration = {
      val conf = new Configuration()
      conf.setString(StateBackendOptions.STATE_BACKEND, "filesystem")
      conf.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, s"file://$checkpointDir")
      conf.setString("execution.checkpointing.interval", "5s")
      conf.setString(
        "execution.checkpointing.externalized-checkpoint-retention",
        "RETAIN_ON_CANCELLATION"
      )
      conf
    }

    val cluster = new MiniCluster(
      new MiniClusterConfiguration.Builder()
        .setConfiguration(flinkConfig)
        .build()
    )

    cluster.start()

    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.createLocalEnvironment(1, flinkConfig)

    analyzeAllEvents(events(env))
      .addSink(new LoggerSink[OUT](logger, "error"))
      .name("Logger Output")
      .uid("logger-output")

    val jobGraph = env.getStreamGraph.getJobGraph

    jobGraph.setSavepointRestoreSettings(
      savepointDir
        .fold(SavepointRestoreSettings.none())(
          path => SavepointRestoreSettings.forPath(path.toUri.toString)
        )
    )

    cluster.submitJob(jobGraph)
  }

  protected def events(env: StreamExecutionEnvironment): DataStream[IN] = {
    val consumer = KafkaSource
      .builder[IN]
      .setBootstrapServers("localhost:9092")
      .setTopics(kafkaTopic)
      .setGroupId("kafka-reader-savepoints-new")
      .setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      .setValueOnlyDeserializer(new JsonDeserializer[IN])
      .build()

    env
      .fromSource(consumer, WatermarkStrategy.noWatermarks[IN], "Event Source")
      .uid("event-source")
  }

}
