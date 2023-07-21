package code

import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.{CheckpointingOptions, Configuration}
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings
import org.apache.flink.runtime.minicluster.{MiniCluster, MiniClusterConfiguration}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.util.Properties

abstract class AnalyticsBot[IN: Decoder: TypeInformation, OUT: TypeInformation]
    extends LazyLogging
    with Serializable {

  def kafkaTopic: String

  protected def analyzeAllEvents(eventStream: DataStream[IN]): DataStream[OUT]

  final def analyze(): Unit = {
    val checkpointDir = "/Users/rzk91/Documents/Work/Git/flink-checkpoints-test/checkpoints"
    val savepointDir = s"$checkpointDir/83a2af1520c2eab7ac48971393867aeb/chk-3"

    val flinkConfig: Configuration = {
      val conf = new Configuration()
      conf.setString(CheckpointingOptions.STATE_BACKEND, "filesystem")
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
      SavepointRestoreSettings.forPath(savepointDir)
    )

    cluster.submitJob(jobGraph)
  }

  protected def events(env: StreamExecutionEnvironment): DataStream[IN] = {
    val consumer = new FlinkKafkaConsumer[IN](
      kafkaTopic,
      new KafkaDeserializationSchemaWrapper[IN](new JsonDeserializer[IN]), {
        val props = new Properties
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-reader-savepoints-2")
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        props
      }
    )

    env
      .addSource(consumer)
      .name("Event Source")
      .uid("event-source")
  }

}
