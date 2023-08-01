package code.bots

import code.common.KeyValue
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.time.Instant
import scala.util.Random

class RandomEventSource extends SourceFunction[KeyValue[Int]] with LazyLogging {

  @volatile var isRunning: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[KeyValue[Int]]): Unit = {
    val lock = ctx.getCheckpointLock

    while (isRunning) {
      val event = KeyValue(randomString, randomInt())

      lock.synchronized {
        ctx.collectWithTimestamp(event, Instant.now.toEpochMilli)
      }

      ctx.markAsTemporarilyIdle()
      logger.info("Idling source for 1 second")
      try {
        Thread.sleep(1000)
      } catch {
        case _: InterruptedException => logger.warn("Sleep interrupted")
      }
    }
  }

  private def randomInt(n: Int = 10): Int = Random.nextInt(n)

  private def randomString: String = {
    val length = 5 + randomInt(5)
    Random.nextString(length)
  }

  override def cancel(): Unit = isRunning = false
}
