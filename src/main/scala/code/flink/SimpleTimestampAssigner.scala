package code.flink

import code.common.TimestampedObject
import code.util.extensionmethods._
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner

class SimpleTimestampAssigner[A: TimestampedObject] extends SerializableTimestampAssigner[A] {
  override def extractTimestamp(element: A, recordTimestamp: Long): Long = element.timestamp
}
