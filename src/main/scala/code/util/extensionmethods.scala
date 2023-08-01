package code.util

import code.common.TimestampedObject
import io.findify.flink.api.{DataStream, KeyedStream}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.datastream.{KeyedStream => JavaKeyedStream}

import java.time.{Duration => JDuration}
import scala.concurrent.duration.FiniteDuration

object extensionmethods {

  implicit class RichDataStream[A](private val stream: DataStream[A]) extends AnyVal {

    def debug(formatter: A => String = (a: A) => a.toString, logger: String => Unit = println(_))(
      implicit typeInfo: TypeInformation[A]
    ): DataStream[A] =
      stream.map { a: A =>
        logger(formatter(a))
        a
      }

    def keyedBy[K: TypeInformation](f: A => K): KeyedStream[A, K] = {
      val keyType = implicitly[TypeInformation[K]]
      val keyExtractor = new KeySelector[A, K] with ResultTypeQueryable[K] {
        def getKey(in: A): K = f(in) // TODO: Clean function before use

        override def getProducedType: TypeInformation[K] = keyType
      }

      new KeyedStream[A, K](new JavaKeyedStream(stream.javaStream, keyExtractor, keyType))
    }
  }

  implicit class RichDataSet[A](private val dataset: DataSet[A]) extends AnyVal {

    def debug(
      formatter: A => String = (a: A) => a.toString,
      logger: String => Unit = println(_)
    ): DataSet[A] =
      dataset.map {
        new MapFunction[A, A] {
          override def map(value: A): A = {
            logger(formatter(value))
            value
          }
        }
      }

    def void(): Unit = dataset.collect.clear()
  }

  implicit def durationToJavaDuration(duration: FiniteDuration): JDuration =
    JDuration.of(duration.length, duration.unit.toChronoUnit)

  implicit class TimestampedObjectOps[A](private val obj: A) extends AnyVal {
    def timestamp(implicit timedObj: TimestampedObject[A]): Long = timedObj.timestamp(obj)
  }
}
