package code.flink

import io.circe.Decoder
import io.circe.parser._
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

class JsonDeserializer[IN: Decoder: TypeInformation] extends DeserializationSchema[IN] {

  override def deserialize(message: Array[Byte]): IN =
    decode(new String(message)).toOption.get // Fail fast

  override def isEndOfStream(nextElement: IN): Boolean = false

  override def getProducedType: TypeInformation[IN] = implicitly[TypeInformation[IN]]
}
