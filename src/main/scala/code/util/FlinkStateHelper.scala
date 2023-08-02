package code.util

import org.apache.flink.api.common.functions.AbstractRichFunction
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation

trait FlinkStateHelper { _: AbstractRichFunction =>

  def valueState[A: TypeInformation](name: String): ValueState[A] =
    getRuntimeContext.getState(new ValueStateDescriptor[A](name, implicitly[TypeInformation[A]]))

  def mapState[K: TypeInformation, V: TypeInformation](name: String): MapState[K, V] =
    getRuntimeContext.getMapState(
      new MapStateDescriptor[K, V](
        name,
        implicitly[TypeInformation[K]],
        implicitly[TypeInformation[V]]
      )
    )

  def listState[A: TypeInformation](name: String): ListState[A] =
    getRuntimeContext.getListState(new ListStateDescriptor[A](name, implicitly[TypeInformation[A]]))
}
