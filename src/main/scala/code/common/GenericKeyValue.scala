package code.common

import cats.syntax.eq._
import cats.syntax.monoid._
import cats.{Eq, Monoid}

case class GenericKeyValue[K, A](timestamp: Long, key: K, value: A)

object GenericKeyValue {

  implicit def timestampedGenericKeyValue[K, A]: TimestampedObject[GenericKeyValue[K, A]] =
    TimestampedObject.instance(_.timestamp)

  implicit def gkvMonoid[K: Monoid: Eq, A: Monoid]: Monoid[GenericKeyValue[K, A]] =
    Monoid.instance(emptyValue = GenericKeyValue(0L, Monoid[K].empty, Monoid[A].empty), cmb = {
      case (gkv1, gkv2) if gkv1.key === gkv2.key =>
        gkv2.copy(math.max(gkv1.timestamp, gkv2.timestamp), value = gkv1.value |+| gkv2.value)
      case (gkv1, gkv2) if gkv1.key.isEmpty => gkv2
      case (gkv1, gkv2) if gkv2.key.isEmpty => gkv1
      case (gkv1, gkv2) =>
        throw new NoSuchMethodException(
          s"Combination of general key value elements is only possible for same keys: Got ${gkv1.key} and ${gkv2.key}"
        )
    })
}
