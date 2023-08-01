package code.common

import cats.syntax.eq._
import cats.syntax.semigroup._
import cats.{Eq, Monoid}

case class KeyValue[A](key: String, value: A) {
  def tupled: (String, A) = (key, value)
}

object KeyValue {
  implicit def timedKeyValue[A]: TimestampedObject[A] = TimestampedObject.instance(_ => 0L)

  implicit def kvMonoid[A: Monoid]: Monoid[KeyValue[A]] = Monoid.instance(
    emptyValue = KeyValue("", Monoid[A].empty),
    cmb = {
      case (kv1, kv2) if kv1.key === kv2.key => kv2.copy(value = kv1.value |+| kv2.value)
      case _ =>
        throw new NoSuchMethodException(
          "Combination of key value elements is only possible for same keys"
        )
    }
  )

  implicit def kvEq[A: Eq]: Eq[KeyValue[A]] =
    Eq.instance((kv1, kv2) => kv1.key == kv2.key && kv1.value == kv2.value)
}
