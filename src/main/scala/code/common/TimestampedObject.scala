package code.common

trait TimestampedObject[A] extends Serializable {
  def timestamp(a: A): Long
}

object TimestampedObject {

  /**
    * Access an implicit `TimestampedObject[A]`
    */
  @inline final def apply[A](implicit ev: TimestampedObject[A]): TimestampedObject[A] = ev

  /**
    * Create a `TimestampedObject` instance from the given function
    */
  @inline def instance[A](timestampExtractor: A => Long): TimestampedObject[A] =
    timestampExtractor(_)
}
