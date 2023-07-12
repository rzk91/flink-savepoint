package code

case class KeyValue[A] (key: String, value: A) {
  def tupled: (String, A) = (key, value)
}