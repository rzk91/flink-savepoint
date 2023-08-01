package code.common

import cats.Monoid

case class ValueCount[A](value: A, count: Int)

object ValueCount {
  def init[A: Monoid]: ValueCount[A] = ValueCount(Monoid[A].empty, 0)
}
