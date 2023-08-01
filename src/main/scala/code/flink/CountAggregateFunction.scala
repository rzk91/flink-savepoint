package code.flink

import cats.Monoid
import cats.implicits._
import code.common.ValueCount
import org.apache.flink.api.common.functions.AggregateFunction

final class CountAggregateFunction[IN, OUT](f: ValueCount[IN] => OUT)(implicit monoid: Monoid[IN])
    extends AggregateFunction[IN, ValueCount[IN], OUT] {
  override def createAccumulator(): ValueCount[IN] = ValueCount.init

  override def add(value: IN, acc: ValueCount[IN]): ValueCount[IN] =
    ValueCount(acc.value |+| value, acc.count + 1)
  override def getResult(acc: ValueCount[IN]): OUT = f(acc)

  override def merge(a: ValueCount[IN], b: ValueCount[IN]): ValueCount[IN] =
    ValueCount(a.value |+| b.value, a.count + b.count)
}
