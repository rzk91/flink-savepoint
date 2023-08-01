import code.common.{GenericKeyValue, KeyValue}

package object code {
  type KVString = KeyValue[String]
  type GenericKVStringInt = GenericKeyValue[KVString, Int]
  type GenericKVStringDouble = GenericKeyValue[KVString, Double]
}
