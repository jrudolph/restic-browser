package net.virtualvoid.restic

import spray.json._

object JsonExtra {
  implicit class RichJsValue(val jsValue: JsValue) extends AnyVal {
    def +(field: (String, JsValue)): JsObject = {
      val obj = jsValue.asJsObject
      obj.copy(fields = obj.fields + field)
    }
    def field(name: String): JsValue =
      jsValue.asJsObject.fields(name)
  }

  trait DeriveFormat[T] {
    def apply[U](toT: U => T, fromT: T => U)(implicit tFormat: JsonFormat[T]): JsonFormat[U]
    def to[U](toT: U => T, fromT: T => U)(implicit tFormat: JsonFormat[T]): JsonFormat[U] = apply(toT, fromT)(tFormat)
  }
  def deriveFormatFrom[T]: DeriveFormat[T] =
    new DeriveFormat[T] {
      override def apply[U](toT: U => T, fromT: T => U)(implicit tFormat: JsonFormat[T]): JsonFormat[U] = new JsonFormat[U] {
        def read(json: JsValue): U = fromT(json.convertTo[T])
        def write(obj: U): JsValue = toT(obj).toJson
      }
    }
}

