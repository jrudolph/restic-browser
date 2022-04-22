package net.virtualvoid.restic

import spray.json.{ JsValue, JsonFormat }

import java.util.concurrent.ConcurrentHashMap

object DeduplicationCache {
  def apply[T](): T => T = {
    val cache = new ConcurrentHashMap[T, T]()

    t => cache.computeIfAbsent(t, _ => t)
  }

  def cachedFormat[T](format: JsonFormat[T], cache: T => T = apply[T]()): JsonFormat[T] = new JsonFormat[T] {
    override def write(obj: T): JsValue = format.write(obj)
    override def read(json: JsValue): T = cache(format.read(json))
  }
}