package join

trait Row[K, T] {
  type Key
  def key: K
  def value: T
  def asString: String
}

case class Data(override val key: Long, override val value: String) extends Row[Long, String] {
  type Key = Long
  override def asString: String = s"$key\t$value"
}

case class Result(override val key: Long, left: String, right: String) extends Row[Long, (String, String)] {
  type Key = Long
  override def value: (String, String) = (left, right)
  override def asString: String =
    if (right != null) s"$key\t$left\t$right"
    else s"$key\t$left\t"
}

object Row {
  def fromString(s: String): Data = s.trim.split('\t') match {
    case Array(k, v) => Data(k.toLong, v)
    case _           => throw new IllegalArgumentException(s"invalid format: [$s]")
  }

  def ordering[K, R <: Row[K, _]](implicit ord: Ordering[K]): Ordering[R] = Ordering.by(_.key)
}
