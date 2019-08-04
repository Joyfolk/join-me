package join

trait DataAccumulator[H] {
  def close(): Unit
  def reset(): Unit
  def currentKey: Option[Data#Key]
  def setKey(key: Data#Key): Unit
  protected def writer: DataWriter[H, Data]
  def reader: DataReader[H]

  def accumulate(iter: BufferedIterator[Data], key: Data#Key): Long = {
    reset()
    val w = writer
    var count = 0L
    try {
      while (iter.hasNext && iter.head.key <= key) {
        val r = iter.next()
        if (r.key == key) {
          setKey(key)
          w.write(r)
          count += 1
        }
      }
    } finally w.close()
    count
  }
}
