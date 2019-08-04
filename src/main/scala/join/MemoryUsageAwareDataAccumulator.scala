package join

class MemoryUsageAwareDataAccumulator(primary: InMemoryDataAccumulator, secondary: FileDataAccumulator, oracle: Oracle)
    extends DataAccumulator[Any] { self =>

  private[this] var useSecondary: Boolean = false

  override def close(): Unit = {
    primary.close()
    secondary.close()
  }

  override def reset(): Unit = {
    primary.close()
    if (useSecondary)
      secondary.close()
  }

  override def currentKey: Option[Data#Key] =
    if (useSecondary) secondary.currentKey
    else primary.currentKey

  override def setKey(key: Data#Key): Unit = {
    secondary.setKey(key)
    primary.setKey(key)
  }

  override def writer: DataWriter[_, Data] = new DataWriter[Any, Data] {
    override def write(row: Data): Unit =
      if (useSecondary) secondary.writer.write(row)
      else if (!oracle.underMemoryPressure) primary.writer.write(row)
      else {
        useSecondary = true
        val w = secondary.writer
        try {
          val r = primary.reader
          try {
            r.iterator.foreach(w.write)
          } finally r.close()
        } finally w.close()
        primary.reset()
        w.write(row)
      }

    override def handle: Any =
      if (useSecondary) secondary.writer.handle
      else primary.writer.handle

    override def close(): Unit =
      if (useSecondary) secondary.close()
      else primary.close()
  }

  override def reader: DataReader[Any] =
    if (useSecondary) secondary.reader
    else primary.reader
}
