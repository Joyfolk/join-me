package join
import scala.collection.mutable.ArrayBuffer

class InMemoryDataAccumulator extends DataAccumulator[Unit] {
  private[this] val buffer = ArrayBuffer[Data]()
  private[this] var key: Option[Data#Key] = None

  override def close(): Unit = {}

  override def reset(): Unit = {
    buffer.clear()
    key = None
  }

  override def currentKey: Option[Data#Key] = key

  override def setKey(key: Data#Key): Unit = this.key = Some(key)

  override def writer: DataWriter[Unit, Data] = new DataWriter[Unit, Data] {
    override def write(r: Data): Unit = {
      key = Some(r.key)
      buffer.append(r)
    }

    override def handle: Unit = ()
    override def close(): Unit = {}
  }

  override def reader: DataReader[Unit] = new DataReader[Unit] {
    override def iterator: BufferedIterator[Data] = buffer.iterator.buffered
    override def handle: Unit = ()
    override def close(): Unit = {}
  }
}
