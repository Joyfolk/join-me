package join
import java.io.File

class FileDataAccumulator(dataIO: DataIO[File]) extends DataAccumulator[File] {
  private[this] var dataWriter: Option[DataWriter[File, Data]] = None
  private[this] var dataReader: Option[DataReader[File]] = None
  private[this] var key: Option[Data#Key] = None

  override def close(): Unit = {
    dataWriter.foreach(_.close())
    dataReader.foreach(_.close())
  }

  override def reset(): Unit = {
    close()
    dataReader = None
    dataWriter = None
    key = None
  }

  override def currentKey: Option[Data#Key] = key

  override def setKey(key: Data#Key): Unit = this.key = Some(key)

  override def writer: DataWriter[File, Data] = {
    dataWriter.foreach(_.close())
    dataWriter = Some(dataIO.temporaryWriter)
    dataWriter.get
  }

  override def reader: DataReader[File] = {
    require(dataWriter.nonEmpty, "DataWriter should exist")
    val handle = dataWriter.get.handle
    dataWriter.foreach(_.close())
    dataReader.foreach(_.close())
    dataReader = Some(dataIO.reader(handle))
    dataReader.get
  }

}
