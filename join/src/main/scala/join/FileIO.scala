package join
import java.io.{Closeable, File, PrintWriter}

import scala.io.{BufferedSource, Source}
import scala.util.control.NonFatal

class FileIO extends DataIO[File] {
  override def reader(h: File): DataReader[File] = new DataReader[File] {
    def source: BufferedSource = Source.fromFile(handle, "UTF8")

    override def handle: File = h
    override def close(): Unit = source.close
    override def iterator: BufferedIterator[Data] = source.getLines().map(Row.fromString).buffered
  }

  override def resultWriter(h: File): DataWriter[File, Result] = new DataWriter[File, Result] {
    val writer = new PrintWriter(h)

    override def write(r: Result): Unit = writer.println(r.asString)
    override def handle: File = h
    override def close(): Unit = {
      writer.flush()
      writer.close()
    }
  }

  override def dataWriter(h: File): DataWriter[File, Data] =
    new DataWriter[File, Data] {
      val writer = new PrintWriter(h)

      override def write(r: Data): Unit = writer.println(r.asString)
      override def handle: File = h
      override def close(): Unit = {
        writer.flush()
        writer.close()
      }
    }

  override def temporaryWriter: DataWriter[File, Data] = new DataWriter[File, Data] {
    override val handle: File = {
      val f = File.createTempFile("join_", ".csv")
//      f.deleteOnExit()
      f
    }

    val writer = new PrintWriter(handle)

    override def write(r: Data): Unit =
      writer.println(r.asString)

    override def close(): Unit =
      writer.close()
  }

  override def close(c: Closeable): Unit =
    try {
      c.close()
    } catch {
      case NonFatal(_) => // ignored
    }

  override def accumulator: DataAccumulator[File] = new FileDataAccumulator(this)
}
