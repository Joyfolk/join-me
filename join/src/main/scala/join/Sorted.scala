package join
import java.io.Closeable

import join.Settings.JoinType

import scala.util.Try

object Sorted {

  def join[H](left: H, right: H, joinType: JoinType, output: H)(implicit dataIO: DataIO[H]): Try[Unit] = {
    val lReader = Try(dataIO.reader(left))
    val rReader = Try(dataIO.reader(right))
    val writer = Try(dataIO.resultWriter(output))
    val result = for {
      l <- lReader
      r <- rReader
      w <- writer
      output <- doJoin(l.iterator, r.iterator, joinType, w, dataIO.accumulator)
    } yield output
    lReader.foreach(dataIO.close)
    rReader.foreach(dataIO.close)
    writer.foreach(dataIO.close)
    result
  }

  private[join] def doJoin[H](left: Iterator[Data],
                              right: BufferedIterator[Data],
                              joinType: JoinType,
                              writer: DataWriter[H, Result],
                              acc: DataAccumulator[_]): Try[Unit] = Try {
    val iter = leftJoin(left, right, acc)
    try {
      joinType match {
        case Settings.LeftJoin  => iter.foreach(writer.write)
        case Settings.InnerJoin => iter.filter(_.right != null).foreach(writer.write)
      }
    } finally iter.close()
  }

  private[join] def leftJoin(left: Iterator[Data], right: BufferedIterator[Data], acc: DataAccumulator[_]) =
    new LeftJoinIterator(left, right, acc)

  private[this] sealed trait IteratorMode
  private[this] case object ReadLeft extends IteratorMode
  private[this] case class ReadRight(l: Data, iterator: Iterator[Data]) extends IteratorMode

  private[join] class LeftJoinIterator(left: Iterator[Data], right: BufferedIterator[Data], acc: DataAccumulator[_])
      extends Iterator[Result]
      with Closeable {
    private[this] var mode: IteratorMode = ReadLeft

    override def hasNext: Boolean =
      mode match {
        case ReadLeft           => left.hasNext
        case ReadRight(_, iter) => iter.hasNext || left.hasNext
      }

    override def next(): Result = mode match {
      case ReadLeft =>
        val l = left.next()
        if (acc.currentKey.contains(l.key)) {
          val iter = acc.reader.iterator
          val r = iter.next()
          mode = ReadRight(l, iter)
          Result(l.key, l.value, r.value)
        } else if (right.hasNext) {
          val cnt = acc.accumulate(right, l.key)
          if (cnt > 0) {
            val iter = acc.reader.iterator
            val r = iter.next()
            mode = ReadRight(l, iter)
            Result(l.key, l.value, r.value)
          } else {
            mode = ReadLeft
            Result(l.key, l.value, null)
          }
        } else {
          mode = ReadLeft
          Result(l.key, l.value, null)
        }
      case ReadRight(_, iter) if !iter.hasNext =>
        mode = ReadLeft
        next()
      case ReadRight(l, iter) =>
        val r = iter.next()
        Result(l.key, l.value, r.value)
    }

    override def close(): Unit =
      acc.close()
  }
}
