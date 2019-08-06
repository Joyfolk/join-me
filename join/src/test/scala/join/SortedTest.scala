package join
import java.io.{File, PrintWriter}

import join.Settings.{InnerJoin, LeftJoin}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Success

class SortedTest extends FunSuite {
  val fileIO = new FileIO

  Seq(new InMemoryDataAccumulator, new FileDataAccumulator(fileIO)).foreach { acc =>
    test(s"leftJoin: left join rows with same ids using ${acc.getClass.getSimpleName}") {
      assert(
        Sorted
          .leftJoin(
            Seq(Data(1L, "a1"), Data(2L, "a2"), Data(3L, "a3")).iterator,
            Seq(Data(1L, "b1"), Data(2L, "b2"), Data(3L, "b3")).iterator.buffered,
            acc
          )
          .toSeq === Seq(Result(1L, "a1", "b1"), Result(2L, "a2", "b2"), Result(3L, "a3", "b3")))

      assert(
        Sorted
          .leftJoin(
            Seq(Data(1L, "a1"), Data(2L, "a2"), Data(3L, "a3")).iterator,
            Seq(Data(1L, "b1"), Data(2L, "b2")).iterator.buffered,
            acc
          )
          .toSeq === Seq(Result(1L, "a1", "b1"), Result(2L, "a2", "b2"), Result(3L, "a3", null)))

      assert(
        Sorted
          .leftJoin(
            Seq(Data(1L, "a1"), Data(2L, "a2"), Data(3L, "a3")).iterator,
            Seq(Data(1L, "b1"), Data(3L, "b3")).iterator.buffered,
            acc
          )
          .toSeq === Seq(Result(1L, "a1", "b1"), Result(2L, "a2", null), Result(3L, "a3", "b3")))

      assert(
        Sorted
          .leftJoin(
            Seq(Data(1L, "a1"), Data(2L, "a2"), Data(3L, "a3")).iterator,
            Seq(Data(0L, "b0"), Data(3L, "b3"), Data(4L, "a4")).iterator.buffered,
            acc
          )
          .toSeq === Seq(Result(1L, "a1", null), Result(2L, "a2", null), Result(3L, "a3", "b3")))

    }

    test(s"leftJoin: correctly do one-to-many joins using ${acc.getClass.getSimpleName}") {
      assert(
        Sorted
          .leftJoin(
            Seq(Data(1L, "a1"), Data(2L, "a2"), Data(3L, "a3")).iterator,
            Seq(Data(1L, "b1"), Data(2L, "b2"), Data(2L, "b2a")).iterator.buffered,
            acc
          )
          .toSeq === Seq(Result(1L, "a1", "b1"),
                         Result(2L, "a2", "b2"),
                         Result(2L, "a2", "b2a"),
                         Result(3L, "a3", null)))

      assert(
        Sorted
          .leftJoin(
            Seq(Data(1L, "a1"), Data(2L, "a2"), Data(3L, "a3")).iterator,
            Seq(Data(1L, "b1"), Data(1L, "b1a"), Data(2L, "b2"), Data(2L, "b2a")).iterator.buffered,
            acc
          )
          .toSeq === Seq(Result(1L, "a1", "b1"),
                         Result(1L, "a1", "b1a"),
                         Result(2L, "a2", "b2"),
                         Result(2L, "a2", "b2a"),
                         Result(3L, "a3", null)))

    }

    test(s"leftJoin: correctly do many-to-many joins using ${acc.getClass.getSimpleName}") {
      assert(
        Sorted
          .leftJoin(
            Seq(Data(1L, "a1"), Data(1L, "a1a"), Data(2L, "a2"), Data(3L, "a3")).iterator,
            Seq(Data(1L, "b1"), Data(2L, "b2"), Data(2L, "b2a")).iterator.buffered,
            acc
          )
          .toSeq === Seq(Result(1L, "a1", "b1"),
                         Result(1L, "a1a", "b1"),
                         Result(2L, "a2", "b2"),
                         Result(2L, "a2", "b2a"),
                         Result(3L, "a3", null)))

      assert(
        Sorted
          .leftJoin(
            Seq(Data(1L, "a1"), Data(1L, "a1a"), Data(2L, "a2"), Data(3L, "a3")).iterator,
            Seq(Data(1L, "b1"), Data(1L, "b1a"), Data(2L, "b2"), Data(2L, "b2a")).iterator.buffered,
            acc
          )
          .toSeq === Seq(
          Result(1L, "a1", "b1"),
          Result(1L, "a1", "b1a"),
          Result(1L, "a1a", "b1"),
          Result(1L, "a1a", "b1a"),
          Result(2L, "a2", "b2"),
          Result(2L, "a2", "b2a"),
          Result(3L, "a3", null)
        ))
    }
  }

  test("doJoin: test left and inner joins") {
    object MockDataWriter extends DataWriter[Unit, Result] {
      val buf: ArrayBuffer[Result] = new ArrayBuffer[Result]()
      override def write(r: Result): Unit = buf.append(r)
      override def handle: Unit = ()
      override def close(): Unit = {}
    }
    Sorted.doJoin(
      Seq(Data(1L, "a1"), Data(1L, "a1a"), Data(2L, "a2"), Data(3L, "a3"), Data(4L, "a4")).iterator,
      Seq(Data(0L, "b0"), Data(2L, "b2"), Data(2L, "b2a")).iterator.buffered,
      InnerJoin,
      MockDataWriter,
      new InMemoryDataAccumulator
    )
    assert(MockDataWriter.buf == Seq(Result(2L, "a2", "b2"), Result(2L, "a2", "b2a")))

    MockDataWriter.buf.clear()
    Sorted.doJoin(
      Seq(Data(1L, "a1"), Data(1L, "a1a"), Data(2L, "a2"), Data(3L, "a3"), Data(4L, "a4")).iterator,
      Seq(Data(0L, "b0"), Data(2L, "b2"), Data(2L, "b2a")).iterator.buffered,
      LeftJoin,
      MockDataWriter,
      new InMemoryDataAccumulator
    )
    assert(
      MockDataWriter.buf == Seq(Result(1L, "a1", null),
                                Result(1L, "a1a", null),
                                Result(2L, "a2", "b2"),
                                Result(2L, "a2", "b2a"),
                                Result(3L, "a3", null),
                                Result(4L, "a4", null)))
  }

  def createTempFileAndPrint(lines: Seq[String]): File = {
    val f = File.createTempFile("test_", ".csv")
    f.deleteOnExit()
    val w = new PrintWriter(f)
    try {
      lines.foreach(w.println)
      w.flush()
    } finally w.close()
    f
  }

  test("join: basic file join") {
    implicit val dataIO: FileIO = new FileIO
    val left = createTempFileAndPrint(Seq("1\tMoscow", "2\tParis", "3\tLondon"))
    val right = createTempFileAndPrint(Seq("1\tLondon", "2\tMoscow", "3\tParis"))
    val output = createTempFileAndPrint(Seq())
    assert(Sorted.join(left, right, InnerJoin, output) == Success(()))
    val s = Source.fromFile(output, "UTF8")
    try {
      assert(s.getLines().toSeq == Seq("1\tMoscow\tLondon", "2\tParis\tMoscow", "3\tLondon\tParis"))
    } finally s.close()
  }

  test("join: empty right file join") {
    implicit val dataIO: FileIO = new FileIO
    val left = createTempFileAndPrint(Seq("1\tMoscow", "2\tParis", "3\tLondon"))
    val right = createTempFileAndPrint(Seq())
    val output = createTempFileAndPrint(Seq())
    assert(Sorted.join(left, right, LeftJoin, output) == Success(()))
    val s = Source.fromFile(output, "UTF8")
    try {
      assert(s.getLines().toSeq == Seq("1\tMoscow\t", "2\tParis\t", "3\tLondon\t"))
    } finally s.close()
  }

  test("join: empty left file join") {
    implicit val dataIO: FileIO = new FileIO
    val left = createTempFileAndPrint(Seq())
    val right = createTempFileAndPrint(Seq("1\tLondon", "2\tMoscow", "3\tParis"))
    val output = createTempFileAndPrint(Seq())
    assert(Sorted.join(left, right, LeftJoin, output) == Success(()))
    val s = Source.fromFile(output, "UTF8")
    try {
      assert(s.getLines().toSeq == Seq())
    } finally s.close()
  }
}
