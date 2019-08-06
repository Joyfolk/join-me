package join
import java.io.{File, PrintWriter}
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class ExternalSortTest extends FunSuite {
  implicit val ord: Ordering[Data] = Ordering.by[Data, Data#Key](_.key).reverse
  implicit val dataIO: FileIO = new FileIO

  test("splitSort: Sort single file") {
    implicit val oracle: Oracle = Oracle()
    val f = File.createTempFile("test_", ".csv")
    val w = new PrintWriter(f)
    try {
      w.println("5\tLondon")
      w.println("4\tMoscow")
      w.println("3\tParis")
      w.println("42\tMagadan")
    } finally w.close()
    val res = ExternalSort.splitSort(f)
    assert(res.size == 1)
    val s = Source.fromFile(res(0).handle)
    try {
      assert(
        s.getLines().map(Row.fromString).toSeq == Seq(Data(3L, "Paris"),
                                                      Data(4L, "Moscow"),
                                                      Data(5L, "London"),
                                                      Data(42L, "Magadan")))
    } finally s.close
  }

  class MockDataWriter extends DataWriter[Unit, Data] {
    val buf: ArrayBuffer[Data] = ArrayBuffer()
    override def write(r: Data): Unit = buf.append(r)
    override def handle: Unit = ()
    override def close(): Unit = {}
  }

  class MockDataReader(val s: Seq[Data]) extends DataReader[Unit] {
    override def iterator: BufferedIterator[Data] = s.iterator.buffered
    override def handle: Unit = ()
    override def close(): Unit = {}
  }

  test("joinSort") {
    val w = new MockDataWriter
    ExternalSort.joinSort(
      IndexedSeq(
        new MockDataReader(Seq(Data(1L, "a1"), Data(5L, "a5"), Data(9L, "a9"))),
        new MockDataReader(Seq(Data(2L, "b2"), Data(3L, "b3"), Data(4L, "b4"), Data(5L, "b5"))),
        new MockDataReader(Seq(Data(5L, "c5"))),
        new MockDataReader(Seq())
      ),
      w
    )
    assert(
      w.buf == Seq(
        Data(1L, "a1"),
        Data(2L, "b2"),
        Data(3L, "b3"),
        Data(4L, "b4"),
        Data(5L, "a5"),
        Data(5L, "b5"),
        Data(5L, "c5"),
        Data(9L, "a9")
      ))
  }

  test("splitSort: using oracle") {
    implicit val oracle: Oracle = new Oracle {
      val counter: AtomicInteger = new AtomicInteger(0)
      override def underMemoryPressure: Boolean = counter.incrementAndGet() == 2
    }

    val f = File.createTempFile("test_", ".csv")
    val w = new PrintWriter(f)
    try {
      w.println("5\tLondon")
      w.println("4\tMoscow")
      w.println("3\tParis")
      w.println("42\tMagadan")
    } finally w.close()
    val res = ExternalSort.splitSort(f)
    assert(res.size == 2)
    var s = Source.fromFile(res(0).handle)
    try {
      assert(s.getLines().map(Row.fromString).toSeq == Seq(Data(4L, "Moscow"), Data(5L, "London")))
      s.close()
      s = Source.fromFile(res(1).handle)
      assert(s.getLines().map(Row.fromString).toSeq == Seq(Data(3L, "Paris"), Data(42L, "Magadan")))
    } finally s.close
  }

  test("externalSort: sort") {
    implicit val oracle: Oracle = new Oracle {
      val counter: AtomicInteger = new AtomicInteger(0)
      override def underMemoryPressure: Boolean = counter.incrementAndGet() == 2
    }

    val f = File.createTempFile("test_", ".csv")
    val w = new PrintWriter(f)
    try {
      w.println("5\tLondon")
      w.println("4\tMoscow")
      w.println("3\tParis")
      w.println("42\tMagadan")
    } finally w.close()
    val t = File.createTempFile("test_res_", ".csv")
    ExternalSort.sort(f, t)
    val s = Source.fromFile(t)
    try {
      assert(
        s.getLines().map(Row.fromString).toSeq == Seq(Data(3L, "Paris"),
                                                      Data(4L, "Moscow"),
                                                      Data(5L, "London"),
                                                      Data(42L, "Magadan")))
    } finally s.close

  }
}
