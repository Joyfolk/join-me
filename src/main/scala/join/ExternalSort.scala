package join
import scala.collection.mutable

object ExternalSort {
  implicit val ord: Ordering[Data] = Row.ordering[Data#Key, Data].reverse
  implicit val oracle: Oracle = null

  def sort[H](source: H, target: H)(implicit dataIO: DataIO[H]): Unit =
    joinSort(splitSort(source), dataIO.dataWriter(target))

  private[join] def joinSort[H](parts: IndexedSeq[DataReader[H]], output: DataWriter[H, Data])(
      implicit ordData: Ordering[Data]): Unit = {
    implicit val ord: Ordering[(Data, Int)] = Ordering.by(_._1)
    val heap = mutable.PriorityQueue[(Data, Int)]()
    val iterators = parts.map(_.iterator)
    iterators.zipWithIndex.foreach {
      case (iter, i) if iter.hasNext =>
        heap.enqueue((iter.next(), i))
      case _ => // ignore
    }
    while (heap.nonEmpty) {
      val (d, i) = heap.dequeue()
      output.write(d)
      if (iterators(i).hasNext)
        heap.enqueue((iterators(i).next(), i))
    }
  }

  private[this] def dumpHeap[H](heap: mutable.PriorityQueue[Data])(implicit dataIO: DataIO[H]): DataWriter[H, Data] = {
    val w = dataIO.temporaryWriter
    try {
      w.writeAll(heap.dequeueAll)
      System.gc()
      w
    } finally w.close()
  }

  private[join] def splitSort[H](
      source: H)(implicit dataIO: DataIO[H], oracle: Oracle, ord: Ordering[Data]): IndexedSeq[DataReader[H]] = {
    val heap = mutable.PriorityQueue[Data]()
    var parts = IndexedSeq[DataWriter[H, Data]]()
    val reader = dataIO.reader(source)
    try {
      reader.iterator.foreach { d =>
        if (!oracle.underMemoryPressure)
          heap.enqueue(d)
        else {
          val w = dumpHeap(heap)
          parts = parts :+ w
        }
      }
      if (heap.nonEmpty)
        parts = parts :+ dumpHeap(heap)
      parts.map(_.handle).map(dataIO.reader)
    } finally reader.close()
  }
}
