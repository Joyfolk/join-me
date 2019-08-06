package join
import java.io.Closeable

trait DataReader[+H] extends Closeable {
  def iterator: BufferedIterator[Data]
  def handle: H
}
