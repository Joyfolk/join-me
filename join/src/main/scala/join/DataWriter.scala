package join
import java.io.Closeable

trait DataWriter[+H, R <: Row[_, _]] extends Closeable {
  def write(r: R): Unit
  def handle: H
}
