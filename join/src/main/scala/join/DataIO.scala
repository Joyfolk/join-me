package join
import java.io.Closeable

trait DataIO[H] {
  def reader(handle: H): DataReader[H]
  def resultWriter(handle: H): DataWriter[H, Result]
  def dataWriter(handle: H): DataWriter[H, Data]
  def temporaryWriter: DataWriter[H, Data]
  def accumulator: DataAccumulator[H]
  def close(c: Closeable)
}
