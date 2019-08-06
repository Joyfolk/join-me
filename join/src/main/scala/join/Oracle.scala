package join
import scala.ref.SoftReference

trait Oracle {
  def underMemoryPressure: Boolean
}

object Oracle {

  def apply(): Oracle = new Oracle() {
    val softReference: SoftReference[Object] = new SoftReference[Object](new Object)
    override def underMemoryPressure: Boolean = softReference.underlying.get == null
  }
}
