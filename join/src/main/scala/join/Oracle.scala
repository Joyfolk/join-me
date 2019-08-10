package join

trait Oracle {
  def underMemoryPressure: Boolean
}

object Oracle {
  private[this] val runtime: Runtime = Runtime.getRuntime
  val maxMemory: Long = runtime.maxMemory()

  def apply(): Oracle = new Oracle() {
    override def underMemoryPressure: Boolean =
      runtime.freeMemory() < maxMemory / 5
  }
}
