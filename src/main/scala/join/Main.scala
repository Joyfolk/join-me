package join
import java.io.File
import java.util.concurrent.Executors

import join.Settings.JoinType

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object Main extends App {
  implicit val dataIO: FileIO = new FileIO
  type Data = (Long, String)

  Settings.read(args) match {
    case Left(error) =>
      System.err.println(s"Error: $error")
    case Right(settings) =>
      System.out.println(s"Starting job: $settings")
      implicit val ec: ExecutionContext =
        ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(settings.threads))
      val res = Await.result(startJob(settings), Duration.Inf)
      System.out.println(s"Resulting file: $res")
      System.exit(0)
  }

  def sort(file: File)(implicit ec: ExecutionContext): Future[File] = Future {
    implicit val oracle: Oracle = Oracle()
    val target = File.createTempFile("sort_", ".csv")
    target.deleteOnExit()
    ExternalSort.sort(file, target)
    target
  }

  def join(left: File, right: File, joinType: JoinType)(implicit ec: ExecutionContext): Future[File] =
    Future {
      val output = File.createTempFile("join_result_", ".csv")
      Sorted.join(left, right, joinType, output)
      output
    }

  def startJob(settings: Settings)(implicit ec: ExecutionContext): Future[File] = {
    val leftSorted = sort(settings.left)
    val rightSorted = sort(settings.right)
    for {
      l <- leftSorted
      r <- rightSorted
      j <- join(l, r, settings.jointType)
    } yield j
  }
}
