package join
import java.io.File
import java.util.concurrent.Executors

import join.Settings.JoinType

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object Main extends App {
  implicit val dataIO: FileIO = new FileIO
  type Data = (Long, String)

  Settings.read(args) match {
    case Left(error) =>
      System.err.println(s"Error: $error")
      System.exit(-1)
    case Right(settings) =>
      System.out.println(s"Starting job: $settings")
      val executor = Executors.newFixedThreadPool(settings.threads)
      implicit val ec: ExecutionContext =
        ExecutionContext.fromExecutorService(executor)
      val res = Await.result(startJob(settings), Duration.Inf)
      executor.shutdown()
      res match {
        case Failure(ex) =>
          System.err.println(s"Internal error: ${ex.getMessage}")
          ex.printStackTrace(System.err)
          System.exit(-2)
        case Success(file) =>
          System.out.println(s"Resulting file: $file")
          System.exit(0)
      }
  }

  def sort(file: File)(implicit ec: ExecutionContext): Future[File] = Future {
    implicit val oracle: Oracle = Oracle()
    val target = File.createTempFile("sort_", ".csv")
    target.deleteOnExit()
    ExternalSort.sort(file, target)
    target
  }

  def join(left: File, right: File, joinType: JoinType, output: File)(
    implicit ec: ExecutionContext
  ): Future[Try[File]] =
    Future {
      Sorted.join(left, right, joinType, output).map(_ => output)
    }

  def startJob(
    settings: Settings
  )(implicit ec: ExecutionContext): Future[Try[File]] = {
    val leftSorted = sort(settings.left)
    val rightSorted = sort(settings.right)
    for {
      l <- leftSorted
      r <- rightSorted
      output = settings.result.getOrElse(
        File.createTempFile("join_result_", ".csv")
      )
      j <- join(l, r, settings.jointType, output)
    } yield j
  }
}
