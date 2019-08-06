package join
import java.io.File

import join.Settings.JoinType

import scala.util.Try

case class Settings(threads: Int, jointType: JoinType, left: File, right: File)

object Settings {
  sealed trait JoinType
  case object LeftJoin extends JoinType
  case object InnerJoin extends JoinType

  lazy val defaultParallelism: Int = Runtime.getRuntime.availableProcessors()

  def read(args: Array[String], defaultParallelism: Int = defaultParallelism): Either[String, Settings] = {
    case class SettingsOpt(threads: Option[Int] = None,
                           joinType: Option[JoinType] = None,
                           left: Option[File] = None,
                           right: Option[File] = None) {
      def settings: Either[String, Settings] =
        for {
          l <- left.toRight("required option leftFile")
          r <- right.toRight("required option rightFile")
        } yield Settings(threads.getOrElse(defaultParallelism), joinType.getOrElse(InnerJoin), l, r)
    }

    def parseUnsigned(n: String): Either[String, Int] =
      Try(Integer.parseInt(n)).toEither.left.map(_.getMessage)

    def exist(filename: String): Boolean = new File(filename).exists()

    def _read(parsed: SettingsOpt, args: List[String]): Either[String, SettingsOpt] = args match {
      case "--threads" :: n :: xs =>
        parseUnsigned(n)
          .map(threads => parsed.copy(threads = Some(threads)))
          .flatMap(s => _read(s, xs))
      case "--join-type" :: "left" :: xs =>
        _read(parsed.copy(joinType = Some(LeftJoin)), xs)
      case "--join-type" :: "inner" :: xs =>
        _read(parsed.copy(joinType = Some(InnerJoin)), xs)
      case leftFile :: rightFile :: Nil if exist(leftFile) && exist(rightFile) =>
        Right(parsed.copy(left = Some(new File(leftFile)), right = Some(new File(rightFile))))
      case leftFile :: rightFile :: Nil =>
        Left(s"Join files not accessible: $leftFile, $rightFile")
      case unknown :: _ =>
        Left(s"Unknown command line argument: $unknown")
      case Nil =>
        Left(
          "Usage: [--threads <threadCount> [= availableProcessors()]] [--join-type inner|left [= inner]] " +
            "leftFile rightFile"
        )
    }

    _read(SettingsOpt(), args.toList).flatMap(_.settings)
  }
}
