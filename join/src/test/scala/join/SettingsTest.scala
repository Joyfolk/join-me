package join
import java.io.File

import join.Settings.{InnerJoin, LeftJoin}
import org.scalatest.FunSuite

class SettingsTest extends FunSuite {
  test("parse required arguments in any order") {
    assert(
      Settings.read(
        Array("src/test/resources/left.csv", "src/test/resources/right.csv")
      ) ===
        Right(
          Settings(
            Runtime.getRuntime.availableProcessors(),
            InnerJoin,
            new File("src/test/resources/left.csv"),
            new File("src/test/resources/right.csv"),
            None
          )
        )
    )
  }

  test("error if not enough required arguments") {
    assert(Settings.read(Array("src/test/resources/left.csv")).isLeft)
  }

  test("error if files not exist") {
    assert(
      Settings
        .read(
          Array(
            "src/test/resources/left.not-exist",
            "src/test/resources/right.not-exist"
          )
        )
        .isLeft
    )
  }

  test("parse optional arguments") {
    assert(
      Settings.read(
        Array(
          "--threads",
          "132",
          "src/test/resources/left.csv",
          "src/test/resources/right.csv"
        )
      ) ===
        Right(
          Settings(
            132,
            InnerJoin,
            new File("src/test/resources/left.csv"),
            new File("src/test/resources/right.csv"),
            None
          )
        )
    )
    assert(
      Settings.read(
        Array(
          "--join-type",
          "left",
          "src/test/resources/left.csv",
          "src/test/resources/right.csv"
        )
      ) ===
        Right(
          Settings(
            Runtime.getRuntime.availableProcessors(),
            LeftJoin,
            new File("src/test/resources/left.csv"),
            new File("src/test/resources/right.csv"),
            None
          )
        )
    )
    assert(
      Settings
        .read(
          Array(
            "--join-type",
            "left",
            "--threads",
            "164",
            "src/test/resources/left.csv",
            "src/test/resources/right.csv"
          )
        ) ===
        Right(
          Settings(
            164,
            LeftJoin,
            new File("src/test/resources/left.csv"),
            new File("src/test/resources/right.csv"),
            None
          )
        )
    )
    assert(
      Settings
        .read(
          Array(
            "--join-type",
            "left",
            "--threads",
            "164",
            "--result",
            "/tmp/result.csv",
            "src/test/resources/left.csv",
            "src/test/resources/right.csv"
          )
        ) ===
        Right(
          Settings(
            164,
            LeftJoin,
            new File("src/test/resources/left.csv"),
            new File("src/test/resources/right.csv"),
            Some(new File("/tmp/result.csv"))
          )
        )
    )
  }
}
