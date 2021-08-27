import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * ${DESCRIPTION}
 *
 * @author lianghuahuang
 * @date 2021/8/24
 *
 * */
object SparkDistCPOptionsParser {
  def parse(args:Array[String],hadoopConfiguration: Configuration):Config={
    val parser = new scopt.OptionParser[Config]("") {
      opt[Unit]("i")
        .action((_, c) => c.copyOptions(_.copy(ignoreFailures = true)))
        .text("Ignore failures")

      opt[Int]("m")
        .action((i, c) => c.copyOptions(_.copy(maxConcurrence = i)))
        .text("Maximum Concurrence number of task to copy")

      arg[String]("<source_path> <dest_path>")
        .unbounded().minOccurs(2).maxOccurs(2)
        .optional()
        .action((x, c) => c.copy(uris = c.uris :+ new URI(x)))
    }
    parser.parse(args, Config()) match {
      case Some(config) =>
        config.options.validateOptions()
        config
      case _ =>
        throw new RuntimeException("Failed to parse arguments")
    }
  }


}

case class Config(options: SparkDistCPOptions = SparkDistCPOptions(), uris: Seq[URI] = Seq.empty) {

  def copyOptions(f: SparkDistCPOptions => SparkDistCPOptions): Config = {
    this.copy(options = f(options))
  }



}
