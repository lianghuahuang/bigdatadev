/**
 * ${DESCRIPTION}
 *
 * @author lianghuahuang
 * @date 2021/8/24
 *
 * */
object SparkDistCPOptions {

  object Defaults {
    val ignoreFailures:Boolean = false;
    //控制同时copy的最大并发task数
    val maxConcurrence:Int = 2;
  }
}

case class SparkDistCPOptions(ignoreFailures:Boolean = SparkDistCPOptions.Defaults.ignoreFailures,
                              maxConcurrence:Int = SparkDistCPOptions.Defaults.maxConcurrence){

  def validateOptions(): Unit = {
    assert(maxConcurrence > 0, "maxConcurrence must be positive")
  }

}
