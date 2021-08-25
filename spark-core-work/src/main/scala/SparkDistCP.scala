import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * ${DESCRIPTION}
 *
 * @author lianghuahuang
 * @date 2021/8/23
 *
 * */
object SparkDistCP {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]").appName("SparkDistCp")
      .config("spark.driver.host","192.168.0.105")
      .config("spark.driver.bindAddress","192.168.0.105")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext;
    val srcPath: Path =new Path("hdfs://master:9000/aa/bb");
    val destPath: Path = new Path("hdfs://master:9000/cc")
    val maxCurrence =2
    sc.hadoopConfiguration.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive",true)
    val fileSystem:FileSystem = srcPath.getFileSystem(sc.hadoopConfiguration)
    val fileList  = checkDirectories(srcPath,destPath,fileSystem);


    val fileRDD = sc.makeRDD(fileList,maxCurrence)
    fileRDD.mapPartitions(v=>{
      val list = new mutable.MutableList[(String,Boolean)]
      while(v.hasNext){
        val p = v.next()
        val result = FileUtil.copy(fileSystem,new Path(p._1),fileSystem,new Path(p._2),false,sc.hadoopConfiguration)
        list.+=:(p._1,result)
      }
      return list
    }).collect().foreach(println)
  }

  def run(sparkSession: SparkSession,srcPath:Path,distPath:Path,options:SparkDistCPOptions): Unit ={
    options.validateOptions()
    sparkSession.sparkContext.hadoopConfiguration.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive",true)
    val qualifiedSrcPaths = pathToQualifiedPath(sparkSession.sparkContext.hadoopConfiguration, srcPath)
    val qualifiedDestPath = pathToQualifiedPath(sparkSession.sparkContext.hadoopConfiguration, distPath)
  }

  def pathToQualifiedPath(hadoopConfiguration: Configuration, path: Path): Path = {
    val fs = FileSystem.get(hadoopConfiguration)
    path.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }


  /**
   * 创建目标目录文件夹
   * @param srcPath
   * @param destPath
   * @return
   */
  def checkDirectories(srcPath:Path,destPath:Path,fileSystem:FileSystem): mutable.MutableList[(String,String)] ={
    val list = new mutable.MutableList[(String,String)]
    fileSystem.listStatus(destPath).foreach(f => {
      if(f.isDirectory()){
        val subPath  = f.getPath.toString.split(srcPath.toString())(1)
        val s = srcPath.toUri + destPath.toString + subPath
        fileSystem.mkdirs(new Path(s))
        val p = new Path(destPath + subPath)
        checkDirectories(f.getPath,p,fileSystem)
      }else{
        list.+=: (f.getPath.toString,destPath.toString)
      }
    })
    list
  }
}
