import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * ${DESCRIPTION}
 *
 * @author lianghuahuang
 * @date 2021/8/23
 *
 * */
object SparkDistCP {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val spark = SparkSession.builder()
      .master("local[*]").appName("SparkDistCp")
      .config("spark.driver.host","192.168.0.105")
      .config("spark.driver.bindAddress","192.168.0.105")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    @transient   val sc = spark.sparkContext;
    @transient   val srcPath: Path =new Path("hdfs://master:9000/aa");
    @transient   val destPath: Path = new Path("hdfs://master:9000/cc")
    val maxCurrence =2
    sc.hadoopConfiguration.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive",true)
    @transient val sfileSystem:FileSystem = srcPath.getFileSystem(sc.hadoopConfiguration)
    @transient val dfileSystem:FileSystem = destPath.getFileSystem(sc.hadoopConfiguration)
    checkDirectories(srcPath,destPath,sfileSystem,dfileSystem);
    val fileIter = srcPath.getFileSystem(sc.hadoopConfiguration).listFiles(srcPath,true);
    val fileList = new mutable.MutableList[(String,String)]
    while(fileIter.hasNext){
      val f = fileIter.next();
      val subPath  = f.getPath.toString.split(srcPath.toString())(1)
      val s =  destPath.toString + subPath
      fileList.+=:(f.getPath.toString,s)
    }

    val fileRDD = sc.makeRDD(fileList,maxCurrence)

    fileRDD.mapPartitions(v=>transfrom(v, sfileSystem, dfileSystem,sc.hadoopConfiguration)).collect().foreach(println)
  }

  def transfrom(v: Iterator[(String,String)],sfileSystem:FileSystem,dfileSystem:FileSystem,hadoopConfiguration: Configuration): Iterator[(String,Boolean)] = {
    val list = new mutable.MutableList[(String,Boolean)]
    while(v.hasNext){
      val p = v.next()
      val result = FileUtil.copy(sfileSystem,new Path(p._1),dfileSystem,new Path(p._2),false,hadoopConfiguration)
      list.+=:(p._2,result)
    }
    list.iterator
  }

/*  def run(sparkSession: SparkSession,srcPath:Path,distPath:Path,options:SparkDistCPOptions): Unit ={
    options.validateOptions()
    sparkSession.sparkContext.hadoopConfiguration.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive",true)
    val qualifiedSrcPaths = pathToQualifiedPath(sparkSession.sparkContext.hadoopConfiguration, srcPath)
    val qualifiedDestPath = pathToQualifiedPath(sparkSession.sparkContext.hadoopConfiguration, distPath)
  }

  def pathToQualifiedPath(hadoopConfiguration: Configuration, path: Path): Path = {
    val fs = FileSystem.get(hadoopConfiguration)
    path.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }*/


  /**
   * 创建目标目录文件夹
   * @param srcPath
   * @param destPath
   * @return
   */
  def checkDirectories(srcPath:Path,destPath:Path,sfileSystem:FileSystem,dfileSystem:FileSystem): ListBuffer[String] ={
    //val list = new mutable.MutableList[(String,String)]
    val fileList  =new ListBuffer[String]()
    sfileSystem.listStatus(srcPath).foreach(f => {
      if(f.isDirectory()){
        val subPath  = f.getPath.toString.split(srcPath.toString())(1)
        val s =  destPath.toString + subPath
        dfileSystem.mkdirs(new Path(s))
        val p = new Path(destPath + subPath)
        checkDirectories(f.getPath,p,sfileSystem, dfileSystem)
      }else{
      //  list.+=: (f.getPath.toString,destPath.toString)
        fileList.append(f.getPath.toString)
      }
    })
    println(fileList)
    fileList
  }
}
