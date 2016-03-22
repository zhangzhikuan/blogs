# spark输出文件到HDFS
---
1. Spark通过LzopOutputstream的API写HDFS

	```
	 def main(args: Array[String]) {
	    //保存的路径
	    val basePath = "/tmp/kuan2"
	    //设置日志级别
	    //    Example.setStreamingLogLevels()
	    //创建sparkConf
	    val sparkConf = new SparkConf().setAppName("runJob")
	    //设置master,此处设置本地执行
	    //    sparkConf.setMaster("local[*]")
	    //创建SparkContext
	    val sc = new SparkContext(sparkConf)
	    /**
	    val hadoopRDD = sc.newAPIHadoopFile[LongWritable, Text, LzoTextInputFormat]("/tmp/mls.nginx201601281705",
	      classOf[LzoTextInputFormat],
	      classOf[LongWritable],
	      classOf[Text])
	    val rdd:RDD[String] = hadoopRDD.map(x=>x._2.toString)
	      * */
	    val rdd = sc.makeRDD(List("1", "2", "会议", "1", "2", "会议", "1", "2", "会议", "1", "2", "会议"), 2)
	
	    //在每个executor上执行的函数
	    //此处定义的是,针对每个分区,我们把计算好的结果写入到本地目录中
	    val func = (tc: TaskContext, it: Iterator[String]) => {
	      //输出文件路径
	      val outFilePath: String =
	        s"""$basePath/${tc.partitionId()}.lzo"""
	      val outIndexFilePath = s"""${basePath}/${tc.partitionId()}.index"""
	
	      //****************开始往文件中写数据********************//
	      //得到文件系统
	      val fs: FileSystem = FileSystem.get(new Configuration)
	      //目标路径
	      val dstPath = new Path(outFilePath)
	      val dstIndexPath = new Path(outIndexFilePath)
	
	      //打开一个输出流
	      val lzopCodec = new LzopCodec()
	      lzopCodec.setConf(new Configuration())
	      val lzoIndexOuputStream = lzopCodec.createIndexedOutputStream(fs.create(dstPath),fs.create(dstIndexPath))
	      val pw:PrintWriter = new PrintWriter(new OutputStreamWriter(lzoIndexOuputStream,Charset.forName("UTF-8")));
	
	      try {
	        var count = 0
	        while (it.hasNext) {
	          //写数据
	          pw.println(it.next())
	
	          //增加计数
	          count = count + 1
	          //判断是否需要将数据写到硬盘中
	          if (count >= 1000) {
	
	            //强制写入到存储中
	            pw.flush()
	            //数量重新计算
	            count = 0
	          }
	
	        }
	      } finally {
	        //关闭数据流
	        pw.close()
	        fs.close()
	      }
	
	      //此处单机测试,所有的输出本机文件,如果分布式运行,那么输出文件还是放到hdfs吧
	      s"I Am Partition ${tc.partitionId()}"
	    }
	
	    //开始执行函数
	    val res = sc.runJob(rdd, func)
	    //输出各个partition的执行结果.如果返回结果比较小,直接返回到driver
	    res.foreach(println)
	    sc.stop()
	  }
	```
2. Spark通过MapReduce写HDFS

	```
	rdd.saveAsTextFile(path, classOf[com.hadoop.compression.lzo.LzopCodec])
	```
	
#### 两者的区别
1. 直接写API的方式，可以自动创建index文件，而MR的方式不能
2. 直接写API的方式，可以自定义每个文件的名称。
