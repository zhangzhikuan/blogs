# runJob的使用
```
    //保存的路径
    val basePath = "/tmp/kuan2"

    //设置日志级别
    //    Example.setStreamingLogLevels()
    //创建sparkConf
    val sparkConf = new SparkConf().setAppName("runJob")
    //设置master,此处设置本地执行
    sparkConf.setMaster("local[*]")
    //创建SparkContext
    val sc = new SparkContext(sparkConf)

    //创建3个分区的RDD
    val rdd = sc.makeRDD(List("a", "b", "c", "d", "e", "f", "g", "宽"), 3).map(_ * 10)


    //在每个executor上执行的函数
    //此处定义的是,针对每个分区,我们把计算好的结果写入到本地目录中
    val func = (tc: TaskContext, it: Iterator[String]) => {
      //根据partitionID,创建待生成的文件名
      val out = new PrintWriter(s"${basePath}/${tc.partitionId()}", "UTF-8")
      try {
        while (it.hasNext) {
          out.println(it.next())
        }
      } finally {
        out.close()
      }
      //此处单机测试,所有的输出本机文件,如果分布式运行,那么输出文件还是放到hdfs吧
      //测试输出
      s"I Am Partition ${tc.partitionId()}"
    }

    //开始执行函数
    val res = sc.runJob(rdd, func)
    //输出各个partition的执行结果.如果返回结果比较小,直接返回到driver
    res.foreach(println)
	  }
```
