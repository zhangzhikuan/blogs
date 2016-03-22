# IP地址和数字的转换
spark在运算过程需要根据ip得到对应省份和城市的信息，需要把ip地址转变成数字，才能进行进行操作。下面就是用scala对ip地址和数字进行相互转换的代码。

```
object ip {
  def main(args: Array[String]) {
    long2ip(ip2long("12222.168.1.1000"))
  }

  def ip2long(ip: String): Long ={
    val Array(a, b, c, d) = ip.split( """\.""")
    val retVal = (a.toLong << 24) + (b.toLong << 16) + (c.toLong << 8) + (d.toLong)
    printf(s"value=$retVal\n")
    retVal
  }

  def long2ip(ip:Long): Unit ={

    val a:Long = (ip >>> 24) & 0xff
    val b:Long = (ip >>> 16) & 0xff
    val c:Long = (ip >>> 8) & 0xff
    val d:Long = (ip) & 0xff

    printf(s"ip=$a.$b.$c.$d")
  }
}
```
