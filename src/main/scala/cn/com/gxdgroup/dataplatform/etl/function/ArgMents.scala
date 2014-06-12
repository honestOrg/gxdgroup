package cn.com.gxdgroup.dataplatform.etl.function

/**
 * Created by SZZ on 14-6-5
 */
object ArgMents {
  def unapplySeq(in: String): Option[Seq[String]] = {
    if (in.trim == "") None
    else {
      val tem = in.split(",", -1)
      if (tem.length < 3) None else Some(tem)
    }
  }
}
