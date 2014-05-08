package cn.com.gxdgroup.dataplatform.demo

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
 * Created by wq on 14-5-6.
 */
class KeyBasedOutput [T >: Null ,V <: AnyRef] extends MultipleTextOutputFormat[T , V] {

  /**
   * Use they key as part of the path for the final output file.
   */

  override protected def generateFileNameForKeyValue(key: T, value: V, leaf: String) = {
    key.toString()
  }

  /**
   * When actually writing the data, discard the key since it is already in
   * the file path.
   */

  override protected def generateActualKey(key: T, value: V) = {
    null
  }
}
