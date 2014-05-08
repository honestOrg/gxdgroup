/**
 * 
 */
package cn.com.gxdgroup.dataplatform.demo;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * @author training
 * 
 */
public class MultipleTextOutputFormatByKey<K,V> extends
		MultipleOutputFormat<K, V> {

	/**
	 * Use they key as part of the path for the final output file.
	 */
	@Override
	protected String generateFileNameForKeyValue(K key, V value,
			String leaf) {
		//return new Path(key.toString(), leaf).toString();
		//return new Path(key.toString(), value.toString()+"2013").toString();
		return new Path(value.toString(), value.toString()).toString();
	}

	/**
	 * When actually writing the data, discard the key since it is already in
	 * the file path.
	 */
	@Override
	protected K generateActualKey(K key, V value) {
		return null;
		//return key;
	}

	@Override
	protected RecordWriter<K, V> getBaseRecordWriter(FileSystem fs,
			JobConf job, String name, Progressable arg3) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
}
