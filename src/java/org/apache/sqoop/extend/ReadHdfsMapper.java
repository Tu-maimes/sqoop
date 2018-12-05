package org.apache.sqoop.extend;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.sqoop.mapreduce.SqoopMapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author wangshuai
 * @version 2018-12-04 12:50
 * describe:
 * 目标文件：
 * 目标表：
 */
public class ReadHdfsMapper extends SqoopMapper<LongWritable, Text, NullWritable, Put> {
    public static final Log LOG = LogFactory.getLog(ReadHdfsMapper.class.getName());

    protected String columnFamily;
    protected String[] columns;
    protected String inputLinesTerminatedBy;
    protected ArrayList<Integer> rowKeyIndex = new ArrayList<>();
    protected StringBuffer rowKeyBuffer = new StringBuffer();
    protected String rowKeySeparator = "_";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        LOG.info("初始化mapreduce作业参数!");
        Configuration configuration = context.getConfiguration();
        columnFamily = configuration.get("sqoop.opt.hbase.col.family");
        columns = configuration.get("sqoop.opt.hbase-col").split(",");
        inputLinesTerminatedBy = configuration.get("sqoop.opt.hdfs-line-separator");
        String s = configuration.get("sqoop.opt.hbase-rowkey-separator");
        if (s != null) {
            rowKeySeparator = s;
        }
        String[] rowKeys = configuration.get("sqoop.opt.hbase.row.key.col").split(",");
        for (String rowkey : rowKeys) {
            int a = 0;
            for (String column : columns) {
                if (column.equalsIgnoreCase(rowkey)) {
                    rowKeyIndex.add(a);
                    break;
                }
                a++;
            }
        }
    }

    private byte[] getRowKey(String[] dataValue) {
        for (Integer keyIndex : rowKeyIndex) {
            rowKeyBuffer.append(dataValue[keyIndex]);
            rowKeyBuffer.append(rowKeySeparator);
        }
        if (rowKeyBuffer.length() > 1) {
            rowKeyBuffer.delete(rowKeyBuffer.length() - 1, rowKeyBuffer.length());
        }
        return rowKeyBuffer.toString().getBytes();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataValue = value.toString().split(inputLinesTerminatedBy);
        if (dataValue.length == columns.length) {
            Put put = new Put(getRowKey(dataValue));
            for (int a = 0; a < dataValue.length; a++) {
                put.addColumn(columnFamily.getBytes(), columns[a].getBytes(), dataValue[a].getBytes());
            }
            rowKeyBuffer.setLength(0);
            context.write(NullWritable.get(), put);
        } else {
            LOG.error("传入的hbase的column的数量与文件的列数量不匹配!检查是否是分隔符以及相关配置!");
            throw new InterruptedException("传入的hbase的column的数量与文件的列数量不匹配!");
        }
    }
}
