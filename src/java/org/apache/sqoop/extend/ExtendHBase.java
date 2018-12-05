package org.apache.sqoop.extend;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.cli.RelatedOptions;
import org.apache.sqoop.cli.ToolOptions;
import org.apache.sqoop.tool.BaseSqoopTool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wangshuai
 * @version 2018-12-03 10:47
 * describe:
 * 目标文件：
 * 目标表：
 */
public class ExtendHBase extends BaseSqoopTool {

    /*
    * sqoop extendHbase \
    --hbase-table mytable \
    --column-family i \
    --hbase-row-key GDDM,GDXM \
    --export-dir /yss/guzhi/interface/gh.tsv \
    --hbase-rowkey-separator _ \
    --hdfs-line-separator \\t \
    --hbase-col GDDM,GDXM,BCRQ,CJBH,GSDM,CJSL,BCYE,ZQDM,SBSJ,CJSJ,CJJG,CJJE,SQBH,BS,MJBH
    *
    *
    *
    *
    * */
    public static final Log LOG = LogFactory.getLog(ExtendHBase.class.getName());
    public static final String HBASE_COL = "hbase-col";
    public static final String HDFS_LINE_SEPARATOR = "hdfs-line-separator";
    public static final String HBASE_ROWKEY_SEPARATOR = "hbase-rowkey-separator";

    public ExtendHBase() {
        super("extendHbase");
    }

    public void putSqoopOptionsToConfiguration(SqoopOptions opts, Configuration configuration) {
        LOG.info("SqoopOptions中的属性映射到Hadoop的基础属性中方便后期调用!");
        for (Map.Entry<Object, Object> e : opts.writeProperties().entrySet()) {
            String key = (String) e.getKey();
            if (key.equalsIgnoreCase("customtool.options.jsonmap")) {
                LOG.info("SqoopOptions中自定义的属性存放在map集合映射到Hadoop的基础属性中方便后期调用!");
                try {
                    for (Map.Entry<String, String> objectEntry : opts.getCustomToolOptions().entrySet()) {
                        configuration.set("sqoop.opt." + objectEntry.getKey(), objectEntry.getValue());
                    }
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            } else {
                String value = (String) e.getValue();
                // We don't need to do if(value is empty) because that is already done
                // for us by the SqoopOptions.writeProperties() method.
                configuration.set("sqoop.opt." + key, value);
            }
        }
    }

    //这是该工具的主要方法，并充当自定义工具的执行入口点。
    @Override
    public int run(SqoopOptions options) {
        try {
            Configuration conf = options.getConf();
            putSqoopOptionsToConfiguration(options, conf);
            Job job = Job.getInstance(conf);
            job.setJarByClass(ReadHdfsMapper.class);
            job.setMapperClass(ReadHdfsMapper.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setNumReduceTasks(0);
            FileInputFormat.setInputPaths(job, new Path(options.getExportDir()));
            TableMapReduceUtil.initTableReducerJob(options.getHBaseTable(), null, job);
            TableMapReduceUtil.addDependencyJars(job);
            boolean b = job.waitForCompletion(true);
            LOG.info("程序运行结束返回:" + b);
        } catch (IOException e) {
            e.printStackTrace();
            return 1;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return 1;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return 1;
        }
        return 0;
    }

    //配置我们希望接收的命令行参数。您还可以指定所有命令行参数的描述。当用户执行时 sqoop help <your tool>，将以该方法提供的信息输出给用户。
    @Override
    public void configureOptions(ToolOptions toolOptions) {
        super.configureOptions(toolOptions);
        toolOptions.addUniqueOptions(getOutputFormatOptions());
        toolOptions.addUniqueOptions(getInputFormatOptions());
        toolOptions.addUniqueOptions(getHBaseOptions());
        RelatedOptions formatOpts = new RelatedOptions(
                "设置相关mapreduce的参数");
        formatOpts.addOption(OptionBuilder.withArgName("dir")
                .hasArg()
                .withDescription("要导出数据的hdfs地址")
                .withLongOpt(EXPORT_PATH_ARG)
                .create());
        formatOpts.addOption(OptionBuilder.withArgName("char")
                .hasArg()
                .withDescription("hdfs数据的行分隔符")
                .withLongOpt(HDFS_LINE_SEPARATOR)
                .create());
        formatOpts.addOption(OptionBuilder.withArgName("char")
                .hasArg()
                .withDescription("hbase的列簇")
                .withLongOpt(HBASE_COL)
                .create());
        formatOpts.addOption(OptionBuilder.withArgName("char")
                .hasArg()
                .withDescription("hbase的复合rowkey的字段分隔符")
                .withLongOpt(HBASE_ROWKEY_SEPARATOR)
                .create());


        toolOptions.addUniqueOptions(formatOpts);
    }

    //解析所有选项并填充SqoopOptions，它在完成执行期间充当数据传输对象。
    @Override
    public void applyOptions(CommandLine in, SqoopOptions out) throws SqoopOptions.InvalidOptionsException {
        try {
            Map<String, String> optionsMap = new HashMap<String, String>();
            super.applyOptions(in, out);
//            applyOutputFormatOptions(in, out);
//            applyInputFormatOptions(in, out);
            applyHBaseOptions(in, out);
            if (in.hasOption(EXPORT_PATH_ARG)) {
                out.setExportDir(in.getOptionValue(EXPORT_PATH_ARG));
            }
            if (in.hasOption(HDFS_LINE_SEPARATOR)) {
                optionsMap.put(HDFS_LINE_SEPARATOR, in.getOptionValue(HDFS_LINE_SEPARATOR));
            }
            if (in.hasOption(HBASE_COL)) {
                optionsMap.put(HBASE_COL, in.getOptionValue(HBASE_COL));
            }
            if (in.hasOption(HBASE_ROWKEY_SEPARATOR)) {
                optionsMap.put(HBASE_ROWKEY_SEPARATOR, in.getOptionValue(HBASE_ROWKEY_SEPARATOR));
            }
            if (out.getCustomToolOptions() == null) {
                out.setCustomToolOptions(optionsMap);
            }


        } catch (NumberFormatException nfe) {
            throw new SqoopOptions.InvalidOptionsException("Error: expected numeric argument.\n"
                    + "Try --help for usage.");
        }
    }


    // 提供您的选项所需的任何验证。
    @Override
    public void validateOptions(SqoopOptions options) throws SqoopOptions.InvalidOptionsException {
//        super.validateOptions(options);
    }

    @Override
    /** {@inheritDoc} */
    public void printHelp(ToolOptions toolOptions) {
        super.printHelp(toolOptions);
    }


}
