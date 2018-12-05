package org.apache.sqoop.extend;

import org.apache.sqoop.tool.ToolDesc;
import org.apache.sqoop.tool.ToolPlugin;

import java.util.Collections;
import java.util.List;

/**
 * @author wangshuai
 * @version 2018-12-03 14:38
 * describe:
 * 目标文件：
 * 目标表：
 */
public class ExtendHBasePlugin extends ToolPlugin {
    @Override
    public List<ToolDesc> getTools() {
        return Collections
                .singletonList(new ToolDesc("extendHbase", ExtendHBase.class, "HDFS上的数据导入到HBase"));
    }
}
