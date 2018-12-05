/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.hbase;

import org.apache.sqoop.lib.FieldMapProcessor;
import org.apache.sqoop.lib.FieldMappable;
import org.apache.sqoop.lib.ProcessingException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * SqoopRecordProcessor that performs an HBase "put" operation
 * that contains all the fields of the record.
 *
 * HBasePUT处理器
 */
public class HBasePutProcessor implements Closeable, Configurable,
        FieldMapProcessor {

    public static final Log LOG = LogFactory.getLog(
            HBasePutProcessor.class.getName());

    /** Configuration key specifying the table to insert into.
     *
     * 配置键，指定要插入的表
     *
     *
     * */
    public static final String TABLE_NAME_KEY = "sqoop.hbase.insert.table";

    /** Configuration key specifying the column family to insert into. */
    public static final String COL_FAMILY_KEY =
            "sqoop.hbase.insert.column.family";

    /** Configuration key specifying the column of the input whose value
     * should be used as the row id.
     *
     *
     * 配置键，指定其值的输入的列
     * *应该用作行id。
     */
    public static final String ROW_KEY_COLUMN_KEY =
            "sqoop.hbase.insert.row.key.column";

    public static final String NULL_INCREMENTAL_MODE = "hbase.null.incremental.mode";

    /**
     * Configuration key specifying the PutTransformer implementation to use.
     * 配置键，指定要使用的PutTransformer实现。
     */
    public static final String TRANSFORMER_CLASS_KEY =
            "sqoop.hbase.insert.put.transformer.class";

    /**
     *  Configuration key to enable/disable hbase bulkLoad.
     *  配置键来启用/禁用hbase bulkLoad。
     */
    public static final String BULK_LOAD_ENABLED_KEY =
            "sqoop.hbase.bulk.load.enabled";

    /** Configuration key to specify whether to add the row key column into
     *  HBase. Set to false by default.
     *  配置键，以指定是否将行键列添加到
     * * HBase。默认设置为false。
     */
    public static final String ADD_ROW_KEY = "sqoop.hbase.add.row.key";
    public static final boolean ADD_ROW_KEY_DEFAULT = false;

    private Configuration conf;

    // An object that can transform a map of fieldName->object
    // into a Put command.
    //一个对象，它可以转换字段名->对象的映射
    //输入Put命令。
    private PutTransformer putTransformer;

    private Connection hbaseConnection;
    private BufferedMutator bufferedMutator;

    public HBasePutProcessor() {
    }

    HBasePutProcessor(Configuration conf, PutTransformer putTransformer, Connection hbaseConnection, BufferedMutator bufferedMutator) {
        this.conf = conf;
        this.putTransformer = putTransformer;
        this.hbaseConnection = hbaseConnection;
        this.bufferedMutator = bufferedMutator;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setConf(Configuration config) {
        this.conf = config;

        // Get the implementation of PutTransformer to use.
        //获取要使用的PutTransformer的实现。
        // By default, we call toString() on every non-null field.
        //默认情况下，我们对每个非空字段调用toString()。
        Class<? extends PutTransformer> xformerClass =
                (Class<? extends PutTransformer>)
                        this.conf.getClass(TRANSFORMER_CLASS_KEY, ToStringPutTransformer.class);
        this.putTransformer = (PutTransformer)
                ReflectionUtils.newInstance(xformerClass, this.conf);
        if (null == putTransformer) {
            throw new RuntimeException("Could not instantiate PutTransformer.");
        }
        putTransformer.init(conf);
        initHBaseMutator();
    }

    private void initHBaseMutator() {
        String tableName = conf.get(TABLE_NAME_KEY, null);
        try {
            hbaseConnection = ConnectionFactory.createConnection(conf);
            bufferedMutator = hbaseConnection.getBufferedMutator(TableName.valueOf(tableName));
        } catch (IOException e) {
            if (hbaseConnection != null) {
                try {
                    hbaseConnection.close();
                } catch (IOException connCloseException) {
                    LOG.error("Cannot close HBase connection.", connCloseException);
                }
            }
            throw new RuntimeException("Could not create mutator for HBase table " + tableName, e);
        }
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    /**
     * Processes a record by extracting its field map and converting
     * it into a list of Put commands into HBase.
     */
    public void accept(FieldMappable record)
            throws IOException, ProcessingException {
        Map<String, Object> fields = record.getFieldMap();
        List<Mutation> mutationList = putTransformer.getMutationCommand(fields);
        if (mutationList == null) {
            return;
        }
        for (Mutation mutation : mutationList) {
            if (!canAccept(mutation)) {
                continue;
            }
            if (!mutation.isEmpty()) {
                bufferedMutator.mutate(mutation);
            } else {
                logEmptyMutation(mutation);
            }
        }
    }

    private void logEmptyMutation(Mutation mutation) {
        String action = null;
        if (mutation instanceof Put) {
            action = "insert";
        } else if (mutation instanceof Delete) {
            action = "delete";
        }
        LOG.warn("Could not " + action + " row with no columns "
                + "for row-key column: " + Bytes.toString(mutation.getRow()));
    }

    private boolean canAccept(Mutation mutation) {
        return mutation != null && (mutation instanceof Put || mutation instanceof Delete);
    }

    @Override
    /**
     * Closes the HBase table and commits all pending operations.
     */
    public void close() throws IOException {
        try {
            bufferedMutator.flush();
        } finally {
            try {
                bufferedMutator.close();
            } finally {
                try {
                    hbaseConnection.close();
                } catch (IOException e) {
                    LOG.error("Cannot close HBase connection.", e);
                }
            }
        }
    }

}
