/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.yosegi.spark.blackbox;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DecimalType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class DataSourceTest {
    private static SparkSession spark;
    private static SQLContext sqlContext;
    private static final String appName = "DataSourceTest";

    public boolean deleteDirectory(final File directory) {
        final File[] allContents = directory.listFiles();
        if (allContents != null) {
            for (final File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directory.delete();
    }

    public String getTmpPath() {
        String tmpdir = System.getProperty("java.io.tmpdir");
        if (tmpdir.endsWith("/")) {
            tmpdir = tmpdir.substring(0, tmpdir.length() - 1);
        }
        return tmpdir + "/" + appName;
    }

    public String getLocation(final String table) {
        String tmpPath = getTmpPath();
        return tmpPath + "/" + table;
    }

    @BeforeAll
    static void initAll() {
        spark = SparkSession.builder().appName(appName).master("local[*]").getOrCreate();
        sqlContext = spark.sqlContext();
    }

    @AfterAll
    static void tearDownAll() {
        spark.close();
    }

    @AfterEach
    void tearDown() {
        deleteDirectory(new File(getTmpPath()));
    }

    @Test
    void T_DataSource_Primitive_1() throws IOException {
        final String location = getLocation("primitive1");
        final int precision = DecimalType.MAX_PRECISION();
        final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
        /**
         * CREATE TABLE primitive1 (
         *   id INT,
         *   bo BOOLEAN,
         *   by BYTE,
         *   de DECIMAL(38,6),
         *   do DOUBLE,
         *   fl FLOAT,
         *   in INT,
         *   lo LONG,
         *   sh SHORT,
         *   st STRING
         * )
         * USING yosegi
         * LOCATION ''
         */
        final String ddl = String.format("CREATE TABLE primitive1 (\n" +
            "  id INT,\n" +
            "  bo BOOLEAN,\n" +
            "  by BYTE,\n" +
            "  de DECIMAL(%d, %d),\n" +
            "  do DOUBLE,\n" +
            "  fl FLOAT,\n" +
            "  in INT,\n" +
            "  lo LONG,\n" +
            "  sh SHORT,\n" +
            "  st STRING\n" +
            ")\n" +
            "USING yosegi\n" +
            "LOCATION '%s';", precision, scale, location);
        spark.sql(ddl);

        /**
         * FIXME: cannot insert decimal value.
         */
        final String insertSql = "INSERT INTO primitive1\n" +
            "(id, bo, by, de, do, fl, in, lo, sh, st)\n" +
            "VALUES\n" +
            "(0, true, 127, 123.45678901, 1.7976931348623157e+308, 3.402823e+37, 2147483647, 9223372036854775807, 32767, 'value1');";
        spark.sql(insertSql);

        List<Row> rows = spark.sql("SELECT * FROM primitive1;").collectAsList();
        assertEquals(rows.get(0).getAs("id"), Integer.valueOf(0));
        assertEquals(rows.get(0).getAs("bo"), Boolean.valueOf(true));
        assertEquals(rows.get(0).getAs("by"), Byte.valueOf((byte) 127));
        assertNull(rows.get(0).getAs("de"));
        assertEquals(rows.get(0).getAs("do"), Double.valueOf(1.7976931348623157e+308));
        assertEquals(rows.get(0).getAs("fl"), Float.valueOf(3.402823e+37F));
        assertEquals(rows.get(0).getAs("in"), Integer.valueOf(2147483647));
        assertEquals(rows.get(0).getAs("lo"), Long.valueOf(9223372036854775807L));
        assertEquals(rows.get(0).getAs("sh"), Short.valueOf((short) 32767));
        assertEquals(rows.get(0).getAs("st"), "value1");
    }

    @Test
    void T_DataSource_Primitive_2() throws IOException {
        final String location = getLocation("primitive2");
        final int precision = DecimalType.MAX_PRECISION();
        final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
        /**
         * CREATE TABLE primitive2 (
         *   id INT,
         *   bo BOOLEAN,
         *   by BYTE,
         *   de DOUBLE,
         *   do DOUBLE,
         *   fl FLOAT,
         *   in INT,
         *   lo LONG,
         *   sh SHORT,
         *   st STRING
         * )
         * USING yosegi
         * LOCATION ''
         */
        final String ddl1 = String.format("CREATE TABLE primitive2 (\n" +
            "  id INT,\n" +
            "  bo BOOLEAN,\n" +
            "  by BYTE,\n" +
            "  de DOUBLE,\n" +
            "  do DOUBLE,\n" +
            "  fl FLOAT,\n" +
            "  in INT,\n" +
            "  lo LONG,\n" +
            "  sh SHORT,\n" +
            "  st STRING\n" +
            ")\n" +
            "USING yosegi\n" +
            "LOCATION '%s';", location);
        spark.sql(ddl1);

        final String insertSql = "INSERT INTO primitive2\n" +
            "(id, bo, by, de, do, fl, in, lo, sh, st)\n" +
            "VALUES\n" +
            "(0, true, 127, 123.45678901, 1.7976931348623157e+308, 3.402823e+37, 2147483647, 9223372036854775807, 32767, 'value1');";
        spark.sql(insertSql);

        spark.sql("DROP TABLE primitive2;");

        /**
         * CREATE TABLE primitive2 (
         *   id INT,
         *   bo BOOLEAN,
         *   by BYTE,
         *   de DECIMAL(38,6),
         *   do DOUBLE,
         *   fl FLOAT,
         *   in INT,
         *   lo LONG,
         *   sh SHORT,
         *   st STRING
         * )
         * USING yosegi
         * LOCATION ''
         */
        final String ddl2 = String.format("CREATE TABLE primitive2 (\n" +
            "  id INT,\n" +
            "  bo BOOLEAN,\n" +
            "  by BYTE,\n" +
            "  de DECIMAL(%d, %d),\n" +
            "  do DOUBLE,\n" +
            "  fl FLOAT,\n" +
            "  in INT,\n" +
            "  lo LONG,\n" +
            "  sh SHORT,\n" +
            "  st STRING\n" +
            ")\n" +
            "USING yosegi\n" +
            "LOCATION '%s';", precision, scale, location);
        spark.sql(ddl2);

        List<Row> rows = spark.sql("SELECT * FROM primitive2;").collectAsList();
        assertEquals(rows.get(0).getAs("id"), Integer.valueOf(0));
        assertEquals(rows.get(0).getAs("bo"), Boolean.valueOf(true));
        assertEquals(rows.get(0).getAs("by"), Byte.valueOf((byte) 127));
        assertEquals(rows.get(0).getAs("de"), BigDecimal.valueOf(123.456789));
        assertEquals(rows.get(0).getAs("do"), Double.valueOf(1.7976931348623157e+308));
        assertEquals(rows.get(0).getAs("fl"), Float.valueOf(3.402823e+37F));
        assertEquals(rows.get(0).getAs("in"), Integer.valueOf(2147483647));
        assertEquals(rows.get(0).getAs("lo"), Long.valueOf(9223372036854775807L));
        assertEquals(rows.get(0).getAs("sh"), Short.valueOf((short) 32767));
        assertEquals(rows.get(0).getAs("st"), "value1");
    }

    @Test
    void T_DataSource_Expand_1() throws IOException {
        final String location = getLocation("flatten1");
        /**
         * CREATE TABLE expand1 (
         *   id INT,
         *   a ARRAY<INT>
         * )
         * USING yosegi
         * LOCATION '';
         */
        final String ddl1 = String.format("CREATE TABLE expand1 (\n" +
            "  id INT,\n" +
            "  a ARRAY<INT>\n" +
            ")\n" +
            "USING yosegi\n" +
            "LOCATION '%s';", location);
        spark.sql(ddl1);

        final String insertSql = "INSERT INTO expand1\n" +
            "(id, a)\n" +
            "VALUES\n" +
            "(0, array(1,2,3));";
        spark.sql(insertSql);

        spark.sql("DROP TABLE expand1;");

        /**
         * CREATE TABLE expand1(
         *   id INT,
         *   aa INT
         * )
         * USING yosegi
         * LOCATION ''
         * OPTIONS (
         *   'spread.reader.expand.column'='{"base":{"node":"a", "link_name":"aa"}}'
         * );
         */
        final String ddl2 = String.format("CREATE TABLE expand1(\n" +
            "  id INT,\n" +
            "  aa INT\n" +
            ")\n" +
            "USING yosegi\n" +
            "LOCATION '%s'\n" +
            "OPTIONS (\n" +
            "  'spread.reader.expand.column'='{\"base\":{\"node\":\"a\", \"link_name\":\"aa\"}}'\n" +
            ");", location);
        spark.sql(ddl2);

        List<Row> rows = spark.sql("SELECT * FROM expand1 ORDER BY id, aa;").collectAsList();
        assertEquals(rows.get(0).getAs("id"), Integer.valueOf(0));
        assertEquals(rows.get(1).getAs("id"), Integer.valueOf(0));
        assertEquals(rows.get(2).getAs("id"), Integer.valueOf(0));
        assertEquals(rows.get(0).getAs("aa"), Integer.valueOf(1));
        assertEquals(rows.get(1).getAs("aa"), Integer.valueOf(2));
        assertEquals(rows.get(2).getAs("aa"), Integer.valueOf(3));
    }

    @Test
    void T_DataSource_Flatten_1() throws IOException {
        final String location = getLocation("flatten1");
        /**
         * CREATE TABLE flatten1 (
         *   id INT,
         *   m MAP<STRING, STRING>
         * )
         * USING yosegi
         * LOCATION '';
         */
        final String ddl1 = String.format("CREATE TABLE flatten1 (\n" +
            "  id INT,\n" +
            "  m MAP<STRING, STRING>\n" +
            ")\n" +
            "USING yosegi\n" +
            "LOCATION '%s';", location);
        spark.sql(ddl1);

        final String insertSql = "INSERT INTO flatten1\n" +
            "(id, m)\n" +
            "VALUES\n" +
            "(0, map('k1', 'v1', 'k2', 'v2'));";
        spark.sql(insertSql);

        spark.sql("DROP TABLE flatten1;");

        /**
         * CREATE TABLE flatten1 (
         *   id INT,
         *   mk1 STRING,
         *   mk2 STRING
         * )
         * USING yosegi
         * LOCATION ''
         * OPTIONS (
         *   'spread.reader.flatten.column'='[{"link_name":"id", "nodes":["id"]}, {"link_name":"mk1", "nodes":["m","k1"]}, {"link_name":"mk2", "nodes":["m","k2"]}]'
         * );
         */
        final String ddl2 = String.format("CREATE TABLE flatten1 (\n" +
            "  id INT,\n" +
            "  mk1 STRING,\n" +
            "  mk2 STRING\n" +
            ")\n" +
            "USING yosegi\n" +
            "LOCATION '%s'\n" +
            "OPTIONS (\n" +
            "  'spread.reader.flatten.column'='[{\"link_name\":\"id\", \"nodes\":[\"id\"]}, {\"link_name\":\"mk1\", \"nodes\":[\"m\",\"k1\"]}, {\"link_name\":\"mk2\", \"nodes\":[\"m\",\"k2\"]}]'\n" +
            ");", location);
        spark.sql(ddl2);

        List<Row> rows = spark.sql("SELECT * FROM flatten1;").collectAsList();
        assertEquals(rows.get(0).getAs("id"), Integer.valueOf(0));
        assertEquals(rows.get(0).getAs("mk1"), "v1");
        assertEquals(rows.get(0).getAs("mk2"), "v2");
    }
}
