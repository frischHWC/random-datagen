/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.frisch.randomdatagen.model.type;

import com.cloudera.frisch.randomdatagen.Utils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.orc.TypeDescription;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;

public class FloatField extends Field<Float> {

    FloatField(String name, Integer length, List<Float> possibleValues, LinkedHashMap<String, Integer> possible_values_weighted, String min, String max) {
        if(length==null || length==-1) {
            this.length = Integer.MAX_VALUE;
        } else {
            this.length = length;
        }
        if(max==null) {
            this.max = Long.MAX_VALUE;
        } else {
            this.max = Long.parseLong(max);
        }
        if(min==null) {
            this.min = Long.MIN_VALUE;
        } else {
            this.min = Long.parseLong(min);
        }
        this.name = name;
        this.possibleValues = possibleValues;
        this.possible_values_weighted = possible_values_weighted;
    }

    public Float generateRandomValue() {
        if(!possibleValues.isEmpty()) {
            return possibleValues.get(random.nextInt(possibleValues.size()));
        } else if (!possible_values_weighted.isEmpty()){
            String result = Utils.getRandomValueWithWeights(random, possible_values_weighted);
            return result.isEmpty() ? 0f :  Float.parseFloat(result);
        } else {
            float randomFloat = random.nextFloat();
            while(randomFloat < min && randomFloat > max) {
                randomFloat = random.nextLong();
            }
            return randomFloat;
        }
    }

    /*
     Override if needed Field function to insert into special sinks
     */

    @Override
    public String toStringValue(Float value) {
        return value.toString();
    }
    @Override
    public Float toCastValue(String value) {
        return Float.valueOf(value);
    }

    @Override
    public Put toHbasePut(Float value, Put hbasePut) {
        hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name), Bytes.toBytes(value));
        return hbasePut;
    }

    @Override
    public PartialRow toKudu(Float value, PartialRow partialRow) {
        partialRow.addFloat(name, value);
        return partialRow;
    }

    @Override
    public Type getKuduType() {
        return Type.FLOAT;
    }

    @Override
    public HivePreparedStatement toHive(Float value, int index, HivePreparedStatement hivePreparedStatement) {
        try {
            hivePreparedStatement.setFloat(index, value);
        } catch (SQLException e) {
            logger.warn("Could not set value : " +value.toString() + " into hive statement due to error :", e);
        }
        return hivePreparedStatement;
    }

    @Override
    public String getHiveType() {
        return "FLOAT";
    }

    @Override
    public String getGenericRecordType() { return "float"; }

    @Override
    public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
        return batch.cols[cols];
    }

    @Override
    public TypeDescription getTypeDescriptionOrc() {
        return TypeDescription.createFloat();
    }
}
