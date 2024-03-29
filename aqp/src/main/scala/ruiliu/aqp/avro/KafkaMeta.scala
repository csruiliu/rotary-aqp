/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ruiliu.aqp.avro

import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType

object KafkaMeta {
  val checkpointLocation = "hdfs://localhost:9000/spark_checkpoint"

  val userTopics = "User"
  val userschema = new StructType().add("u_id", "int")
                                    .add("u_name", "string")
                                    .add("u_n_id", "int")
  val userPath = "/home/totemtang/slothdb/slothdb_testsuite/datadir/testdata/user"

  val userAvroSchema = SchemaConverters.toAvroType(userschema)
}
