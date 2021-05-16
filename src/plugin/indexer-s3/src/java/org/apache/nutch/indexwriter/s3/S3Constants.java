/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.indexwriter.s3;

public interface S3Constants {
  /**
   * The S3 bucket to which all outputs are written.
   */
  String BUCKET = "bucket";

  /**
   * The S3 region, e.g. us-east-1, to which all outputs are written.
   */
  String REGION = "region";

  /**
   * A folder in the S3 bucket to which all outputs are written.
   *
   * Metadata files are written to uuid.json, one per NutchDocument
   * Full text files, if requested, are written to uuid.txt with the same uuid as the metadata.
   */
  String FOLDER = "folder";

  /**
   * A JSON template specifying the arrangement and grouping of all output fields.
   *
   * Example '{"location":{"url":"","type":""},"citation":{"title":"","date":""}}'
   *
   * The copy and rename sections of the index-writer.xml then needs to map
   * the NutchDocument field names to a dot-notation path to place any field in this structure.
   * Use the remove section to eliminate fields that are not desired in the output.
   * Only mapped fields will be included.
   *
   *  &lt;rename&gt;
   *    &lt;field source="url" dest="location.url"/&gt;
   *    &lt;field source="media_type" dest="location.type"/&gt;
   *    &lt;field source="title" dest="citation.title"/&gt;
   *    &lt;field source="date" dest="citation.date"/&gt;
   *  &lt;/rename&gt;
   *
   * The mapping supports string and int arrays in the output.
   * Example '{"authors":[]}'
   *
   * There is limited support for object arrays in the output; they must use key and value attributes.
   * ..."props":[{"key":"prop1","value":""},{"key":"prop2","value":""},]
   * And to map a NutchDocument field to prop2, use:
   *
   *  &lt;field source="field" dest="props.key=prop2"/&gt;
   *
   * If you map 'Content' to 'full_text_file' then a separate file
   * will be written containing the Content only, with type text/plain
   * and this field will be excluded from the metadata file.
   *
   * Other Features:
   * You can place static values on output just by simply coding them into the template.
   * if the template contains the string "randomuuid" this will be replaced with a random UUID per use.
   * if the template contains the string "uuid" this will be replaced with a UUID generated from a uuid_seed field on input.
   */
  String TEMPLATE = "template";

  /**
   * An optional https POST validation endpoint that will check the constructed JSON.
   *
   * The JSON will be sent with content-type "application/json" with the string JSON as payload.
   * The validator must return "OK" somewhere in the response to accept the structure.
   * Otherwise it should return a useful message that will be LOG'ed to the console
   */
  String VALIDATOR = "validator";

  /**
   * An optional folder in the S3 bucket to which all invalid outputs are written.
   *
   * The message from the validator will be placed into the object with the key
   * "validation_message" inserted into the root.
   * If no folder is provided, then only console logging will be provided.
   */
  String INVALID_FOLDER = "invalid_folder";

  /** 
   * The aws_access_key_id permitting write access to the bucket.
   */
  String AWS_ACCESS_KEY_ID = "aws_access_key_id";

  /**
   * The aws_secret_access_key permitting write access to the bucket.
   */
  String AWS_SECRET_ACCESS_KEY = "aws_secret_access_key";
}
