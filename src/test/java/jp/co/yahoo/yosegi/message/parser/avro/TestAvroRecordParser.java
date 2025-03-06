/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.co.yahoo.yosegi.message.parser.avro;

import jp.co.yahoo.yosegi.message.TestAvroDataUtil;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.message.design.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class TestAvroRecordParser {

  @Test
  public void T_checkAllFields() throws IOException{
    String json = "{ \"struct_col\": { \"col1\":1, \"col2\":2, \"col3\":3 } }";
    StructContainerField schema = new StructContainerField( "root" );
    StructContainerField structCol = new StructContainerField( "struct_col" );
    structCol.set( new IntegerField( "col1" ) );
    structCol.set( new IntegerField( "col2" ) );
    structCol.set( new IntegerField( "col3" ) );
    schema.set( structCol );
    byte[] avro = TestAvroDataUtil.jsonToAvro( json , schema );
    AvroMessageReader avroReader = new AvroMessageReader( schema );
    IParser avroParser = avroReader.create( avro );
    IParser jsonParser = new JacksonMessageReader().create( json );
    assertEquals( avroParser.size() , jsonParser.size() );

    IParser avroStructParser = avroParser.getParser( "struct_col" );
    IParser jsonStructParser = jsonParser.getParser( "struct_col" );
    assertEquals( avroStructParser.size() , 3 );
    assertEquals( avroStructParser.size() , jsonStructParser.size() );
    for ( String key : jsonStructParser.getAllKey() ) {
      assertEquals( avroStructParser.get( key ).getInt() , jsonStructParser.get( key ).getInt() );
    }
  }
}
