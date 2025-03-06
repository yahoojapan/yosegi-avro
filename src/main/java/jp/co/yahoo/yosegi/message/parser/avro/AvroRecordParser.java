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

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.avro.AvroObjectToPrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class AvroRecordParser implements IParser {

  private final GenericRecord record;

  public AvroRecordParser( final GenericRecord record ) {
    this.record = record;
  } 

  @Override
  public PrimitiveObject get( final String key ) throws IOException {
    return AvroObjectToPrimitiveObject.get( record.get( key ) );
  }

  @Override
  public PrimitiveObject get( final int index ) throws IOException {
    return AvroObjectToPrimitiveObject.get( record.get( index ) );
  }

  @Override
  public IParser getParser( final String key ) throws IOException {
    return AvroObjectToParser.get( record.get( key ) );
  }

  @Override
  public IParser getParser( final int index ) throws IOException {
    return AvroObjectToParser.get( record.get( index ) );
  }

  @Override
  public String[] getAllKey() throws IOException {
    Schema schema = record.getSchema();
    List<Schema.Field> childFieldList = schema.getFields();
    String[] keys = new String[childFieldList.size()];
    for ( int i = 0 ; i < childFieldList.size() ; i++ ) {
      keys[i] = childFieldList.get(i).name();
    }

    return keys;
  }

  @Override
  public boolean containsKey( final String key ) throws IOException {
    return record.get( key ) != null;
  }

  @Override
  public int size() throws IOException {
    return record.getSchema().getFields().size();
  }

  @Override
  public boolean isArray() throws IOException {
    return false;
  }

  @Override
  public boolean isMap() throws IOException {
    return false;
  }

  @Override
  public boolean isStruct() throws IOException {
    return true;
  }

  @Override
  public boolean hasParser( final int index ) throws IOException {
    return AvroObjectToParser.hasParser( record.get( index ) );
  }

  @Override
  public boolean hasParser( final String key ) throws IOException {
    return AvroObjectToParser.hasParser( record.get( key ) );
  }

  @Override
  public Object toJavaObject() throws IOException {
    Map<String,Object> result = new HashMap<String,Object>();
    for ( String key : getAllKey() ) {
      if ( hasParser(key) ) {
        IntStream.range( 0 , size() )
            .forEach( i -> {
              try {
                result.put( key , getParser(i).toJavaObject() );
              } catch ( IOException ex ) {
                throw new UncheckedIOException( "IOException in lambda." , ex );
              }
            } );
      } else {
        IntStream.range( 0 , size() )
            .forEach( i -> {
              try {
                result.put( key , get(i) );
              } catch ( IOException ex ) {
                throw new UncheckedIOException( "IOException in lambda." , ex );
              }
            } );
      }
    }

    return result;
  }

}
