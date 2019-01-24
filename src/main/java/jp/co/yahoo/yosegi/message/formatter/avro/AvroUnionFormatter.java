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

package jp.co.yahoo.yosegi.message.formatter.avro;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AvroUnionFormatter implements IAvroFormatter {

  private final List<IAvroFormatter> childContainer;

  /**
   * Initialize by setting the schema information.
   */
  public AvroUnionFormatter( final Schema avroSchema ) {
    childContainer = new ArrayList<IAvroFormatter>();
    List<Schema> childSchemas = avroSchema.getTypes();
    for ( Schema child : childSchemas ) {
      childContainer.add( AvroFormatterFactory.get( child ) );
    }
  }

  @Override
  public Object write( final Object obj ) throws IOException {
    Object retVal = null;
    for ( IAvroFormatter formatter : childContainer ) {
      formatter.clear();
      retVal = formatter.write( obj );
      if ( retVal != null ) {
        return retVal;
      }
    }

    return retVal;
  }

  @Override
  public Object writeParser(
      final PrimitiveObject obj , final IParser parser ) throws IOException {
    Object retVal = null;
    for ( IAvroFormatter formatter : childContainer ) {
      formatter.clear();
      retVal = formatter.writeParser( obj , parser );
      if ( retVal != null ) {
        return retVal;
      }
    }
    return retVal;
  }

  @Override
  public void clear() throws IOException {
    for ( IAvroFormatter formatter : childContainer ) {
      formatter.clear();
    }
  }

}
