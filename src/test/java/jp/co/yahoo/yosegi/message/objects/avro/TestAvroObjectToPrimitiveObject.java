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

package jp.co.yahoo.yosegi.message.objects.avro;

import jp.co.yahoo.yosegi.message.objects.BooleanObj;
import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.NullObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.StringObj;

import org.apache.avro.AvroTypeException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class TestAvroObjectToPrimitiveObject {

  @Test
  public void T_convertBooleanObj_equalsYosegiObj_whenTrue() throws IOException {
    PrimitiveObject obj = AvroObjectToPrimitiveObject.get( Boolean.valueOf( true ) );
    assertTrue( obj instanceof BooleanObj );
    assertTrue( obj.getBoolean() );
  }

  @Test
  public void T_convertBooleanObj_equalsYosegiObj_whenFalse() throws IOException {
    PrimitiveObject obj = AvroObjectToPrimitiveObject.get( Boolean.valueOf( false ) );
    assertTrue( obj instanceof BooleanObj );
    assertFalse( obj.getBoolean() );
  }

  @Test
  public void T_convertBytesObj_equalsYosegiObj_whenEmpty() throws IOException {
    PrimitiveObject obj = AvroObjectToPrimitiveObject.get( ByteBuffer.wrap( new byte[0] ) );
    assertTrue( obj instanceof BytesObj );
    assertEquals( obj.getString() , "" );
  }

  @Test
  public void T_convertBytesObj_equalsYosegiObj_whenString() throws IOException {
    PrimitiveObject obj = AvroObjectToPrimitiveObject.get( ByteBuffer.wrap( new byte[]{'a','b','c'} ) );
    assertTrue( obj instanceof BytesObj );
    assertEquals( obj.getString() , "abc" );
  }

  @Test
  public void T_convertStringObj_equalsYosegiObj_whenEmpty() throws IOException {
    PrimitiveObject obj = AvroObjectToPrimitiveObject.get( "" );
    assertTrue( obj instanceof StringObj );
    assertEquals( obj.getString() , "" );
  }

  @Test
  public void T_convertStringObj_equalsYosegiObj_whenString() throws IOException {
    PrimitiveObject obj = AvroObjectToPrimitiveObject.get( "abc" );
    assertTrue( obj instanceof StringObj );
    assertEquals( obj.getString() , "abc" );
  }

  @Test
  public void T_convertIntegerObj_equalsYosegiObj() throws IOException {
    PrimitiveObject obj = AvroObjectToPrimitiveObject.get( 100 );
    assertTrue( obj instanceof IntegerObj );
    assertEquals( obj.getInt() , 100 );
  }

  @Test
  public void T_convertLongObj_equalsYosegiObj() throws IOException {
    PrimitiveObject obj = AvroObjectToPrimitiveObject.get( 100L );
    assertTrue( obj instanceof LongObj );
    assertEquals( obj.getLong() , 100L );
  }

  @Test
  public void T_convertFloatObj_equalsYosegiObj() throws IOException {
    PrimitiveObject obj = AvroObjectToPrimitiveObject.get( 0.1f );
    assertTrue( obj instanceof FloatObj );
    assertEquals( obj.getFloat() , 0.1f );
  }

  @Test
  public void T_convertDoubleObj_equalsYosegiObj() throws IOException {
    PrimitiveObject obj = AvroObjectToPrimitiveObject.get( 0.1d );
    assertTrue( obj instanceof DoubleObj );
    assertEquals( obj.getDouble() , 0.1d );
  }

  @Test
  public void T_convertNullObj_equalsYosegiObj() throws IOException {
    PrimitiveObject obj = AvroObjectToPrimitiveObject.get( null );
    assertTrue( obj instanceof NullObj );
  }

  @Test
  public void T_convertArrayObj_equalsYosegiObj() throws IOException {
    PrimitiveObject obj = AvroObjectToPrimitiveObject.get( Arrays.asList(0, 1, 2) );
    assertTrue( obj instanceof NullObj );
  }

  @Test
  public void T_convertEmptyArrayObj_isNull() throws IOException {
    PrimitiveObject obj = AvroObjectToPrimitiveObject.get( Arrays.asList() );
    assertTrue( obj instanceof NullObj );
  }

  @Test
  public void T_convertUnknownObj_isNull() throws IOException {
    PrimitiveObject obj = AvroObjectToPrimitiveObject.get( new StringObj( "" ) );
    assertTrue( obj instanceof NullObj );
  }

}
