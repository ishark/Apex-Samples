package com.example.streamcodec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.netlet.util.Slice;

public class SampleStreamCodec implements StreamCodec<String>, Serializable
{
  private static final long serialVersionUID = 1632149260243352131L;
  private String filterByClass = "";
  @Override
  public Object fromByteArray(Slice fragment)
  {
    // Custom logic to deserialize 
    try {
      byte[] decompressed = decompress(fragment.toByteArray());
      return new String(decompressed);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (DataFormatException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Slice toByteArray(String o)
  {
    // Custom logic to serialize
    try {
      byte[] compressed = compress(o.getBytes());
      return new Slice(compressed);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

 
  /*https://dzone.com/articles/how-compress-and-uncompress */
  public static byte[] compress(byte[] data) throws IOException {  
    Deflater deflater = new Deflater();  
    deflater.setInput(data);  
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);   
    deflater.finish();  
    byte[] buffer = new byte[1024];   
    while (!deflater.finished()) {  
     int count = deflater.deflate(buffer);  
     outputStream.write(buffer, 0, count);   
    }  
    outputStream.close();  
    byte[] output = outputStream.toByteArray();  
    return output;  
   } 
  
  public static byte[] decompress(byte[] data) throws IOException, DataFormatException {  
    Inflater inflater = new Inflater();   
    inflater.setInput(data);  
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);  
    byte[] buffer = new byte[1024];  
    while (!inflater.finished()) {  
     int count = inflater.inflate(buffer);  
     outputStream.write(buffer, 0, count);  
    }  
    outputStream.close();  
    byte[] output = outputStream.toByteArray();
    return output;  
   } 
  @Override
  public int getPartition(String o)
  {
    // Custom partitioning
    if (o.contains(filterByClass)) {
      return 0;
    }
    return 1;
  }

  public String getFilterByClass()
  {
    return filterByClass;
  }

  public void setFilterByClass(String filterByClass)
  {
    this.filterByClass = filterByClass;
  }

}
