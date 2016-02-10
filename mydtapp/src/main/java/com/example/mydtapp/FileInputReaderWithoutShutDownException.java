package com.example.mydtapp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

public class FileInputReaderWithoutShutDownException extends AbstractFileInputOperator<String>
{
  private BufferedReader bufferedReader;
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  @Override
  protected InputStream openFile(Path path) throws IOException
  {
    InputStream is = super.openFile(path);
    this.bufferedReader = new BufferedReader(new InputStreamReader(is));
    return is;
  }

  @Override
  protected String readEntity() throws IOException
  {
    String payload = this.bufferedReader.readLine();
    return payload;
  }

  @Override
  protected void emit(String tuple)
  {
    output.emit(tuple);
  }
  
  public static Logger logger = LoggerFactory.getLogger(FileInputReaderWithoutShutDownException.class);
}
