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

public class FileInputReaderWithShutDown extends AbstractFileInputOperator<String>
{
  
  private boolean doneReading = false;

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
  
  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    // TODO Auto-generated method stub
    super.closeFile(is);
    if(pendingFiles.isEmpty() && failedFiles.isEmpty()) {
      doneReading = true;
    }
  }

  @Override
  public void endWindow()
  {
    // TODO Auto-generated method stub
    super.endWindow();
    if(doneReading) {
      logger.info("Shutting down....");
      throw new ShutdownException();
    }
  }
  
  public static Logger logger = LoggerFactory.getLogger(FileInputReaderWithShutDown.class);
}
