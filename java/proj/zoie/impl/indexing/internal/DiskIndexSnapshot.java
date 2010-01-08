
package proj.zoie.impl.indexing.internal;
/**
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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;

import proj.zoie.impl.indexing.internal.ZoieIndexDeletionPolicy.Snapshot;

/**
 * @author ymatsuda
 *
 */
public class DiskIndexSnapshot
{
  private IndexSignature _sig;
  private Snapshot _snapshot;
  
  public DiskIndexSnapshot(IndexSignature sig, Snapshot snapshot)
  {
    _sig = sig;
    _snapshot = snapshot;
  }
  
  public void close()
  {
    _snapshot.close();
  }
  
  public long writeTo(WritableByteChannel channel) throws IOException
  {
    // format:
    //   <format_version> <sig_len> <sig_data> { <idx_file_name_len> <idx_file_name> <idx_file_len> <idx_file_data> }...
    
    long amount = 0;
    
    // format version
    amount += writeInt(channel, 1);
    
    // index signature
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    _sig.save(baos);
    byte[] sigBytes = baos.toByteArray();
    
    amount += writeLong(channel, (long)sigBytes.length); // data length
    amount += channel.write(ByteBuffer.wrap(sigBytes)); // data

    // index files
    File dir = _snapshot.getDirectory();
    Collection<String> fileNames = _snapshot.getFileNames();
    amount += writeInt(channel, fileNames.size()); // number of files
    for(String fileName : fileNames)
    {
      amount += writeString(channel, fileName);
      amount += transferFromFileToChannel(new File(dir, fileName), channel);
    }
    return amount;
  }
  
  public static void readSnapshot(ReadableByteChannel channel, File dest) throws IOException
  {
    // format version
    int formatVersion = readInt(channel);
    if(formatVersion != 1)
    {
      throw new IOException("snapshot format version mismatch [" + formatVersion + "]");
    }
    
    // index signature
    if(!transferFromChannelToFile(channel, new File(dest, IndexReaderDispenser.INDEX_DIRECTORY)))
    {
      throw new IOException("bad snapshot file");
    }

    // index files
    File indexDir = new File(dest, IndexReaderDispenser.INDEX_DIR_NAME);
    int numFiles = readInt(channel); // number of files
    if(numFiles < 0)
    {
      throw new IOException("bad snapshot file");      
    }
    while(numFiles-- > 0)
    {
      String fileName = readString(channel);
      if(fileName == null)
      {
        throw new IOException("bad snapshot file");
      }
      if(!transferFromChannelToFile(channel, new File(indexDir, fileName)))
      {
        throw new IOException("bad snapshot file");
      }
    }
  }
  
  private static long transferFromFileToChannel(File src, WritableByteChannel channel) throws IOException
  {
    long amount = 0;
    RandomAccessFile raf = null;
    FileChannel fc = null;
    try
    {
      raf = new RandomAccessFile(src, "rw");
      fc = raf.getChannel();
      long dataLen = fc.size();
      amount += writeLong(channel, dataLen);
      amount += fc.transferTo(0, dataLen, channel);
    }
    finally
    {
      if(raf != null) raf.close();
    }
    return amount;
  }
  
  private static boolean transferFromChannelToFile(ReadableByteChannel channel, File dest) throws IOException
  {
    long dataLen = readLong(channel);
    if(dataLen < 0) return false;
    
    RandomAccessFile raf = null;
    FileChannel fc = null;
    try
    {
      raf = new RandomAccessFile(dest, "rw");
      fc = raf.getChannel();
      return (fc.transferFrom(channel, 0, dataLen) == dataLen);
    }
    finally
    {
      if(raf != null) raf.close();
    }
  }
  
  private static long writeInt(WritableByteChannel channel, int val) throws IOException
  {
    ByteBuffer buf = ByteBuffer.allocate(4);
    buf.putInt(val);
    buf.rewind();
    return channel.write(buf);
  }
  
  private static long writeLong(WritableByteChannel channel, long val) throws IOException
  {
    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.putLong(val);
    buf.rewind();
    return channel.write(buf);
  }
  
  private static long writeString(WritableByteChannel channel, String val) throws IOException
  {
    int len = val.length();
    ByteBuffer buf = ByteBuffer.allocate(4 + 2 * len);
    buf.putInt(len);
    for(int i = 0; i < len; i++)
    {
      buf.putChar(val.charAt(i));
    }
    buf.rewind();
    return channel.write(buf);
  }
  
  private static int readInt(ReadableByteChannel channel) throws IOException
  {
    ByteBuffer buf = ByteBuffer.allocate(4);
    if(fillBuffer(channel, buf, true))
    {
      buf.rewind();
      return buf.getInt();
    }
    return -1;
  }
  
  private static long readLong(ReadableByteChannel channel) throws IOException
  {
    ByteBuffer buf = ByteBuffer.allocate(8);
    if(fillBuffer(channel, buf, true))
    {
      buf.rewind();
      return buf.getLong();
    }
    return -1L;
  }
  
  private static String readString(ReadableByteChannel channel) throws IOException
  {
    int nameLen = readInt(channel); // name length
    if(nameLen < 0) return null;
    
    ByteBuffer buf = ByteBuffer.allocate(nameLen * 2);
    if(fillBuffer(channel, buf, true))
    {
      char[] name = new char[nameLen];
      buf.rewind();
      for(int i = 0; i < nameLen; i++)
      {
        name[i] = buf.getChar();
      }
      return new String(name);
    }
    return null;
  }
  
  private static boolean fillBuffer(ReadableByteChannel channel, ByteBuffer buf, boolean clear) throws IOException
  {
    if(clear) buf.clear();
    
    while(true)
    {
      int cnt = channel.read(buf);
      if(cnt < 0) return false;
      if(buf.limit() == buf.capacity()) break;
    }
    return true;
  }
}
