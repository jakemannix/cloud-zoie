package proj.zoie.impl.indexing;

import org.apache.log4j.Logger;
import proj.zoie.api.DataConsumer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class SingleFileDataProvider<T> extends StreamDataProvider<T> {

  private static final Logger log = Logger.getLogger(SingleFileDataProvider.class);

  private final Deserializer<T> deserializer;
  private final int datumByteSize;

  private ReadableByteChannel inputChannel;
  private ByteBuffer buf;
  private byte[] bytes;

  private long position;

  public SingleFileDataProvider(String fileName,
                                int datumByteSize,
                                Deserializer<T> deserializer) throws IOException {
    this.deserializer = deserializer;
    this.datumByteSize = datumByteSize;
    bytes = new byte[datumByteSize];
    inputChannel = new FileInputStream(new File(fileName)).getChannel();
    buf = ByteBuffer.wrap(bytes);
    position = 0;
    new Thread(new Starter()).start();
  }

  private final class Starter implements Runnable {

    @Override
    public void run() {
      try {
        Thread.sleep(10000);
        SingleFileDataProvider.this.start();
      } catch(Exception e) {
        System.out.println(e);
      }
    }
  }

  public static interface Deserializer<T> {
    public T deserialize(byte[] bytes);
  }

  private long time = 0;

  @Override
  public DataConsumer.DataEvent<T> next() {
    try {
      long version = position;
      buf.clear();
      while(buf.position() == 0) {
        position += inputChannel.read(buf) / datumByteSize;
        if(buf.position() == 0) {
          try { Thread.sleep(25); } catch (InterruptedException ie) {
            // ignore
          }
        }
      }
      if((position % 100000) == 0) {
        System.out.println(((float)(System.nanoTime() - time)) / 1e9 + "s per 100k docs");
        time = System.nanoTime();
      }
      T t = deserializer.deserialize(buf.array());
      return new DataConsumer.DataEvent<T>(version, t);
    } catch (IOException ioe) {
      log.error("Cannot read from file!", ioe);
      try { inputChannel.close(); } catch(Exception e) {
        log.warn("Could not close FileChannel: ", e);
      }
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void reset() {
    // noop!
  }
}
