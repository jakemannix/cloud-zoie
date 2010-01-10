package proj.zoie.example.service;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class TwitterFileDataProvider extends SingleFileDataProvider<Tweet> {

  public static final int TWEET_SIZE = 200;

  public static final class TweetDeserializer implements Deserializer<Tweet> {
    @Override
    public Tweet deserialize(byte[] bytes) throws IOException {
      byte[] tweetBuf = new byte[bytes.length];
      System.arraycopy(bytes, 0, tweetBuf, 0, bytes.length);
      Tweet t = new Tweet(tweetBuf);
      return t;
    }
  }

  public TwitterFileDataProvider(ReadableByteChannel ch) {
    super(TWEET_SIZE);
    inputChannel = ch;
    bytes = new byte[datumByteSize];
    buf = ByteBuffer.wrap(bytes);
    deserializer = new TweetDeserializer();
  }

  public TwitterFileDataProvider(String dataFileName) throws IOException {
    super(dataFileName, TWEET_SIZE, new TweetDeserializer());
  }
}
