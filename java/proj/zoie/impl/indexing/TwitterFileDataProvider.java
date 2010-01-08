package proj.zoie.impl.indexing;


import java.io.IOException;

public class TwitterFileDataProvider extends SingleFileDataProvider<Tweet> {

  private static final int TWEET_SIZE = 200;

  private static final class TweetDeserializer implements Deserializer<Tweet> {
    @Override
    public Tweet deserialize(byte[] bytes) {
      byte[] tweetBuf = new byte[bytes.length];
      System.arraycopy(bytes, 0, tweetBuf, 0, bytes.length);
      Tweet t = new Tweet();
      t.data = tweetBuf;
      return t;
    }
  }

  public TwitterFileDataProvider(String dataFileName) throws IOException {
    super(dataFileName, TWEET_SIZE, new TweetDeserializer());
  }
}
