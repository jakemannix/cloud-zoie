package proj.zoie.example.service;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;

import java.text.DecimalFormat;


public class IndexableTweeterpreter implements ZoieIndexableInterpreter<Tweet> {

  public static class TweetIndexable extends AbstractZoieIndexable {
    private Tweet tweet;

    public TweetIndexable(Tweet t) {
      tweet = t;
    }

		public IndexingReq[] buildIndexingReqs(){
			IndexingReq req = new IndexingReq(buildDocument(), null);
			return new IndexingReq[]{req};
		}

    private static final DecimalFormat formatter = new DecimalFormat("0000000000"); // make this as wide as you need
    public static String pad(long n) {
      return formatter.format(n);
    }

		public Document buildDocument()
		{
			Document doc = new Document();
      doc.add(new Field("content", tweet.getText(), Field.Store.YES, Field.Index.ANALYZED_NO_NORMS));
      doc.add(new Field("user", tweet.getScreenName(), Field.Store.YES, Field.Index.ANALYZED_NO_NORMS));
      doc.add(new Field("num_followers", pad(tweet.getNumFollowers()), Field.Store.YES, Field.Index.ANALYZED_NO_NORMS));
      doc.add(new Field("timestamp", pad(System.currentTimeMillis() / 1000), Field.Store.YES, Field.Index.ANALYZED_NO_NORMS));
  //    doc.add(new NumericField("num_followers", Field.Store.NO, true).setIntValue(numFollowers));
  //    doc.add(new NumericField("timestamp", Field.Store.NO, true).setLongValue(createdAt));
      return doc;
    }

    @Override
    public long getUID() {
      return tweet.getUid();
    }

    @Override
    public boolean isDeleted() {
      return false;
    }

    @Override
    public boolean isSkip() {
      return false;
    }
  }

  @Override
  public ZoieIndexable convertAndInterpret(Tweet src) {
    //System.out.print("*");
    return new TweetIndexable(src);
  }
}
