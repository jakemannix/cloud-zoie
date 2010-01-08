package proj.zoie.impl.indexing;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;

import java.text.DecimalFormat;


public class IndexableTweeterpreter implements ZoieIndexableInterpreter<Tweet> {

  public static class TweetIndexable extends AbstractZoieIndexable {
    private char[] tweetChars;

    private long uid;
    private long createdAt;
    private int numFollowers;
    private String screenName;
    private String text;

    private static int count = 0;

    public TweetIndexable(byte[] b) {
      tweetChars = new char[b.length];
      for(int i=0; i<b.length; i++)
      {
        tweetChars[i] = (char)b[i];
      }
System.out.println(new String(tweetChars).replace(' ', '.').replace('\n','*').replace('\r','&'));
for(int i=0; i<b.length; i++) { System.out.print(i%10);}
      int c = 0;
      try{
      uid = Long.parseLong(new String(tweetChars, c, 12).trim());
      c += 13;
      createdAt = Long.parseLong(new String(tweetChars, c, 12).trim());
      c += 12;
      numFollowers = Integer.parseInt(new String(tweetChars, c, 12).trim());
      c += 12;
      screenName = new String(tweetChars, c, 20);
      c += 20;
      text = new String(tweetChars, c, 140);
      } catch(RuntimeException e) {
        System.out.println("\nFailure!!!" + e.getMessage());
      }
System.out.println("\nSuccess!");
if((count++ % 10000) == 0) System.out.println("\nProcessed " + (count) + " tweets so far.");
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
      doc.add(new Field("content", text, Field.Store.YES, Field.Index.ANALYZED));
      doc.add(new Field("user", screenName, Field.Store.YES, Field.Index.ANALYZED_NO_NORMS));
      doc.add(new Field("num_followers", pad(numFollowers), Field.Store.NO, Field.Index.ANALYZED_NO_NORMS));
      doc.add(new Field("timestamp", pad(createdAt), Field.Store.NO, Field.Index.ANALYZED_NO_NORMS));
  //    doc.add(new NumericField("num_followers", Field.Store.NO, true).setIntValue(numFollowers));
  //    doc.add(new NumericField("timestamp", Field.Store.NO, true).setLongValue(createdAt));
      return doc;
    }

    @Override
    public long getUID() {
      return uid;
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
    return new TweetIndexable(src.data);
  }
}
