package proj.zoie.example.service;

import java.io.IOException;


public class Tweet {
  private char[] tweetChars;

  private long uid;
  private long createdAt;
  private int numFollowers;
  private String screenName;
  private String text;

  public Tweet(byte[] b1, byte[] b2, int offset) throws IOException {
    this(combine(b1, b2, offset));
  }

  private static byte[] combine(byte[] b1, byte[] b2, int offset) {
    byte[] newBuf = new byte[b1.length + b2.length];
    System.arraycopy(b1, offset, newBuf, 0, b1.length - offset);
    System.arraycopy(b2, 0, newBuf, offset, b2.length);
    return newBuf;
  }
  static int count = 0;

  public Tweet(byte[] b) throws IOException {
    tweetChars = new char[b.length];
    for(int i=0; i<b.length; i++)
    {
      tweetChars[i] = (char)b[i];
    }
    //System.out.println(new String(tweetChars).replace(' ', '.').replace('\n','*').replace('\r','&'));
    //for(int i=0; i<b.length; i++) { System.out.print(i%10);}
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
      System.out.print("?");
      System.out.flush();
      throw new IOException(e);
    }

    System.out.print(".");
    if((count++ % 1000) == 0) System.out.flush();
    //  if((count++ % 10000) == 0) System.out.println("\nProcessed " + (count) + " tweets so far.");
  }

  public long getUid() {
    return uid;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public int getNumFollowers() {
    return numFollowers;
  }

  public String getScreenName() {
    return screenName;
  }

  public String getText() {
    return text;
  }

  public String toString() {
    return getUid() + " at " + getCreatedAt() + ", "+ getScreenName() + " said:\n" + getText();
  }
}
