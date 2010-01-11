package proj.zoie.example.service.impl;

import java.io.IOException;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.Scorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.util.Version;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.example.service.Tweet;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.service.api.SearchHit;
import proj.zoie.service.api.SearchRequest;
import proj.zoie.service.api.SearchResult;
import proj.zoie.service.api.ZoieSearchService;

public class ExampleZoieSearchServiceImpl<R extends IndexReader> implements ZoieSearchService {

	private static final Logger log = Logger.getLogger(ExampleZoieSearchServiceImpl.class);
	
	private IndexReaderFactory<R>[] _idxReaderFactory;
	
	public ExampleZoieSearchServiceImpl(IndexReaderFactory<R> idxReaderFactory1, IndexReaderFactory<R> idxReaderFactory2){
		_idxReaderFactory = new IndexReaderFactory[] { idxReaderFactory1, idxReaderFactory2 };
	}

  public ExampleZoieSearchServiceImpl(IndexReaderFactory<R> idx) {
    _idxReaderFactory = new IndexReaderFactory[] { idx };
  }

  public ExampleZoieSearchServiceImpl(ZoieSystem<R, Tweet> zoie1, ZoieSystem<R, Tweet> zoie2) {
    this((IndexReaderFactory<R>)zoie1, (IndexReaderFactory<R>)zoie2);
  }

  private static final class HC extends Collector {
    ZoieIndexReader r;
    int docBase;
   
    @Override
    public void setScorer(org.apache.lucene.search.Scorer scorer) throws IOException {
    }

    @Override
    public void collect(int i) throws IOException {

    }

    @Override
    public void setNextReader(IndexReader indexReader, int docBase) throws IOException {
      r = (ZoieIndexReader)indexReader;
      this.docBase = docBase;
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }
  }

	private static Map<String,String[]> convert(Document doc)
	{
		Map<String,String[]> map = new HashMap<String,String[]>();
		if (doc != null)
		{
			List<Fieldable> fields = (List<Fieldable>)doc.getFields();
			Iterator<Fieldable> iter = fields.iterator();
			while(iter.hasNext())
			{
				Fieldable fld = iter.next();
				String fieldname = fld.name();
        String[] val = doc.getValues(fieldname);
        if(fieldname.equals("num_followers")) {
          for(int i=0; i<val.length; i++) {
            int j = 0;
            for(j = 0; j<val[i].length(); j++) {
              if(val[i].charAt(j) != '0') break;
            }
            val[i] = val[i].substring(j);
          }
        } else if(fieldname.equals("timestamp")) {
          long tweetSeconds = Long.parseLong(val[0]);
          long timeDiff = System.currentTimeMillis()/1000 - tweetSeconds;
          String diff = "";
          if(timeDiff < 60) {
            diff = timeDiff + " seconds";
          } else if(timeDiff < 60*60) {
            diff = (timeDiff / 60) + " minutes, " + (timeDiff % 60) + " seconds";
          } else if(timeDiff < 60*60*24) {
            diff = (timeDiff / 60*60) + "hours, " + (timeDiff % (60*60)) + " minutes";
          } else if(timeDiff < 60*60*24*7) {
            diff = (timeDiff / 60*60*24) + " days, " + (timeDiff % (60*60*24)) + " hours";
          } else {
            diff = (timeDiff / 60*60*24*7) + " weeks, " + (timeDiff % (60*60*24*7)) + " days";
          }
          val[0] = diff;
        }
				map.put(fieldname, val);
			}
		}
		return map;
	}
	
	public SearchResult search(SearchRequest req) throws ZoieException{
		String queryString = req.getQuery();
		Analyzer analyzer = _idxReaderFactory[0].getAnalyzer();
		QueryParser qparser = new QueryParser(Version.LUCENE_CURRENT, "content", analyzer);
		
		SearchResult result = new SearchResult();
		
		List<List<R>> readers = new ArrayList<List<R>>();

		MultiReader multiReader = null;
		Searcher searcher = null;
		try
		{
			Query q = null;
			if (queryString == null || queryString.length() ==0)
			{
				q = new MatchAllDocsQuery();
			}
			else
			{
				q = new ConstantScoreQuery(new QueryWrapperFilter(qparser.parse(queryString))); 
			}

      List<R> allReaders = (List<R>)new LinkedList();

      for(int i=0; i<_idxReaderFactory.length; i++) {
        readers.add(_idxReaderFactory[i].getIndexReaders());
        allReaders.addAll(readers.get(i));
      }

      ScoreDoc[] scoreDocs = null;

      multiReader = new MultiReader(allReaders.toArray(new IndexReader[allReaders.size()]), false);
      searcher = new IndexSearcher(multiReader);

      long start = System.nanoTime();
      TopDocs docs = searcher.search(q, null, 50, Sort.INDEXORDER);
      long end = System.nanoTime();

      result.setTime(((end-start))/1000000);
      result.setTotalDocs(multiReader.numDocs());
      result.setTotalHits(docs.totalHits);


      scoreDocs = docs.scoreDocs;
			ArrayList<SearchHit> hitList = new ArrayList<SearchHit>(scoreDocs.length);

      for (ScoreDoc scoreDoc : scoreDocs)
      {
        SearchHit hit = new SearchHit();
        hit.setScore(scoreDoc.score);
        int docid = scoreDoc.doc;

        Document doc = multiReader.document(docid);
        String content = doc.get("content");

        Scorer qs = new QueryScorer(q);

        //SimpleHTMLFormatter formatter = new SimpleHTMLFormatter("<span class=\"hl\">","</span>");
        //Highlighter hl = new Highlighter(formatter,qs);
        String[] fragments = new String[] { content }; //hl.getBestFragments(analyzer, "content", content, 1);

        Map<String,String[]> fields = convert(doc);
        fields.put("fragment",fragments);
        hit.setFields(fields);
        hitList.add(hit);
      }

			
			result.setHits(hitList.toArray(new SearchHit[hitList.size()]));
      System.out.println("queryString: " + queryString + "\n" + result.toString());
			return result;
		}
		catch(Exception e)
		{
			log.error(e.getMessage(),e);
			throw new ZoieException(e.getMessage(),e);
		}
		finally
		{
			try{
			  if (searcher!=null)
			  {
				try {
					searcher.close();
				} catch (IOException e) {
					log.error(e.getMessage(),e);
				}
			  }
			}
			finally{
        for(int i=0; i<_idxReaderFactory.length; i++) {
			    _idxReaderFactory[i].returnIndexReaders(readers.get(i));
        }
			}
		}
	}
}
