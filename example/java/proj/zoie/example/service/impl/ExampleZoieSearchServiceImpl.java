package proj.zoie.example.service.impl;

import java.io.IOException;
import java.util.*;

import com.browseengine.bobo.api.*;
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
          val[0] = new Date(1000 * Long.parseLong(val[0])).toString();
        }
				map.put(fieldname, val);
			}
		}
		return map;
	}

  private boolean bobo = true;
	
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
				q = qparser.parse(queryString); 
			}
      List<R> allReaders = (List<R>)new LinkedList();
      for(int i=0; i<_idxReaderFactory.length; i++) {
        readers.add(_idxReaderFactory[i].getIndexReaders());
        allReaders.addAll(readers.get(i));
      }
      ScoreDoc[] scoreDocs = null;
      BrowseResult bResult;
      if(bobo) {

        BoboBrowser[] browser = new BoboBrowser[allReaders.size()];
        int i=0;
        for(R reader : allReaders) {
          browser[i] = new BoboBrowser((BoboIndexReader) reader);
        }
        MultiBoboBrowser multiBobo = new MultiBoboBrowser(browser);
        BrowseRequest br = new BrowseRequest();
        FacetSpec nf = new FacetSpec();
        nf.setMaxCount(6);
        br.setFacetSpec("num_followers", nf);
        br.setQuery(q);
        br.setSort(new SortField[] {SortField.FIELD_DOC});

        long start = System.nanoTime();
        bResult = multiBobo.browse(br);
        long end = System.nanoTime();
        result.setTime((end-start)/1000000);
        result.setTotalDocs(bResult.getTotalDocs());
        result.setTotalHits(bResult.getNumHits());
        
      } else {

			  multiReader = new MultiReader(allReaders.toArray(new IndexReader[allReaders.size()]), false);
			  searcher = new IndexSearcher(multiReader);
			
			  long start = System.nanoTime();
			  TopDocs docs = searcher.search(q, null, 10);
			  long end = System.nanoTime();
			
			  result.setTime(((end-start))/1000000);
			  result.setTotalDocs(multiReader.numDocs());
			  result.setTotalHits(docs.totalHits);
			

			  scoreDocs = docs.scoreDocs;
      }
			ArrayList<SearchHit> hitList = new ArrayList<SearchHit>(scoreDocs.length);
      if(bobo) {

      } else {

			  for (ScoreDoc scoreDoc : scoreDocs)
			  {
				  SearchHit hit=new SearchHit();
				  hit.setScore(scoreDoc.score);
				  int docid=scoreDoc.doc;
				
				  Document doc=multiReader.document(docid);
				  String content=doc.get("content");
				
				  Scorer qs=new QueryScorer(q);
				
				  SimpleHTMLFormatter formatter=new SimpleHTMLFormatter("<span class=\"hl\">","</span>");
				  Highlighter hl=new Highlighter(formatter,qs);
				  String[] fragments=hl.getBestFragments(analyzer, "content",content, 1);
				
				  Map<String,String[]> fields=convert(doc);
				  fields.put("fragment",fragments);
				  hit.setFields(fields);
				  hitList.add(hit);
			  }
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
