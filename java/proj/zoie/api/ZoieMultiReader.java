package proj.zoie.api;
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

import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiZoieTermDocs;
import org.apache.lucene.index.MultiZoieTermPositions;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermPositions;

import proj.zoie.api.indexing.IndexReaderDecorator;

public class ZoieMultiReader<R extends IndexReader> extends ZoieIndexReader<R> {

	private Map<String,ZoieSegmentReader<R>> _readerMap;
	private ArrayList<ZoieSegmentReader<R>> _subZoieReaders;
	private int[] _starts;
	private List<R> _decoratedReaders;
	
	public ZoieMultiReader(IndexReader in,IndexReaderDecorator<R> decorator) throws IOException {
		super(in,decorator);
		_readerMap = new HashMap<String,ZoieSegmentReader<R>>();
		_decoratedReaders = null; 
		IndexReader[] subReaders = in.getSequentialSubReaders();
		init(subReaders);
	}

	public ZoieMultiReader(IndexReader in,IndexReader[] subReaders,IndexReaderDecorator<R> decorator) throws IOException {
		super(in,decorator);
		_readerMap = new HashMap<String,ZoieSegmentReader<R>>();
		_decoratedReaders = null; 
		init(subReaders);
	}
	
	private void init(IndexReader[] subReaders) throws IOException{
		_subZoieReaders = new ArrayList<ZoieSegmentReader<R>>(subReaders.length);
		_starts = new int[subReaders.length+1];
		int i = 0;
		int startCount=0;
		for (IndexReader subReader : subReaders){
			ZoieSegmentReader<R> zr=null;
			if (subReader instanceof ZoieSegmentReader<?>){
				zr = (ZoieSegmentReader<R>)subReader;
			}
			else if (subReader instanceof SegmentReader){
				SegmentReader sr = (SegmentReader)subReader;
				zr = new ZoieSegmentReader<R>(sr,_decorator);
			}
			if (zr!=null){
			    String segmentName = zr.getSegmentName();
			    _readerMap.put(segmentName, zr);
				_subZoieReaders.add(zr);
				_starts[i]=startCount;
				i++;
				startCount+=zr.maxDoc();
			}
			else{
				throw new IllegalStateException("subreader not instance of "+SegmentReader.class);
			}
		}
		_starts[subReaders.length]=in.maxDoc();
		
		ArrayList<R> decoratedList = new ArrayList<R>(_subZoieReaders.size());
    	for (ZoieSegmentReader<R> subReader : _subZoieReaders){
    		R decoratedReader = subReader.getDecoratedReader();
    		decoratedList.add(decoratedReader);
    	}
    	_decoratedReaders = decoratedList;
	}
	
	@Override
	public long getUID(int docid)
	{
		int idx = readerIndex(docid);
		ZoieIndexReader<R> subReader = _subZoieReaders.get(idx);
		return subReader.getUID(docid-_starts[idx]);
	}
	

	@SuppressWarnings("unchecked")
	@Override
	public ZoieIndexReader<R>[] getSequentialSubReaders() {
		return (ZoieIndexReader<R>[])(_subZoieReaders.toArray(new ZoieIndexReader[_subZoieReaders.size()]));
	}
	
	@Override
	public void markDeletes(LongSet delDocs, LongSet deletedUIDs)
	{
	  ZoieIndexReader<R>[] subReaders = getSequentialSubReaders();
	  if(subReaders != null && subReaders.length > 0)
      {
	    for(int i = 0; i < subReaders.length; i++)
	    {
	      ZoieSegmentReader<R> subReader = (ZoieSegmentReader)subReaders[i];
	      subReader.markDeletes(delDocs, deletedUIDs);
        }
      }
	}
	
	@Override
	public void commitDeletes()
	{
	  ZoieIndexReader<R>[] subReaders = getSequentialSubReaders();
	  if(subReaders != null && subReaders.length > 0)
	  {
	    for(int i = 0; i < subReaders.length; i++)
	    {
	      ZoieSegmentReader<R> subReader = (ZoieSegmentReader)subReaders[i];
	      subReader.commitDeletes();
	    }
	  }
	}
	
	@Override
	public void setDelDocIds()
	{
	  ZoieIndexReader<R>[] subReaders = getSequentialSubReaders();
	  for(ZoieIndexReader<R> subReader : subReaders)
	  {
	    subReader.setDelDocIds();
	  }
	}

	@Override
	public List<R> getDecoratedReaders() throws IOException{
	      return _decoratedReaders;
	}
	
	@Override
	protected boolean hasIndexDeletions(){
		for (ZoieSegmentReader<R> subReader : _subZoieReaders){
			if (subReader.hasIndexDeletions()) return true;
		}
		return false;
	}
	
	@Override
	public boolean isDeleted(int docid){
	   int idx = readerIndex(docid);
	   ZoieIndexReader<R> subReader = _subZoieReaders.get(idx);
	   return subReader.isDeleted(docid-_starts[idx]);
	}
	
	private int readerIndex(int n){
		return readerIndex(n,_starts,_starts.length);
	}
	
	final static int readerIndex(int n, int[] starts, int numSubReaders) {    // find reader for doc n:
	    int lo = 0;                                      // search starts array
	    int hi = numSubReaders - 1;                  // for first element less

	    while (hi >= lo) {
	      int mid = (lo + hi) >>> 1;
	      int midValue = starts[mid];
	      if (n < midValue)
	        hi = mid - 1;
	      else if (n > midValue)
	        lo = mid + 1;
	      else {                                      // found a match
	        while (mid+1 < numSubReaders && starts[mid+1] == midValue) {
	          mid++;                                  // scan to last match
	        }
	        return mid;
	      }
	    }
	    return hi;
	  }

	@Override
	public TermDocs termDocs() throws IOException {
		return new MultiZoieTermDocs(this,_subZoieReaders.toArray(new ZoieIndexReader<?>[_subZoieReaders.size()]),_starts);
	}

	@Override
	public TermPositions termPositions() throws IOException {
		return new MultiZoieTermPositions(this,_subZoieReaders.toArray(new ZoieIndexReader<?>[_subZoieReaders.size()]),_starts);
	}

	@Override
	public DocIDMapper getDocIDMaper() {
		return new DocIDMapper() {
			public int getDocID(long uid) {
				for (int i = 0; i < _subZoieReaders.size(); ++i){
					ZoieIndexReader<R> subReader = _subZoieReaders.get(i);
					int docid = subReader.getDocIDMaper().getDocID(uid);
					if (docid!=DocIDMapper.NOT_FOUND) {
						return docid+_starts[i];
					}
				}
				return DocIDMapper.NOT_FOUND;
			}
		};
	}
	
	
	/*
	@Override
	protected void doClose() throws IOException {
		
		try{
			super.doClose();
		}
		finally{
		  for (ZoieSegmentReader<R> r : _subZoieReaders){
			r.close();
		  }
		}
	}
*/
	
	@Override
	public synchronized ZoieIndexReader<R> reopen(boolean openReadOnly)
			throws CorruptIndexException, IOException {

		long version = in.getVersion();
		IndexReader inner = in.reopen(openReadOnly);
		if (inner == in && inner.getVersion()==version){
			return this;
		}
		
		IndexReader[] subReaders = inner.getSequentialSubReaders();
		ArrayList<IndexReader> subReaderList = new ArrayList<IndexReader>(subReaders.length);
		for (IndexReader subReader : subReaders){
			if (subReader instanceof SegmentReader){
				SegmentReader sr = (SegmentReader)subReader;
				String segmentName = sr.getSegmentName();
				ZoieSegmentReader<R> zoieSegmentReader = _readerMap.get(segmentName);
				if (zoieSegmentReader!=null){
					int numDocs = sr.numDocs();
					int maxDocs = sr.maxDoc();
					if (zoieSegmentReader.numDocs() != numDocs || zoieSegmentReader.maxDoc() != maxDocs){
						// segment has changed
						zoieSegmentReader = new ZoieSegmentReader<R>(sr,_decorator);
					}
					else{
						zoieSegmentReader = new ZoieSegmentReader<R>(zoieSegmentReader,sr);
					}
				}
				else{
					zoieSegmentReader = new ZoieSegmentReader<R>(sr,_decorator);
				}
				subReaderList.add(zoieSegmentReader);
			}
			else{
				throw new IllegalStateException("reader not insance of "+SegmentReader.class);
			}
		}
		
		return newInstance(inner, subReaderList.toArray(new IndexReader[subReaderList.size()]));
	}
	
	protected ZoieMultiReader<R> newInstance(IndexReader inner,IndexReader[] subReaders) throws IOException{
		return new ZoieMultiReader<R>(inner, subReaders ,_decorator);
	}
}
