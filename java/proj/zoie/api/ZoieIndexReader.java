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
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.index.FilterIndexReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.store.Directory;

import proj.zoie.api.indexing.IndexReaderDecorator;

public abstract class ZoieIndexReader<R extends IndexReader> extends FilterIndexReader {
	public static final long DELETED_UID = Long.MIN_VALUE;
	
	protected ThreadLocal<int[]> _delDocIds;
	protected long _minUID;
	protected long _maxUID;
	protected boolean _noDedup = false;
    protected final IndexReaderDecorator<R> _decorator;
	
	
	public static <R extends IndexReader> ZoieIndexReader<R> open(IndexReader r) throws IOException{
		return open(r,null);
	}
	
	public static <R extends IndexReader> ZoieIndexReader<R> open(IndexReader r,IndexReaderDecorator<R> decorator) throws IOException{
		return new ZoieMultiReader<R>(r,decorator);
	}
	
	public static <R extends IndexReader> ZoieIndexReader<R> open(Directory dir,IndexReaderDecorator<R> decorator) throws IOException{
		IndexReader r = IndexReader.open(dir, true);
		// load zoie reader
		try{
			return open(r,decorator);
		}
		catch(IOException ioe){
			if (r!=null){
				r.close();
			}
			throw ioe;
		}
	}
		
	protected ZoieIndexReader(IndexReader in,IndexReaderDecorator<R> decorator) throws IOException
	{
		super(in);
		_decorator = decorator;
		_delDocIds=new ThreadLocal<int[]>();
		_minUID=Long.MAX_VALUE;
		_maxUID=0;
	}
	
	abstract public List<R> getDecoratedReaders() throws IOException;
    abstract public void setDelDocIds();
    abstract public void markDeletes(LongSet delDocs, LongSet deltedUIDs);
    abstract public void commitDeletes();
	     
	public IndexReader getInnerReader(){
		return in;
	}
	
	@Override
	public boolean hasDeletions()
	{
	  if(!_noDedup)
	  {
		int[] delSet = _delDocIds.get();
	    if(delSet != null && delSet.length > 0) return true;
	  }
	  return in.hasDeletions();
	}
	
	protected abstract boolean hasIndexDeletions();
	
	public boolean hasDuplicates()
	{
		int[] delSet = _delDocIds.get();
		return (delSet!=null && delSet.length > 0);
	}

	@Override
	abstract public boolean isDeleted(int docid);
	
	public boolean isDuplicate(int uid)
	{
	  int[] delSet = _delDocIds.get();
	  return delSet!=null && Arrays.binarySearch(delSet, uid) >= 0;
	}
	
	public int[] getDelDocIds()
	{
	  return _delDocIds.get();
	}
	
	public long getMinUID()
	{
		return _minUID;
	}
	
	public long getMaxUID()
	{
		return _maxUID;
	}

	abstract public long getUID(int docid);
	
	abstract public DocIDMapper getDocIDMaper();
	
	public void setNoDedup(boolean noDedup)
	{
	  _noDedup = noDedup;
	}

	@Override
	abstract public ZoieIndexReader<R>[] getSequentialSubReaders();
	
	@Override
	abstract public TermDocs termDocs() throws IOException;
	
	@Override
	abstract public TermPositions termPositions() throws IOException;
	
}
