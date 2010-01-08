package proj.zoie.impl.indexing.internal;
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
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class IndexReaderDispenser<R extends IndexReader>{
	private static final Logger log = Logger.getLogger(IndexReaderDispenser.class);
	private static final int NUM_GENERATIONS = 3;
	
	private static final int INDEX_OPEN_NUM_RETRIES=5;
	
	public static final String  INDEX_DIRECTORY = "index.directory";
	public static final String   INDEX_DIR_NAME = "beef";
	
    static final class InternalIndexReader<R extends IndexReader> extends ZoieMultiReader<R>{
		//private IndexSignature _sig;
		private final IndexReaderDispenser<R> _dispenser;
		private final AtomicInteger _refCount = new AtomicInteger(0);
		private long _generation;
		
		InternalIndexReader(IndexReader in,IndexReaderDecorator<R> decorator,IndexReaderDispenser<R> dispenser,long generation) throws IOException
		{
			super(in, decorator);
			_dispenser = dispenser;
			_generation = generation;
		}
		
		public InternalIndexReader(IndexReader in, IndexReader[] subReaders,
				IndexReaderDecorator<R> decorator,IndexReaderDispenser<R> dispenser,long generation) throws IOException {
			super(in, subReaders, decorator);
			_dispenser = dispenser;
			_generation = generation;
		}

		@Override
		protected ZoieMultiReader<R> newInstance(IndexReader inner,
				IndexReader[] subReaders) throws IOException {
			return new InternalIndexReader<R>(inner,subReaders,_decorator,_dispenser,_generation);
		}
		
		int incrementRef(){
			if (_refCount.get()>=0){
			   return _refCount.incrementAndGet();
			}
			else{
				return _refCount.get();
			}
		}
		
		int decrementRef(){
			if (_refCount.get()>0){
			  return _refCount.decrementAndGet();
			}
			else{
				return _refCount.get();
			}
		}
	}
	
	/**
	   * Gets the current signature
	   * @param indexHome
	   * @return
	   */
	public static IndexSignature getCurrentIndexSignature(File indexHome)
	{
	    File directoryFile = new File(indexHome, INDEX_DIRECTORY);
	    IndexSignature sig=IndexSignature.read(directoryFile);
	    return sig;
	}

    private volatile InternalIndexReader<R> _currentReader;
    private volatile IndexSignature _currentSignature;
	private final IndexReaderDecorator<R> _decorator;
	private final File _indexHome;
	private long _generation;
	private final ConcurrentLinkedQueue<InternalIndexReader<R>> _destroyQueue;
	
	public IndexReaderDispenser(File indexHome, IndexReaderDecorator<R> decorator)
	{
	  _indexHome = indexHome;
	  _decorator = decorator;
	  _currentSignature = null;
	  _generation = 0L;
	  _destroyQueue = new ConcurrentLinkedQueue<InternalIndexReader<R>>();
	  IndexSignature sig = getCurrentIndexSignature(_indexHome);
	  if(sig != null)
	  {
	    try
	    {
	      getNewReader();
	    }
	    catch (IOException e)
	    {
	      log.error(e);
	    }
	  }
	}
	
	public long getCurrentVersion()
	{
		return _currentSignature!=null ? _currentSignature.getVersion(): 0L;
	}
	
	/**
	   * constructs a new IndexReader instance
	   * 
	   * @param indexPath
	   *            Where the index is.
	   * @return Constructed IndexReader instance.
	   * @throws IOException
	   */
	  private InternalIndexReader<R> newReader(File luceneDir, IndexReaderDecorator<R> decorator, IndexSignature signature)
	      throws IOException
	  {
	    if (!luceneDir.exists()){
	      return null;
	    }
	    
		Directory dir=FSDirectory.open(luceneDir);
		
		if (!IndexReader.indexExists(dir)){
			return null;
		}
		
	    int numTries=INDEX_OPEN_NUM_RETRIES;
	    InternalIndexReader<R> reader=null;
	    
	    // try max of 5 times, there might be a case where the segment file is being updated
	    while(reader==null)
	    {
	      if (numTries==0)
	      {
	        log.error("Problem refreshing disk index, all attempts failed.");
	        throw new IOException("problem opening new index");
	      }
	      numTries--;
	      
	      try{
	        if(log.isDebugEnabled())
	        {
	          log.debug("opening index reader at: "+luceneDir.getAbsolutePath());
	        }
	        IndexReader srcReader = IndexReader.open(dir,true);
	        
	        try
	        {
	          reader=new InternalIndexReader<R>(srcReader, decorator,this,_generation);
	          _currentSignature = signature;
	        }
	        catch(IOException ioe)
	        {
	          // close the source reader if InternalIndexReader construction fails
	          if (srcReader!=null)
	          {
	            srcReader.close();
	          }
	          throw ioe;
	        }
	      }
	      catch(IOException ioe)
	      {
	        try
	        {
	          Thread.sleep(100);
	        }
	        catch (InterruptedException e)
	        {
	          log.warn("thread interrupted.");
	          continue;
	        }
	      }
	    }
	    return reader;
	  }

	  /**
	   * get a fresh new reader instance
	   * @return an IndexReader instance, can be null if index does not yet exit
	   * @throws IOException
	   */
	  public ZoieIndexReader<R> getNewReader() throws IOException
	  {
	      int numTries=INDEX_OPEN_NUM_RETRIES;   
	      InternalIndexReader<R> reader=null;
	            
	      // try it for a few times, there is a case where lucene is swapping the segment file, 
	      // or a case where the index directory file is updated, both are legitimate,
	      // trying again does not block searchers,
	      // the extra time it takes to get the reader, and to sync the index, memory index is collecting docs
	     
	      while(reader==null)
	      {
	        if (numTries==0)
	        {
	        	break;
	        }
	        numTries--;
	        try{
	          IndexSignature sig = getCurrentIndexSignature(_indexHome);
	    
	          if (sig==null)
	          {
	            throw new IOException("no index exist");
	          }
	          
	          if (_currentReader==null){
	            String luceneDir = sig.getIndexPath();
	    
	            if (luceneDir == null || luceneDir.trim().length() == 0)
	            {
	              throw new IOException(INDEX_DIRECTORY + " contains no data.");
	            }
	          
	            if (luceneDir != null)
	            {
	        	  reader = newReader(new File(_indexHome,luceneDir), _decorator, sig);
	              break;
	            } 
	          }
	          else{
	        	  reader = (InternalIndexReader<R>)_currentReader.reopen(true);
	        	  reader._generation=_generation;
	        	  _currentSignature = sig;
	          }
	        }
	        catch(IOException ioe)
	        {
	          try
	          {
	            Thread.sleep(100);
	          }
	          catch (InterruptedException e)
	          {
	        	log.warn("thread interrupted.");
	            continue;
	          }
	        }
	      }
	      
	      _generation++;
	      // swap the internal readers
	      _currentReader = reader;
	      
	      log.info("collecting and closing old readers...("+_destroyQueue.size()+"), gen: "+_generation);
	      int numReadersCollected = 0;
	      Iterator<InternalIndexReader<R>> iter = _destroyQueue.iterator();
	      while(iter.hasNext()){
	    	InternalIndexReader<R> r = iter.next();
	    	log.info(r+" - candiate for collection: "+r._generation+"/"+r._refCount.get());
	  		if (r._generation <= (_generation-NUM_GENERATIONS) && r._refCount.get() == 0){
	  			try {
	  				r.close();
	  				
	  				List<R> decoratedReaders = r.getDecoratedReaders();
	  				if (decoratedReaders!=null){
	  					for (R decR : decoratedReaders){
	  						try{
	  							decR.close();
	  						}
	  						catch(AlreadyClosedException ioe){
	  							// read already closed
	  						}
	  					}
	  				}
	  				
	  				iter.remove();
	  				numReadersCollected++;
	  			} catch (AlreadyClosedException ace) {
	  				log.error(ace.getMessage(),ace);
	  			}
	  		}
	      }

	      log.info("done collecting and closing old readers... ("+numReadersCollected+"/"+_destroyQueue.size()+")");
	      
	      if (_currentReader!=null){
	        _destroyQueue.add(_currentReader);
	      }
	      
	      return reader;
	    }
	
	public ZoieIndexReader<R> getIndexReader()
	{
		if (_currentReader!=null){
		  return _currentReader;
		}
		else{
		  return null;
		}
	}
		
	public void returnReaders(List<ZoieIndexReader<R>> readers){
		for (ZoieIndexReader<R> r : readers){
			if (r instanceof InternalIndexReader<?>){
				((InternalIndexReader<R>) r).decrementRef();
			}
		}
	}
	
	/**
	 * Closes the factory.
	 * 
	 */
	public void close()
	{
	  closeReader();
	}
	
	/**
	 * Closes the index reader
	 */
	public void closeReader()
	{
	  if(_currentReader != null)
	  {
	    try
	    {
	      _currentReader.close();
	    }
	    catch(IOException e)
	    {
	      log.error("problem closing reader", e);
	    }
	    _currentReader = null;
	  }
	}
	
	protected void finalize()
	{
	  close();
	}
}
