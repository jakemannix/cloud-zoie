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
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.impl.ZoieMergePolicy;
import proj.zoie.api.impl.ZoieMergePolicy.MergePolicyParams;
import proj.zoie.api.impl.util.IndexUtil;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class DiskSearchIndex<R extends IndexReader> extends BaseSearchIndex<R>{
	  private final File                 _location;
	  private final IndexReaderDispenser<R> _dispenser;
	  
	  final MergePolicyParams _mergePolicyParams;
	  
	  private ZoieIndexDeletionPolicy _deletionPolicy;

	  public static final Logger log = Logger.getLogger(DiskSearchIndex.class);

	  DiskSearchIndex(File location, IndexReaderDecorator<R> decorator)
	  {
	    _location = location;
	    _mergePolicyParams = new MergePolicyParams();
	    _dispenser = new IndexReaderDispenser<R>(_location, decorator);
	    _mergeScheduler = new SerialMergeScheduler();
	    _deletionPolicy = new ZoieIndexDeletionPolicy();
	  }

	  public long getVersion()
	  {
	    return _dispenser.getCurrentVersion();
	  }
	  
	  public MergePolicyParams getMergePolicyParams(){
	     return _mergePolicyParams;
	  }
	  
	  /**
	   * Gets the number of docs in the current loaded index
	   * @return number of docs
	   */
	  public int getNumdocs()
	  {
	    IndexReader reader=_dispenser.getIndexReader();
	    if (reader!=null)
	    {
	      return reader.numDocs();
	    }
	    else
	    {
	      return 0;
	    }
	  }
	  
	  public int getSegmentCount() throws IOException{
		  if (_location == null || !_location.exists()){
			  return 0;
		  }

		  Directory dir = null;
		  try{
			  dir = getIndexDir(_location);
		  }
		  catch(Exception e){
			  return 0;
		  }
		  if (dir == null) return 0;
		  return IndexUtil.getNumSegments(dir);
	  }

	  /**
	   * Close and releases dispenser and clean up
	   */
	  public void close()
	  {
        super.close();
        
        // close the dispenser
	    if (_dispenser != null)
	    {
	        _dispenser.close();
	    }
	  }
	  
	  /**
       * Refreshes the index reader
       */
	  @Override
      public void refresh()
      {
	    synchronized(this)
	    {
	      try {
	        LongSet delDocs = _delDocs;
            clearDeletes();
            _dispenser.getNewReader();
            markDeletes(delDocs); // re-mark deletes
	      } catch (IOException e) {
	        log.error(e.getMessage(),e);
	      }
	    }
      }
      
      public void returnReaders(List<ZoieIndexReader<R>> readers){
    	  _dispenser.returnReaders(readers);
      }

	  @Override
	  protected void finalize()
	  {
	    close();
	  }
	  
	  public static FSDirectory getIndexDir(File location) throws IOException
	  {
		IndexSignature sig = null;
		if (location.exists())
		{
		  sig = IndexReaderDispenser.getCurrentIndexSignature(location);
		}
		  
		if (sig == null)
	    {
	      File directoryFile = new File(location, IndexReaderDispenser.INDEX_DIRECTORY);
	      sig = new IndexSignature(IndexReaderDispenser.INDEX_DIR_NAME, 0L);
	      try
	      {
	        sig.save(directoryFile);
	      }
	      catch (IOException e)
	      {
	        throw e;
	      }
	    }
		

	    File idxDir = new File(location, sig.getIndexPath());
	    FSDirectory directory = FSDirectory.open(idxDir);
	    
	    return directory;
	  }

	  /**
	   * Opens an index modifier.
	   * @param analyzer Analyzer
	   * @return IndexModifer instance
	   */
	  public IndexWriter openIndexWriter(Analyzer analyzer,Similarity similarity) throws IOException
	  {
	    if(_indexWriter != null) return _indexWriter;
	    
	    // create the parent directory
	    _location.mkdirs();
	    
	    FSDirectory directory = getIndexDir(_location);

	    log.info("opening index writer at: "+directory.getFile().getAbsolutePath());
	    
	    // create a new modifier to the index, assuming at most one instance is running at any given time
	    boolean create = !IndexReader.indexExists(directory);  
	    IndexWriter idxWriter = new IndexWriter(directory, analyzer, create, _deletionPolicy, MaxFieldLength.UNLIMITED);
        idxWriter.setMergeScheduler(_mergeScheduler);
        
        ZoieMergePolicy mergePolicy = new ZoieMergePolicy(idxWriter);
        mergePolicy.setMergePolicyParams(_mergePolicyParams);
        idxWriter.setRAMBufferSizeMB(5);
	    
        idxWriter.setMergePolicy(mergePolicy);
	    
	    if (similarity != null)
	    {
	    	idxWriter.setSimilarity(similarity);
	    }
	    _indexWriter = idxWriter;
	    return idxWriter;
	  }
	  
	  /**
	   * Gets the current reader
	   */
	  public ZoieIndexReader<R> openIndexReader() throws IOException
	  {
	    // use dispenser to get the reader
	    return _dispenser.getIndexReader();
	  
	  }
	  
	  
	  @Override
	  protected IndexReader openIndexReaderForDelete() throws IOException {
		_location.mkdirs();
		FSDirectory directory = getIndexDir(_location);
		if (IndexReader.indexExists(directory)){		
			return IndexReader.open(directory,false);
		}
		else{
			return null;
		}
	  }

	/**
	   * Gets a new reader, force a reader refresh
	   * @return
	   * @throws IOException
	   */
	  public ZoieIndexReader<R> getNewReader() throws IOException
	  {
        synchronized(this)
        {
          refresh();
          commitDeletes();
          ZoieIndexReader<R> reader = _dispenser.getIndexReader();
          return reader;
        }
	  }
	  
	  /**
	   * Writes the current version/SCN to the disk
	   */
	  public void setVersion(long version)
	      throws IOException
	  {
	    // update new index file
	    File directoryFile = new File(_location, IndexReaderDispenser.INDEX_DIRECTORY);
	    IndexSignature sig = IndexSignature.read(directoryFile);
	    sig.updateVersion(version);
	    try
	    {
	      // make sure atomicity of the index publication
	      File tmpFile = new File(_location, IndexReaderDispenser.INDEX_DIRECTORY + ".new");
	      sig.save(tmpFile);
	      File tmpFile2 = new File(_location, IndexReaderDispenser.INDEX_DIRECTORY + ".tmp");
	      directoryFile.renameTo(tmpFile2);
	      tmpFile.renameTo(directoryFile);
	      tmpFile2.delete();
	    }
	    catch (IOException e)
	    {
	      throw e;
	    }

	  }
	  
	  public DiskIndexSnapshot getSnapshot()
	  {
	    IndexSignature sig = IndexReaderDispenser.getCurrentIndexSignature(_location);
	    if(sig != null)
	    {
	      ZoieIndexDeletionPolicy.Snapshot snapshot = _deletionPolicy.getSnapshot();
	      if(snapshot != null)
	      {
	        return new DiskIndexSnapshot(sig, snapshot);
	      }
	    }
	    return null;
	  }
	  
	  public void importSnapshot(ReadableByteChannel channel) throws IOException
	  {
        File idxDir = new File(_location, IndexReaderDispenser.INDEX_DIR_NAME);
        idxDir.mkdirs();
        
        DiskIndexSnapshot.readSnapshot(channel, _location);
	  }
}
