package proj.zoie.impl.indexing;
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

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.indexing.ZoieIndexableInterpreter;

public class SimpleZoieSystem<V> extends ZoieSystem<IndexReader,V> {

	/**
	 * @param idxDir
	 * @param interpreter
	 * @param batchSize
	 * @param batchDelay
	 */
	public SimpleZoieSystem(File idxDir, ZoieIndexableInterpreter<V> interpreter, int batchSize, long batchDelay) {
		super(idxDir, interpreter, new DefaultIndexReaderDecorator(), null, null, batchSize, batchDelay, true);
	}

}
