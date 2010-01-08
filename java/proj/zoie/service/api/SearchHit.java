package proj.zoie.service.api;
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
import java.io.Serializable;
import java.util.Map;

public class SearchHit implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private float score;
	private Map<String,String[]> fields;
	
	public SearchHit()
	{
		
	}
	
	public float getScore() {
		return score;
	}
	
	public void setScore(float score) {
		this.score = score;
	}
	
	public Map<String, String[]> getFields() {
		return fields;
	}
	
	public void setFields(Map<String, String[]> fields) {
		this.fields = fields;
	}
}
