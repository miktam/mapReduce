package com.developand;

import java.util.Map;

public interface MapReduce {
	
	void readFile();
	Map<String, Integer> mapReduce();
	String getContent();
	
	Map<String, Integer>  simpleWordCounting();
	
	void sortMap(Map<String, Integer> mapToSort);

}
