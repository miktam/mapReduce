package com.developand;

import java.util.Map;

public interface MapReduce {
	
	Map<String, Integer> mapReduce();
	
	Map<String, Integer>  simpleWordCounting();
	
	Map<String, Integer> sortMap(Map<String, Integer> mapToSort);
	void readFile(String pathToFile);
	void displayMap(Map<String, Integer> contedMap, int values);

}
