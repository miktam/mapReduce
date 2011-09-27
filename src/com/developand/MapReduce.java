package com.developand;

import java.util.Map;

public interface MapReduce {

	Map<Object, Integer> mapReduce();

	Map<String, Integer> simpleWordCounting();

	Map<Object, Integer> sortMap(Map<Object, Integer> mapToSort);

	void readFile(String pathToFile);

	void displayMap(Map<Object, Integer> map, int values);

}
