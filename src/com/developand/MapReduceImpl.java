package com.developand;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class MapReduceImpl implements MapReduce {

	String slurpedFile = new String();
	private final int BUCKET_SIZE = 16;
	
	private final Logger log = Logger.getLogger(MapReduceImpl.class);

	// no multimaps so create so weird construction
	List<Map.Entry<String, Integer>> intermediateMap = new ArrayList<Map.Entry<String, Integer>>();

	@Override
	public Map<String, Integer> mapReduce() {

		// map phase
		int mapsize = 0;
		List<Object[]> buckets = divideIntoBuckets(slurpedFile, BUCKET_SIZE);
		for (Object[] bucket : buckets) {
			List<Map.Entry<String, Integer>> map = createIntermidiateMap(bucket);
			mapsize += map.size();
			
			synchronized (intermediateMap) {
				intermediateMap.addAll(map);
			}
		}
		
		//log.info("size of map as sum of buckets: " + mapsize);

		// reduce phase
		Map<String, Integer> result = reduce(intermediateMap);
		return result;
	}

	private List<Map.Entry<String, Integer>> createIntermidiateMap(
			Object[] tokens) {
		List<Entry<String, Integer>> map = new ArrayList<Map.Entry<String, Integer>>();
		for (Object token : tokens) {
			map.add(new AbstractMap.SimpleEntry<String, Integer>(token.toString(), 1));
		}

		return map;
	}

	private Map<String, Integer> reduce(List<Map.Entry<String, Integer>> list) {
		Map<String, Integer> output = new HashMap<String, Integer>();
		for (Map.Entry<String, Integer> pair : list) {
			if (output.containsKey(pair.getKey())) {
				// need to refresh value - do not use value from pair - it is
				// always has value = 1
				output.put(pair.getKey(), output.get(pair.getKey()) + 1);
			} else {
				output.put(pair.getKey(), 1);
			}
		}
		return output;
	}

	/**
	 * divide string into buckets with provided size
	 * 
	 * @param toDivide
	 * @param bucketSize
	 * @return
	 */
	private List<Object[]> divideIntoBuckets(String toDivide, int bucketSize) {

		String[] tokens = toDivide.split(" ");

		int chunk = tokens.length / bucketSize;

		// System.out.println("all elements: " + tokens.length + " chunk = " +
		// chunk);

		// System.out.println("chunk*bucketsize=" + chunk*bucketSize);
		int rem = tokens.length % chunk;

		List<Object[]> list = new ArrayList<Object[]>();

		
		int subTokenPosition = 0;
		
		List<String> subTokenList = new ArrayList<String>();

		for (int i = 0; i < tokens.length; i++) {
			
			if (subTokenPosition < chunk) {
				subTokenList.add(tokens[i]);
				subTokenPosition++;
			} else {
				// System.out.println(subTokenPosition);
				subTokenPosition = 0;
				list.add(subTokenList.toArray());
				subTokenList = new ArrayList<String>();
				
				// rewind i
				i--;
			}
		}

		// add rest
		String[] subTokensRest = new String[rem];
		for (int rest = 0; rest < rem; rest++) {
			subTokensRest[rest] = tokens[(tokens.length - 1) - (rest)];
		}

		list.add(subTokensRest);

		return list;
	}

	@Override
	public Map<String, Integer> simpleWordCounting() {

		String[] tokens = slurpedFile.split(" ");
		log.info("all tokens: " + tokens.length);
		Map<String, Integer> wordOccurence = new HashMap<String, Integer>();

		for (String token : tokens) {
			if (wordOccurence.containsKey(token)) {
				Integer occured = (wordOccurence.get(token));
				occured++;
				wordOccurence.put(token, occured++);
			} else {
				wordOccurence.put(token, 1);
			}
		}

		return wordOccurence;
	}

	private class MapByValueComparator implements Comparator<String> {
		private Map<String, Integer> map;

		public MapByValueComparator(Map<String, Integer> map) {
			this.map = map;
		}

		@Override
		public int compare(String key1, String key2) {
			int value1 = map.get(key1);
			int value2 = map.get(key2);

			int diff = value2 - value1;
			if (diff == 0)
				return key1.hashCode() - key2.hashCode();
			else
				return diff;
		}
	}

	@Override
	public Map<String, Integer> sortMap(Map<String, Integer> mapToSort) {
		// sort descending by values
		Map<String, Integer> sortedMap = new TreeMap<String, Integer>(
				new MapByValueComparator(mapToSort));
		sortedMap.putAll(mapToSort);
		return sortedMap;
	}

	@Override
	public void readFile(String pathToFile) {
		
		BasicConfigurator.configure();
		try {
			File file = new File(pathToFile);

			Scanner scan = new Scanner(file);
			StringBuffer buf = new StringBuffer();
			while (scan.hasNext()) {

				String word = scan.next();
				word = sanitizeString(word);

				buf.append(word + " ");
			}

			slurpedFile = buf.toString();
			System.out.println("read " + pathToFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private String sanitizeString(String input) {
		StringBuffer buf = new StringBuffer();

		buf = new StringBuffer(input.replace(".", ""));
		buf = new StringBuffer(buf.toString().replace(",", ""));
		buf = new StringBuffer(buf.toString().replace(":", ""));
		buf = new StringBuffer(buf.toString().replace(";", ""));
		buf = new StringBuffer(buf.toString().replace("!", ""));
		buf = new StringBuffer(buf.toString().replace("?", ""));

		return buf.toString().toLowerCase();
	}

	@Override
	public void displayMap(Map<String, Integer> map, int values) {

		log.trace("---\n display map of size: " + map.size());
		for (Map.Entry<String, Integer> pair : map.entrySet()) {
			if (values-- > 0)
				log.info(pair.getKey() + " -> " + pair.getValue());
		}
	}

}
