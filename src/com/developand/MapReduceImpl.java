package com.developand;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

public class MapReduceImpl implements MapReduce {

	String slurpedFile = new String();
	final String FILE_TO_READ = "D:\\sandbox\\mapReduce\\brandesGeorge.txt";
	final String FILE_TO_READ_HUGE = "D:\\sandbox\\mapReduce\\bible.txt";
	Map<String, Integer> wordOccurence = new HashMap<String, Integer>();
	List<Map.Entry<String, Integer>> intermediateMap = new ArrayList<Map.Entry<String, Integer>>();

	@Override
	public Map<String, Integer> mapReduce() {
		map(getContent());
		Map<String, Integer> result = reduce(intermediateMap);
		return result;
	}

	private void map(String from) {
		String[] tokens = from.split(" ");
		for (String token : tokens) {
			intermediateMap.add(new AbstractMap.SimpleEntry<String, Integer>(
					token, 1));
		}
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

	@Override
	public Map<String, Integer> simpleWordCounting() {

		String[] tokens = getContent().split(" ");

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
	public void sortMap(Map<String, Integer> mapToSort) {
		// sort descending by values
		Map<String, Integer> sortedMap = new TreeMap<String, Integer>(
				new MapByValueComparator(mapToSort));
		sortedMap.putAll(mapToSort);
		displayMap(sortedMap);
	}

	@Override
	public void readFile() {
		try {
			File file = new File(FILE_TO_READ_HUGE);

			Scanner scan = new Scanner(file);
			StringBuffer buf = new StringBuffer();
			while (scan.hasNext()) {

				String word = scan.next();
				word = sanitizeString(word);

				buf.append(word + " ");
			}

			slurpedFile = buf.toString();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public String getContent() {
		return slurpedFile;
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

	private void displayMap(Map<String, Integer> map) {
		for (Map.Entry<String, Integer> pair : map.entrySet()) {
			System.out.println(pair.getKey() + " -> " + pair.getValue());
		}

		System.out.println("size of map:" + map.size());
	}

}
