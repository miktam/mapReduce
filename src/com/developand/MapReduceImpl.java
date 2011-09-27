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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class MapReduceImpl implements MapReduce {
	private final int BUCKETS;
	ExecutorService executor = null;

	private final Logger log = Logger.getLogger(MapReduceImpl.class);

	// no multimaps so create so weird construction
	List<Map.Entry<Object, Integer>> intermediateMap = null;
	List<String> words = new ArrayList<String>();

	public MapReduceImpl(int threads) {
		BUCKETS = threads;
	}

	@Override
	public Map<Object, Integer> mapReduce() {

		executor = Executors.newFixedThreadPool(BUCKETS);

		intermediateMap = new ArrayList<Map.Entry<Object, Integer>>();
		finalResult = new HashMap<Object, Integer>();

		List<Object[]> buckets = divideIntoBuckets(BUCKETS);
		for (final Object[] bucket : buckets)
			executor.execute(new MapAndReduce(bucket));
		executor.shutdown();
		while (!executor.isTerminated()) {
		}

		// reduce phase
		// Map<Object, Integer> result = reduce(intermediateMap);
		return finalResult;
	}

	private List<Map.Entry<Object, Integer>> createIntermidiateMap(
			Object[] tokens) {
		List<Entry<Object, Integer>> map = new ArrayList<Map.Entry<Object, Integer>>();
		for (Object token : tokens) {
			map.add(new AbstractMap.SimpleEntry<Object, Integer>(token
					.toString(), 1));
		}

		return map;
	}

	private Map<Object, Integer> reduce(
			List<Entry<Object, Integer>> intermediateMap2) {
		log.debug("start reduce");
		Map<Object, Integer> output = new HashMap<Object, Integer>();
		for (Map.Entry<Object, Integer> pair : intermediateMap2) {
			if (output.containsKey(pair.getKey())) {
				// need to refresh value - do not use value from pair - it is
				// always has value = 1
				output.put(pair.getKey(), output.get(pair.getKey()) + 1);
			} else {
				output.put(pair.getKey(), 1);
			}
		}

		log.debug("finish reduce");
		return output;
	}

	/**
	 * divide string into buckets with provided size
	 * 
	 * @param toDivide
	 * @param bucketSize
	 * @return
	 */
	private List<Object[]> divideIntoBuckets(int bucketSize) {

		log.info("start bucketing");

		Object[] tokens = words.toArray();

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
				subTokenList.add(tokens[i].toString());
				subTokenPosition++;

				// brilliant architecture: if bucket size == 1, add and rest in
				// peace
				if (subTokenPosition == tokens.length) {
					list.add(subTokenList.toArray());
					break;
				}
			} else {
				// System.out.println(subTokenPosition);
				subTokenPosition = 0;

				list.add(subTokenList.toArray());

				subTokenList = new ArrayList<String>();

				// rewind i
				i--;
			}
		}

		if (bucketSize > 2) {
			// add rest
			String[] subTokensRest = new String[rem];
			for (int rest = 0; rest < rem; rest++) {
				subTokensRest[rest] = tokens[(tokens.length - 1) - (rest)]
						.toString();
			}

			list.add(subTokensRest);
		}

		log.info("finish bucketing, buckets: " + list.size());

		return list;
	}

	@Override
	public Map<String, Integer> simpleWordCounting() {

		Object[] tokens = words.toArray();
		// log.info("all tokens: " + tokens.length);
		Map<String, Integer> wordOccurence = new HashMap<String, Integer>();

		for (Object token : tokens) {
			if (wordOccurence.containsKey(token)) {
				Integer occured = (wordOccurence.get(token));
				occured++;
				wordOccurence.put(token.toString(), occured++);
			} else {
				wordOccurence.put(token.toString(), 1);
			}
		}

		return wordOccurence;
	}

	private class MapByValueComparator implements Comparator<Object> {
		private Map<Object, Integer> map;

		public MapByValueComparator(Map<Object, Integer> map) {
			this.map = map;
		}

		@Override
		public int compare(Object key1, Object key2) {
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
	public Map<Object, Integer> sortMap(Map<Object, Integer> mapToSort) {
		// sort descending by values
		Map<Object, Integer> sortedMap = new TreeMap<Object, Integer>(
				new MapByValueComparator(mapToSort));
		sortedMap.putAll(mapToSort);
		return sortedMap;
	}

	@Override
	public void readFile(String pathToFile) {

		BasicConfigurator.configure();
		try {
			log.info("read file:" + pathToFile);
			File file = new File(pathToFile);

			Scanner scan = new Scanner(file);
			while (scan.hasNext()) {

				String word = scan.next();
				word = sanitizeString(word);

				words.add(word);
			}

			log.info("file succesfully read");
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
	public void displayMap(Map<Object, Integer> map, int values) {

		log.info("---\n display map of size: " + map.size());
		for (Map.Entry<Object, Integer> pair : map.entrySet()) {
			if (values-- > 0)
				log.info(pair.getKey() + " -> " + pair.getValue());
		}
	}

	Map<Object, Integer> finalResult;

	class MapAndReduce implements Runnable {

		private Object[] bucket;

		MapAndReduce(Object[] bucket) {
			this.bucket = bucket;

		}

		@Override
		public void run() {

			log.info("A + start map of bucket " + bucket[0]);
			List<Map.Entry<Object, Integer>> map = createIntermidiateMap(bucket);
			log.info("A - finished map " + bucket[0] + " with map size = "
					+ map.size());

			log.info("B + start reduce of " + bucket[0]);
			Map<Object, Integer> result = reduce(map);
			log.info("B - finished reduce of " + bucket[0] + " with "
					+ result.size());

			log.info("C + final map size before:: " + finalResult.size());
			synchronized (finalResult) {

				for (Entry<Object, Integer> entry : result.entrySet()) {

					if (finalResult.containsKey(entry.getKey())) {
						finalResult.put(
								entry.getKey(),
								finalResult.get(entry.getKey())
										+ entry.getValue());
					} else
						finalResult.put(entry.getKey(), entry.getValue());
				}
			}

			log.info("C - finalResult after " + finalResult.size());

		}
	};

	class Mapper implements Runnable {

		private Object[] bucket;

		Mapper(Object[] bucket) {
			this.bucket = bucket;
		}

		@Override
		public void run() {

			log.info("start " + bucket[0]);
			List<Map.Entry<Object, Integer>> map = createIntermidiateMap(bucket);
			synchronized (intermediateMap) {
				intermediateMap.addAll(map);
			}

			log.info("finished " + bucket[0] + "\n");
		}
	};
}
