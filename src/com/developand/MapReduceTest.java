package com.developand;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MapReduceTest {

	static MapReduce mr = null;
	final static String FILE_TO_READ_HUGE = "D:\\sandbox\\mapReduce\\cia_fact_book.txt";
	final static String FILE_TO_READ_SMALLER = "D:\\sandbox\\mapReduce\\noteBooksOfDaVinci.txt";
	final static String FILE_TO_READ_REALLY_SMALL = "D:\\sandbox\\mapReduce\\small.txt";

	private static long totalSimple = 0;
	private static long totalMapReduce = 0;
	private static int THREADS = 8;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Logger.getRootLogger().setLevel(Level.ERROR);
		mr = new MapReduceImpl(THREADS);
		// mr.readFile(FILE_TO_READ_SMALLER);
		mr.readFile(FILE_TO_READ_HUGE);
		// mr.readFile(FILE_TO_READ_REALLY_SMALL);

	}

	@Test
	public void testSimpleSorting() {
		long c = stat();
		Map map = mr.simpleWordCounting();
		totalSimple += stat(c);
		// mr.displayMap(mr.sortMap(map), 3);
	}

	@Test
	public void testMapReduceNotSoHuge() {

		long c = stat();
		Map<Object, Integer> map = mr.mapReduce();
		totalMapReduce += stat(c);

		mr.displayMap(mr.sortMap(map), 3);
	}

	private long stat() {
		return System.currentTimeMillis();
	}

	private long stat(long start) {
		return System.currentTimeMillis() - start;
	}

	@Parameterized.Parameters
	public static List<Object[]> data() {
		// first parameter - times run
		return Arrays.asList(new Object[10][0]);
	}

	public MapReduceTest() {
	}

	@AfterClass
	public static void afterClass() {

		System.out.println("threads used: " + THREADS);
		System.out.println("totals: simple: " + totalSimple);
		System.out.println("totals: mapReduce: " + totalMapReduce);
	}

}
