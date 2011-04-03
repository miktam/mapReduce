package com.developand;

import org.junit.BeforeClass;
import org.junit.Test;

public class MapReduceTest {

	static MapReduce mr = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		mr = new MapReduceImpl();
		mr.readFile();
	}
	

	@Test
	public void testSimpleSorting()
	{
		mr.sortMap(mr.simpleWordCounting());
	}	
	
	@Test
	public void testMapReduce() {
		mr.sortMap(mr.mapReduce());
	}

}
