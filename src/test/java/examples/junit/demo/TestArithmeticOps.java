package examples.junit.demo;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestArithmeticOps {
	
	ArithmeticOps arithOps = null;

	@Before
	public void setUp() throws Exception {
		
		arithOps = new ArithmeticOps();
	}

	@After
	public void tearDown() throws Exception {
		
		arithOps = null;
	}

	@Test
	public void testAdd() {
		assertEquals(11,arithOps.add(5, 6)); 
	}

	@Test
	public void testSub() {
		assertEquals(1,arithOps.sub(6, 5)); 
	}

	@Test
	public void testMultiply() {
		assertEquals(30,arithOps.multiply(5, 6)); 
	}

	@Test
	public void testDivide() {
		assertEquals(2,arithOps.divide(6, 3));
	}
	
	@Test
	public void testExp() {
		assertEquals(new Double(4.0),new Double(arithOps.exp(2, 2)));
	}

}
