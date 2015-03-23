
public class StringTest {

	public static void main(String[] args) {
		String data = "\"0195153448\";\"Classical Mythology\";\"Mark P. O. Morford\";\"2002\";\"Oxford University Press\";\"http://images.amazon.com/images/P/0195153448.01.THUMBZZZ.jpg\";\"http://images.amazon.com/images/P/0195153448.01.MZZZZZZZ.jpg\";\"http://images.amazon.com/images/P/0195153448.01.LZZZZZZZ.jpg\"";
		data = data.replace("\"", "");
		String[] spl = data.split(";");
		for(String str : spl) {
			System.out.println(str);
		}

	}

}
