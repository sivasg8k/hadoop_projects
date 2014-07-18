package examples.mr.wordcount;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class FileFilter extends Configured implements PathFilter {
	
	Pattern pattern;
	Configuration conf;

	@Override
	public boolean accept(Path path) {

		Matcher m = pattern.matcher(path.toString());
		
		return m.matches();
	}
	
	@Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        pattern = Pattern.compile(conf.get("file.pattern"));
    }

}
