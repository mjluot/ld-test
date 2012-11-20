import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
//Hadoops
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.conf.Configuration;
//Language-detectors
import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

public class read_hadoop {

	public static void main(String args[]) throws IOException, InstantiationException, IllegalAccessException, LangDetectException{
        //Loads language profiles
		DetectorFactory.loadProfile("./profiles");
		process_file("1341690147253-textData-00534.hdp");
	}
	
	public static void process_file(String hdp_filepath) throws IOException{
		File file = new File("output.txt");
		if (!file.exists()) {
			file.createNewFile();
		}		
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter out = new BufferedWriter(fw);
		Configuration config = new Configuration();
		Path path = new Path(hdp_filepath);
		SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(config), path, config);
		
		try {
			filter(reader, out);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
	}
	
	public static void filter(SequenceFile.Reader reader, BufferedWriter out) throws IOException, InstantiationException, IllegalAccessException, LangDetectException{
		System.out.println("Starting to read " + reader.toString());
		String magicStringB="####FIPBANK-BEGIN-MARKER: ";
		String magicStringE="\n####FIPBANK-END-MARKER\n";		
		
		WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
		Writable value = (Writable) reader.getValueClass().newInstance();
		while (reader.next(key, value)){
			
			//Load language detector
			Detector detector = DetectorFactory.create();
			String str_val = value.toString();

			//Shorten the text_string
			String short_val;
			if (str_val.length() > 312){ 
				short_val = value.toString().substring(0, 312);
			}
			else{
				short_val = str_val; 
			}
			
			try{
				//Try to identify the language
				detector.append(short_val);
				String lang = detector.detect();
				if(lang.equals("fi")){
		            out.write(magicStringB);
		            out.write(key.toString());
		            out.write("\n");
		            out.write(str_val);
		            out.write(magicStringE);
				}
			}
			catch(LangDetectException e){
				//ignore stuff that's not detected
			}
			
		}
		reader.close();
		System.out.println("Done reading " + reader.toString());			
	}			
}

