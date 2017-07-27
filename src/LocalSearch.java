import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class LocalSearch {
	public static void main(String[] args){
//		String source = "Ryan, Samantha"; String dest = "Starr, Bobbi";
		String source = "Starr, Bobbi"; String dest = "Ryan, Samantha";
		String fileName = "src/local_results";
		HashMap<String, String> map = new HashMap<String, String>();
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
//                System.out.println(tempString);
                String[] infos = tempString.split("\t");
                map.put(infos[0] + "\t" + infos[1], infos[2]);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        String result = map.get(source + "\t" + dest);
        if(result != null) {
        	System.out.println(result);
        	return;
        }
        result = map.get(dest + "\t" + source);
        if(result == null) {
        	System.out.println("No results");
        }
        else {
        	System.out.println(result);
        }
	}
}
