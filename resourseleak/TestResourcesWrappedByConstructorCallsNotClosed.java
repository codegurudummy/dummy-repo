package resourceleak;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TestResourcesWrappedByConstructorCallsNotClosed {


    protected static List<String> testResourcesWrappedByConstructorCallsNotClosed(long timeout) throws IOException {

        List<String> thisAndGitVersion= new ArrayList<String>();

        // Get version of this ASCIIGenome
        thisAndGitVersion.add(ArgParse.VERSION);

        BufferedReader br= null;
        timeout= timeout + System.currentTimeMillis();
        // Get github versions
        URL url = new URL("https://api.github.com/repos/dariober/ASCIIGenome/tags");
        br = new BufferedReader(new InputStreamReader(url.openStream()));

        String line;
        StringBuilder sb= new StringBuilder();
        while ((line = br.readLine()) != null) {
            sb.append(line + '\n');
        }

        JsonElement jelement = new JsonParser().parse(sb.toString());
        JsonArray  jarr = jelement.getAsJsonArray();
        Iterator<JsonElement> iter = jarr.iterator();

        List<String> tag= new ArrayList<String>();
        while(iter.hasNext()){
            // Get all tags
            JsonObject jobj = (JsonObject) iter.next();
            tag.add(jobj.get("name").getAsString().replaceAll("^[^0-9]*", "").trim());
        }
        if(tag.size() == 0){
            thisAndGitVersion.add("0");
        } else {
            thisAndGitVersion.add(tag.get(0));
        }
        return thisAndGitVersion;
    }
}
