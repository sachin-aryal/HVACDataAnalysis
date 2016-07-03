import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

/**
 * Created by who-are-you on 6/2/16.
 */
public class AnalysisReducer extends Reducer<IntWritable,Text,NullWritable,Text>{

    private Splitter splitter;
    private Joiner joiner;
    boolean firstTime;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        splitter = Splitter.on(",").trimResults();
        joiner = Joiner.on(",");
        firstTime = true;
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        double sumExpected=0.0,sumActual=0.0;
        int totalCount = 0;
        String modelNumber = "";
        for(Text val:values){
            List<String> rows = Lists.newArrayList(splitter.split(val.toString()));
            sumExpected+= Double.parseDouble(rows.get(rows.size()-2));
            sumActual+= Double.parseDouble(rows.get(rows.size()-1)+"0");
            modelNumber = rows.get(1);
            totalCount++;
        }

        if(firstTime){
            context.write(null,new Text("Building Id,Building Id,Model Number,Expected,Actual"));
            firstTime = false;
        }

        String mergedVal = String.valueOf(key)+","+modelNumber+","+(sumExpected/totalCount)+","+(sumActual/totalCount);

        context.write(null,new Text(mergedVal));

    }
}
