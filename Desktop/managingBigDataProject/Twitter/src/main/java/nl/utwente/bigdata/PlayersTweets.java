/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.utwente.bigdata;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.regex.Pattern;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;

import org.json.simple.parser.JSONParser;


public class PlayersTweets {
  private static String playersWorldCup ="joe,hart,rafa,marquez,diego,benaglio,antonio,valencia,bryan,ruiz,ashkan,dejagah,sokratis,papastathopoulos,aleksandr,kokorin,madjid,bougherra,emmanuel,emenike,clint,dempsey,ezekial,lavezzi,xherdan,shaqiri,wilfried,bony,john,obi,mikel,jack,wilshere,gervinho,aleksandr,kerzhakov,shinji,kagawa,fabio,coentrao,thomas,muller,diego,forlan,asmir,begovic,bastian,schweinsteiger,diego,godin,joao,moutinho,vasilis,torosidis,jackson,martinez,solomon,kalou,stephan,lichsteiner,blaise,matuidi,thibault,courtois,asamoah,gyan,gokhan,inler,oribe,peralta,michael,bradley,mario,mandzukic,miralem,pjanic,samuel,eto’o,sergio,busquets,mario,gotze,dirk,kuyt,mile,jedinak,james,rodriguez,kevin,prince-boateng,david,silva,paul,pogba,gerard,pique,marco,reus,gonzalo,higuain,steven,gerrard,tim,howard,thiago,motta,keisuke,honda,hugo,lloris,oscar,iker,casillas,giorgio,chiellini,diego,costa,pepe,javier,hernandez,daniel,sturridge,didier,drogba,edin,dzeko,per,mertesacker,mario,balotelli,juan,mata,romelu,lukaku,dani,alves,alexis,sanchez,karim,benzema,franck,ribery,gigi,buffon,david,luiz,pablo,zabaleta,neymar,radamel,falcao,sergio,ramos,xavi,toni,kroos,luka,modric,wesley,sneijder,andrea,pirlo,mesut,ozil,arturo,vidal,thiago,silva,manuel,neuer,edinson,cavani,philipp,lahm,vincent,kompany,sergio,aguero,arjen,robben,eden,hazard,wayne,rooney,robin,van,persie,yaya,toure,andres,iniesta,luis,suarez,lionel,messi,cristiano,ronaldo";
  private static List<String> listPlayers =  Arrays.asList(playersWorldCup.split(","));
  

  public static String deAccent(String str) {
      String nfdNormalizedString = Normalizer.normalize(str, Normalizer.Form.NFD); 
      Pattern pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
      return pattern.matcher(nfdNormalizedString).replaceAll("");
  }

  public static class WorldCupMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text tweetTime  = new Text();
    private JSONParser parser = new JSONParser();
    private Map tweet;
    private final static IntWritable one = new IntWritable(1);
    private Text tweetText = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    try {
        tweet = (Map<String, Object>) parser.parse(value.toString());
      }
      catch (ClassCastException e) {  
        return; // do nothing (we might log this)
      }
      catch (org.json.simple.parser.ParseException e) {  
        return; // do nothing 
      }

      String createdAt = (String) tweet.get("created_at");
      createdAt = createdAt.substring(0, 10);
      tweetTime.set(createdAt);
      String tweetText = deAccent((String)tweet.get("text")).replaceAll("\n", " ").replaceAll("#", " ").toLowerCase();
      List<String> lineInWords =  Arrays.asList(tweetText.split(" "));
        for (Object o: lineInWords) {
          if(listPlayers.contains((String) o)){
               context.write(new Text((String) o), one);
         }
        }
    }
  }

    public static class WorldCupReducer 
       extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
      
       
    }     
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      
      System.exit(2);
    }
    Job job = new Job(conf, "Players Tweets");
    job.setJarByClass(PlayersTweets.class);
    job.setMapperClass(WorldCupMapper.class);
    job.setCombinerClass(WorldCupReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}