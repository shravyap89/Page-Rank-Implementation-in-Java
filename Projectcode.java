package PageRank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import java.io.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import java.net.URI;

public class PageRank {
	final static private double coef = 0.85;
	public static void main(String[] args) throws Exception {
	    
	 //first job Red Link Flagging
	     JobConf conf1 = new JobConf(PageRank.class);
	      conf1.setJobName("RedElim");
	
	      conf1.setOutputKeyClass(Text.class);
	      conf1.setOutputValueClass(Text.class);
	
	      conf1.setMapperClass(Map1.class);
	      conf1.setCombinerClass(Reduce1.class);
	      conf1.setReducerClass(Reduce1.class);
	
	      conf1.set("xmlinput.start", "<page>");
	      conf1.set("xmlinput.end", "</page>");
	      conf1.setInputFormat(XmlInputFormat.class);
	      conf1.setOutputFormat(TextOutputFormat.class);
	
	      FileInputFormat.setInputPaths(conf1, new Path("s3://spring-2014-ds/data/enwiki-latest-pages-articles.xml"));
	      FileOutputFormat.setOutputPath(conf1, new Path("s3n://"+args[0]+"/results/PageRank.RedElim.out"));
	
	      JobClient.runJob(conf1);
	 //second job Page Count
 	     JobConf conf2 = new JobConf(PageRank.class); 
	     conf2.setJobName("PageCount"); 
  
	     conf2.setMapOutputKeyClass(Text.class);
	     conf2.setMapOutputValueClass(Text.class);
	     conf2.setOutputKeyClass(Text.class); 
	     conf2.setOutputValueClass(IntWritable.class); 
  
	     conf2.setMapperClass(Map2.class); 
	     conf2.setReducerClass(Reduce2.class); 
  
	     conf2.setInputFormat(TextInputFormat.class); 
	     conf2.setOutputFormat(TextOutputFormat.class); 
             conf2.setNumReduceTasks(1);
	     FileInputFormat.setInputPaths(conf2, new Path("s3n://"+args[0]+"/results/PageRank.RedElim.out")); 
	     FileOutputFormat.setOutputPath(conf2, new Path("s3n://"+args[0]+"/results/PageRank.n.out")); 
  
	     JobClient.runJob(conf2); 
	//third job Outlink Mapping
	JobConf conf3 = new JobConf(PageRank.class);
	      conf3.setJobName("Outlinks");
	
		conf3.setMapOutputKeyClass(Text.class);
		conf3.setMapOutputValueClass(Text.class);
	      conf3.setOutputKeyClass(Text.class);
	      conf3.setOutputValueClass(Text.class);
	
		  String total = "";
	      try{
              Path pt=new Path("/results/PageRank.n.out/part-00000");
              FileSystem fs = FileSystem.get(new URI("s3n://"+args[0]+"/"), new Configuration());
              BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
              String line;
              line=br.readLine();
              StringTokenizer tokenizer = new StringTokenizer (line);
              tokenizer.nextToken();
              total = tokenizer.nextToken();
		  }catch(Exception e){
		  }
			  conf3.set("N", total);
	
	      conf3.setMapperClass(Map3.class);
	      conf3.setReducerClass(Reduce3.class);
	
	      conf3.setInputFormat(TextInputFormat.class);
	      conf3.setOutputFormat(TextOutputFormat.class);
	
	      FileInputFormat.setInputPaths(conf3, new Path("s3n://"+args[0]+"/results/PageRank.RedElim.out"));
	      FileOutputFormat.setOutputPath(conf3, new Path("s3n://"+args[0]+"/results/PageRank.outlink.out"));
	
	      JobClient.runJob(conf3);
	//fourth job Calcualtion Iterations
	    for(int i = 1; i<=8; i++){
	      JobConf conf4 = new JobConf(PageRank.class);
	      conf4.setJobName("IterCalc");
	
		  conf4.setMapOutputKeyClass(Text.class);
		  conf4.setMapOutputValueClass(Text.class);
	      conf4.setOutputKeyClass(Text.class);
	      conf4.setOutputValueClass(Text.class);
	
	      conf4.setMapperClass(Map4.class);
	      conf4.setReducerClass(Reduce4.class);
	
	      conf4.set("N", total);
		
	      conf4.setInputFormat(TextInputFormat.class);
	      conf4.setOutputFormat(TextOutputFormat.class);
	    String inName="";
	      if(i==1){
		inName = "s3n://"+args[0]+"/results/PageRank.outlink.out";
	      }else{
		inName = "s3n://"+args[0]+"/tmp/PageRank.iter"+(i-1)+".out";
		}

	      FileInputFormat.setInputPaths(conf4, new Path(inName));
	      FileOutputFormat.setOutputPath(conf4, new Path("s3n://"+args[0]+"/tmp/PageRank.iter"+i+".out"));
	      JobClient.runJob(conf4);
	   }
		  
	

	//fifth job Reorder PageRanks
	 for(int i =0; i<2; i++){
	 JobConf conf5 = new JobConf(PageRank.class); 
	  conf5.setJobName("RankingMapper"); 
  
	  conf5.setMapOutputKeyClass(DoubleWritable.class);
	  conf5.setMapOutputValueClass(Text.class);
	  conf5.setOutputKeyClass(DoubleWritable.class); 
	  conf5.setOutputValueClass(Text.class); 
  
	  conf5.setMapperClass(Map5.class); 
	  conf5.setReducerClass(Reduce5.class); 
  	  conf5.set("N", total);
	  conf5.setInputFormat(TextInputFormat.class); 
	  conf5.setOutputFormat(TextOutputFormat.class); 
  	  conf5.setNumReduceTasks(1);
	  if(i==0){
		  FileInputFormat.setInputPaths(conf5, new Path("s3n://"+args[0]+"/tmp/PageRank.iter1.out")); 
		  FileOutputFormat.setOutputPath(conf5, new Path("s3n://"+args[0]+"/results/PageRank.iter1.out")); 
	 }else if(i==1){
		  FileInputFormat.setInputPaths(conf5, new Path("s3n://"+args[0]+"/tmp/PageRank.iter8.out"));
		  FileOutputFormat.setOutputPath(conf5, new Path("s3n://"+args[0]+"/results/PageRank.iter8.out")); 
	 }
	  JobClient.runJob(conf5); 
	}
	}
	
/*===================first job Red Link Flagging Mapper & Reducer===================================*/

	public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	
	      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	        String line = value.toString();
		    Text title = new Text();

			String x = null;
	        Text link = null;
	        int indexStart = line.indexOf("</title>");
	        title.set(line.substring((line.indexOf("<title>") + 7), indexStart).replace(" ", "_")); 
			 output.collect(title, new Text("$"));

	         line = line.substring(indexStart);
                  Matcher matcher = Pattern.compile("\\[\\[.*?\\]]").matcher(line);
                  String sub = null;
                  int leftCount = 0; 
                  int rightCount = 0;
                  int nIndex = 0;
                  char[] sArray = null;
                  while(matcher.find()){ 
                      x = matcher.group(); 
                      x = x.substring(2,x.length()-2);
                      if(x.contains("#"))continue;
                      if(x.contains(":")){ 
                    	  if(x.contains("[[")){
                    		  sub = x + line.substring(line.indexOf(x)+x.length()+2);
                    		  sArray = sub.toCharArray();
                    		  while(leftCount>=rightCount&&rightCount%2==0){
                    			  if(sArray[nIndex]=='['){
                    				  leftCount++;
                    			  }else if(sArray[nIndex]==']'){
                    				  rightCount++;
                    			  }
                    			  nIndex++;
                    		  }
                            	matcher = Pattern.compile("\\[\\[.*?\\]]").matcher(sub.substring(nIndex));
                    		}
                        continue;
                      } 
                      if(x.contains("|")){ 
                          x = x.substring(0,x.indexOf("|")); 
                      }
                      
                      link = new Text (x.replace(" ", "_"));
                      output.collect(link, title);
                  }
                  
                 }
              
	        }
	public static class Reduce1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	       Text linkSet = new Text();
	       String title = key.toString();
	       String link = null;
	       HashSet<String> set = new HashSet<String>();
	       while(values.hasNext()){
	    	   link = values.next().toString();
	    	  if(!link.equals(title)) set.add(link);
	       } 
	       String results = "";
	       String[] lSet = set.toArray(new String[0]);
	       for(String s: lSet){
	    	results += s+" ";   
	       }
	       linkSet.set(results);
	        output.collect(key, linkSet);
	    }
		
	  }
/*===================second job Page Count code===================================*/
	public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	
	      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException { 
            String line = value.toString(); 
            Text Title = new Text();
			if(line.contains("$")){
				StringTokenizer tokenizer = new StringTokenizer(line); 
				output.collect(new Text("N=") , new Text(tokenizer.nextToken()));  
             }
			}
        } 
       
    	
	public static class Reduce2 extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {
	      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
                    int sum = 0; 
                    while (values.hasNext()) 
                    {  
                      values.next(); 
                      sum +=1; 
                    } 
                    output.collect(key, new IntWritable(sum)); 
                  } 
                }
/*===================third job Outlink Mapping code===================================*/

	public static class Map3 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	
	      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	        String line = value.toString();
		  Text link = new Text();
		  StringTokenizer tokenizer = new StringTokenizer(line);
		  link.set(tokenizer.nextToken());
		  if(tokenizer.nextToken().equals("$")){
		  output.collect(link, new Text(""));
		  while(tokenizer.hasMoreTokens()){
			output.collect(new Text(tokenizer.nextToken()), link);
		  }
		}
		}
              
        }
	public static class Reduce3 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private String N;
		public void configure(JobConf job) {
		    N = job.get("N");
		}
	      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	       Text linkSet = new Text();
	   	 String results = "";		
		while(values.hasNext()){
			results += values.next().toString()+" "; 
		}
		double pr = 1/Double.parseDouble(N);
	 	  linkSet.set((Double.toString(pr) +" "+results));
	        output.collect(key, linkSet);
	    }
	  }
/*===================fourth job Calculation Iterations code===================================*/
public static class Map4 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	        String line = value.toString();
		  Text title = new Text();
		  StringTokenizer tokenizer = new StringTokenizer(line);
  		  String t = tokenizer.nextToken();
  		  title.set(t);
		  String find = tokenizer.nextToken();
		  double pr = Double.parseDouble(find);
		   output.collect(title, new Text(" "+ line.substring((line.indexOf(find)+find.length()))));
			int degree = tokenizer.countTokens();
			double calc = pr/degree;
		  while(tokenizer.hasMoreTokens()){
			output.collect(new Text(tokenizer.nextToken()), new Text(Double.toString(calc)));
		  }
		
		}
              
        }
	public static class Reduce4 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private String N;
		public void configure(JobConf job) {
		    N = job.get("N");
		}
	      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		 String x = "";
		 String links = "";
		 double y = 0;
		double pr = (1-coef)/Integer.parseInt(N);
		boolean foundIt = false;
		while(values.hasNext()){
			x = values.next().toString();
			try{
			 y = Double.parseDouble(x);
			}catch(Exception e){
				links += " " + x;

			}
			pr += coef*(y);
		}
		
		String out = Double.toString(pr) + links; 
		output.collect(key, new Text(out));
	    }
	  }
/*===================fifth job Reorder Page Rank code===================================*/
	public static class Map5 extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text> {
			private String N;
				public void configure(JobConf job) {
				    N = job.get("N");
				}
		      public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException { 
			Text page = new Text();
		    	String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			page.set(tokenizer.nextToken());
			double parsedouble = Double.parseDouble(tokenizer.nextToken());
			parsedouble*=-1;
		
				 if (parsedouble>=5/Integer.parseInt(N)){
							output.collect(new DoubleWritable(parsedouble), page);
					} 
					}
			} 
	       
	    	
		public static class Reduce5 extends MapReduceBase implements Reducer<DoubleWritable, Text, DoubleWritable, Text> {
		      public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
		            double rank = -1 * key.get();
				  DoubleWritable pr = new DoubleWritable(rank);
				 while(values.hasNext()){ 				
				  output.collect(pr, new Text (values.next()));  
				}              
			}
		}
/*===================XMLInputFormat code===================================*/

	public static class XmlInputFormat extends TextInputFormat {
  
	  public static final String START_TAG_KEY = "xmlinput.start";
	  public static final String END_TAG_KEY = "xmlinput.end";
	  
	  @Override
	  public RecordReader<LongWritable,Text> getRecordReader(InputSplit inputSplit,
		                                                 JobConf jobConf,
		                                                 Reporter reporter) throws IOException {
	    return new XmlRecordReader((FileSplit) inputSplit, jobConf);
	  }
	  
	  /**
	   * XMLRecordReader class to read through a given xml document to output xml
	   * blocks as records as specified by the start tag and end tag
	   * 
	   */
	  public static class XmlRecordReader implements
	      RecordReader<LongWritable,Text> {
	    private final byte[] startTag;
	    private final byte[] endTag;
	    private final long start;
	    private final long end;
	    private final FSDataInputStream fsin;
	    private final DataOutputBuffer buffer = new DataOutputBuffer();
	    
	    public XmlRecordReader(FileSplit split, JobConf jobConf) throws IOException {
	      startTag = jobConf.get(START_TAG_KEY).getBytes("utf-8");
	      endTag = jobConf.get(END_TAG_KEY).getBytes("utf-8");
	      
	      // open the file and seek to the start of the split
	      start = split.getStart();
	      end = start + split.getLength();
	      Path file = split.getPath();
	      FileSystem fs = file.getFileSystem(jobConf);
	      fsin = fs.open(split.getPath());
	      fsin.seek(start);
	    }
	    
	    @Override
	    public boolean next(LongWritable key, Text value) throws IOException {
	      if (fsin.getPos() < end) {
		if (readUntilMatch(startTag, false)) {
		  try {
		    buffer.write(startTag);
		    if (readUntilMatch(endTag, true)) {
		      key.set(fsin.getPos());
		      value.set(buffer.getData(), 0, buffer.getLength());
		      return true;
		    }
		  } finally {
		    buffer.reset();
		  }
		}
	      }
	      return false;
	    }
	    
	    @Override
	    public LongWritable createKey() {
	      return new LongWritable();
	    }
	    
	    @Override
	    public Text createValue() {
	      return new Text();
	    }
	    
	    @Override
	    public long getPos() throws IOException {
	      return fsin.getPos();
	    }
	    
	    @Override
	    public void close() throws IOException {
	      fsin.close();
	    }
	    
	    @Override
	    public float getProgress() throws IOException {
	      return (fsin.getPos() - start) / (float) (end - start);
	    }
	    
	    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
	      int i = 0;
	      while (true) {
		int b = fsin.read();
		// end of file:
		if (b == -1) return false;
		// save to buffer:
		if (withinBlock) buffer.write(b);
		
		// check if we're matching:
		if (b == match[i]) {
		  i++;
		  if (i >= match.length) return true;
		} else i = 0;
		// see if we've passed the stop point:
		if (!withinBlock && i == 0 && fsin.getPos() >= end) return false;
	      }
	    }
	  }
	}
}
