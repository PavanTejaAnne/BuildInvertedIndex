package edu.cmpe281.hadoop;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;
import java.util.regex.MatchResult;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BuildInvertedIndex  extends Configured implements Tool {


    public static class CountTfMap extends Mapper<LongWritable, Text, TextPairWC, TermInfo> {
        // private final static IntWritable one = new IntWritable(1);

        String fileName;
        protected void setup(Context context) throws IOException, InterruptedException {
            fileName = ((FileSplit) context.getInputSplit()).getPath().toString();
            System.out.println("setup :"+fileName);
        }

        public void map(LongWritable position, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // StringTokenizer tokenizer = new StringTokenizer(line);

            Scanner sc = new Scanner(line).useDelimiter("[^a-zA-Z]");

            // String[] terms = line.trim().split("[^a-zA-Z]");

	            while (sc.hasNext()) {
                Text termText = new Text(sc.next());
                MatchResult match = sc.match();
                System.out.println("SC :" + fileName);
                LongArrayListWrite offsets = new LongArrayListWrite();
                offsets.add(match.start() + position.get());
                context.write(new TextPairWC(termText, new Text(fileName)), new TermInfo(fileName, 1, offsets));

            }

        }
    }

    public static class CountTfCombine extends Reducer<TextPairWC, TermInfo, TextPairWC, TermInfo> {

        public void reduce (TextPairWC termFile, Iterable<TermInfo> termInfos, Context context)
            throws IOException, InterruptedException {
            int sum = 0;
            LongArrayListWrite offsets = new LongArrayListWrite();


            for (TermInfo termInfo: termInfos) {
                sum += termInfo.getTf();
                offsets.addAll(termInfo.getOffsets());
            }
            Collections.sort(offsets);

            context.write(termFile, new TermInfo ( termFile.getSecond().toString(), sum, offsets ));
        }
    }


    public static class TermPartitioner extends Partitioner<TextPairWC, TermInfo> {
        @Override
        public int getPartition(TextPairWC termFile, TermInfo termInfo, int numPartitions) {
            return ( termFile.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(TextPairWC.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            return ((TextPairWC)w1).getFirst ().compareTo (((TextPairWC)w2).getFirst ());
        }
    }

    public static class CountTfReduce extends Reducer<TextPairWC, TermInfo, TextIntWC, TermInfoArray> {
    	public void reduce (TextPairWC termFile, Iterable<TermInfo> termInfos, Context context)
            throws IOException, InterruptedException {

            ArrayList<TermInfo> termInfos_lst = new ArrayList<TermInfo>();
            for (TermInfo termInfo: termInfos)
                termInfos_lst.add((TermInfo)WritableUtils.clone(termInfo, context.getConfiguration()));

            TermInfo [] termInfos_Array = termInfos_lst.toArray(new TermInfo[termInfos_lst.size()]);
            IntWritable df = new IntWritable(termInfos_Array.length);

            context.write(new TextIntWC(termFile.getFirst(), df), new TermInfoArray(termInfos_Array));

        }
    }

    private Job countTfJob(Path inputPath, Path outputPath, Boolean doTextOutput) throws IOException {

        Job job = new Job(getConf(), "step 1: count tf");
        job.setJarByClass(getClass());

        // input
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        // mapper
        job.setMapperClass(CountTfMap.class);
        job.setMapOutputKeyClass(TextPairWC.class);
        job.setMapOutputValueClass(TermInfo.class);

        // combiner
        job.setCombinerClass(CountTfCombine.class);

        // partitioner
        job.setPartitionerClass(TermPartitioner.class);

        // grouping
        job.setGroupingComparatorClass (GroupComparator.class);

        // reducer
        job.setReducerClass(CountTfReduce.class);
        job.setOutputKeyClass(TextIntWC.class);
        job.setOutputValueClass(TermInfoArray.class);

        // job.setNumReduceTasks();
        // job.setNumMapTasks();

        // output
        if (doTextOutput == true) {
            TextOutputFormat.setOutputPath(job, outputPath);
            job.setOutputFormatClass(TextOutputFormat.class);
        } else {
            SequenceFileOutputFormat.setOutputPath(job, outputPath);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
        }

        return job;
    }

    public void printHelp(Options options){
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "BuildInvertedIndex [OPTION]... <INPUTPATH> <OUTPUTPATH>", "Process text files in INPUTPATH and build inverted index to OUTPUTPATH.\n", options, "");

    }

    @Override
    public int run(String[] args) throws Exception {

        Options options = new Options();
        
        options.addOption("text", false, "output in text format(only for checking)");
        options.addOption("help", false, "this help message.");

        CommandLineParser parser = new GnuParser();
        CommandLine cmd ;
        try {
            cmd = parser.parse( options, args);
        }
        catch( ParseException exp ) {
            System.out.println( "Unexpected exception:" + exp.getMessage() );
            printHelp(options);
            return 1;
        }
        if (cmd.hasOption("help") || cmd.getArgs().length < 2 ) { 
            printHelp(options);
            return 1;
        }

        URI uri = new URI("s3://invertedindex-hadoopspark/output");
        //FileSystem fs = FileSystem.get(uri,getConf());
        FileSystem fs = FileSystem.get(getConf());
        Path inputPath = new Path(cmd.getArgs()[0]);
        Path outputPath = new Path(cmd.getArgs()[1]);
        fs.delete(outputPath);
        Boolean doTextOutput = false;
        if(cmd.hasOption("text"))
            doTextOutput = true;

        return (countTfJob(inputPath, outputPath, doTextOutput).waitForCompletion(true) ? 1: 0);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BuildInvertedIndex(), args);
        System.exit(exitCode);
    }

}
