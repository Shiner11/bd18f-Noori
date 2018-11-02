package edu.snu.bd.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.*;
import org.apache.commons.csv.*;

import java.io.*;
import java.util.List;
import java.util.Vector;


public class Homework {
    public interface PipelineOptions extends org.apache.beam.sdk.options.PipelineOptions {
        @Description("Path of the file to read from")
        @Required
        String getInputFile1();

        void setInputFile1(String value);

        @Description("Path of the file to read from")
        @Required
        String getInputFile2();

        void setInputFile2(String value);

        @Description("Path of the file to write to")
        @Required
        String getOutput();

        void setOutput(String value);
    }

    public static class FormatAsTextFn extends SimpleFunction<Row, String> {
        @Override
        public String apply(Row input) {
            List result = input.getValues();
            String st_result = ((List) result).toString();
            System.out.println(st_result);
            return null;
        }
    }
    public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";
    static class ExtractWordsFn extends DoFn<String, String> {
        private final Counter emptyLines = Metrics.counter(Homework.ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist =
                Metrics.distribution(Homework.ExtractWordsFn.class, "lineLenDistro");

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            lineLenDist.update(element.length());
            if (element.trim().isEmpty()) {
                emptyLines.inc();
            }

            // Split the line into words.
            String[] words = element.split(TOKENIZER_PATTERN, -1);

            // Output each word encountered into the output PCollection.
            for (String word : words) {
                if (!word.isEmpty()) {
                    receiver.output(word);
                }
            }
        }
    }
    public static class CountWords
            extends PTransform<PCollection<String>, PCollection<KV<String,Long>>> {
        @Override
        public PCollection<KV<String,Long>> expand(PCollection<String> lines) {
            // Convert lines of text into individual words.
            PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));
            // Count the number of times each word occurs.
            PCollection<KV<String,Long>> wordCounts = words.apply(Count.perElement());
            return wordCounts;
        }
    }
//
//    static void applyWordCountLogic(final Pipeline pipeline, final String input, final String output) {
//        pipeline.apply("ReadLines", TextIO.read().from("Harry_Potter_4_The_Goblet_Of_Fire.txt"))
//                .apply(new CountWords())
//                .apply(MapElements.via(new FormatAsTextFn()))
//                .apply("WriteCounts", TextIO.write().to(output));
//    }

    static void applySQLLogic(final Pipeline pipeline, final String input, final String input2, final String output) {

        Schema type =
                Schema.builder().addStringField("key").addInt32Field("value").build();

        PCollection<Row> table = pipeline.apply("ReadLines", TextIO.read().from(input))
                .apply(new CountWords())
                .apply(
                        ParDo.of(new DoFn<KV<String, Long>, Row>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                // Get the current POJO instance
                                KV<String, Long> wordcount = c.element();

                                // Create a Row with the appSchema schema
                                // and values from the current POJO
                                Row appRow =
                                        Row
                                                .withSchema(type)
                                                .addValues(
                                                        wordcount.getKey(),
                                                        (int) (long) wordcount.getValue()
                                                )
                                                .build();

                                // Output the Row representing the current POJO
                                c.output(appRow);
                            }
                        })).setCoder(RowCoder.of(type));

//        Schema type =
//                Schema.builder().addStringField("key").addInt32Field("value").build();

        PCollection<Row> table2 = pipeline.apply("ReadLines", TextIO.read().from(input2))
                .apply(new CountWords())
                .apply(
                        ParDo.of(new DoFn<KV<String, Long>, Row>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                KV<String, Long> wordcount = c.element();

                                Row appRow =
                                        Row
                                                .withSchema(type)
                                                .addValues(
                                                        wordcount.getKey(),
                                                        (int) (long) wordcount.getValue()
                                                )
                                                .build();

                                // Output the Row representing the current POJO
                                c.output(appRow);
                            }
                        })).setCoder(RowCoder.of(type));

        table.apply(MapElements.via(new FormatAsTextFn()));

        PCollectionTuple xx = PCollectionTuple.of(new TupleTag<>("Table1"), table)
                .and(new TupleTag<>("Table2"), table2);

        PCollection<Row> outputStream =
                xx.apply(SqlTransform.query("select double(value) from Table1 join Table2 on Table1.key = Table2.key")
                        .registerUdf("double", DoubleInteger.class));

        outputStream.apply("log_result", MapElements.via(
                new SimpleFunction<Row, String>() {
                    @Override
                    public String apply(Row input) {
                        return input.getValues().toArray()[0].toString();
                    }
                }))
                .apply(TextIO.write().to("output.txt"));
    }

    static void runSQL(String[] args) throws Exception {

        Homework.PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(Homework.PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        applySQLLogic(p, options.getInputFile1(), options.getInputFile2(), options.getOutput());

        final PipelineResult result =  p.run();

        if (args[0].equals("--runner=SparkRunner")) {
            result.waitUntilFinish(); // Not yet supported by Nemo.
        }
    }

    public static void main(String[] args) throws Exception {
        runSQL(args);
    }
}
