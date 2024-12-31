import java.io.IOException;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.DataInput;
import java.io.DataOutput;

// Custom VertexWritable class for handling graph vertices
class VertexWritable implements Writable {
    public long id;              // Unique identifier for the vertex
    public Vector<Long> adjacent; // List of adjacent vertices
    public long centroid;        // Centroid of the cluster this vertex belongs to
    public short depth;          // Depth of the vertex in BFS traversal

    // Default constructor
    public VertexWritable() {
        adjacent = new Vector<>();
    }

    // Constructor with parameters
    public VertexWritable(long id, Vector<Long> adjacent, long centroid, short depth) {
        this.id = id;
        this.adjacent = adjacent;
        this.centroid = centroid;
        this.depth = depth;
    }

    // Method to write the vertex data to output
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeInt(adjacent.size());
        for (Long v : adjacent) {
            out.writeLong(v);
        }
        out.writeLong(centroid);
        out.writeShort(depth);
    }

    // Method to read the vertex data from input
    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        int size = in.readInt();
        adjacent = new Vector<>(size);
        for (int i = 0; i < size; i++) {
            adjacent.add(in.readLong());
        }
        centroid = in.readLong();
        depth = in.readShort();
    }
}

// Main class for graph partitioning
public class GraphPartition extends Configured implements Tool {
    public static short BFS_depth = 0; // Current depth of BFS
    public static short max_depth = 8;  // Maximum depth for BFS

    // Mapper class to read graph data
    public static class GraphReaderMapper extends Mapper<LongWritable, Text, LongWritable, VertexWritable> {
        private int centroidCount = 0; // Counter for centroids

        // Map method to process each line of the input
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input line into tokens
            String[] tokens = value.toString().split(",");
            long id = Long.parseLong(tokens[0]); // Vertex ID
            Vector<Long> adjacent = new Vector<>(); // List of adjacent vertices
            for (int i = 1; i < tokens.length; i++) {
                adjacent.add(Long.parseLong(tokens[i])); // Add adjacent vertices
            }
            long centroid = (centroidCount < 10) ? id : -1; // Set centroid if within limit
            centroidCount++;

            // Create VertexWritable object and write it to context
            VertexWritable vertex = new VertexWritable(id, adjacent, centroid, (short) 0);
            context.write(new LongWritable(id), vertex);
        }
    }

    // Mapper class for BFS traversal
    public static class BFSMapper extends Mapper<LongWritable, VertexWritable, LongWritable, VertexWritable> {
        // Map method to process vertex data
        public void map(LongWritable key, VertexWritable vertex, Context context) throws IOException, InterruptedException {
            // Write the vertex to context
            context.write(new LongWritable(vertex.id), vertex);  
            // If the vertex is a centroid, propagate to its neighbors
            if (vertex.centroid > 0) {
                for (Long n : vertex.adjacent) {
                    VertexWritable neighbor = new VertexWritable(n, new Vector<>(), vertex.centroid, BFS_depth);
                    context.write(new LongWritable(n), neighbor);
                }
            }
        }
    }

    // Reducer class for processing BFS results
    public static class BFSReducer extends Reducer<LongWritable, VertexWritable, LongWritable, VertexWritable> {
        // Reduce method to aggregate vertex data
        public void reduce(LongWritable key, Iterable<VertexWritable> values, Context context) throws IOException, InterruptedException {
            Vector<Long> adjacent = new Vector<>(); // List of adjacent vertices
            long centroid = -1;                     // Centroid of the cluster
            short minDepth = 1000;                  // Minimum depth encountered

            // Iterate over vertex values
            for (VertexWritable v : values) {
                if (!v.adjacent.isEmpty()) {
                    adjacent = v.adjacent; // Update adjacent vertices
                }
                // Update centroid if found a closer one
                if (v.centroid > 0 && v.depth < minDepth) {
                    minDepth = v.depth;
                    centroid = v.centroid;
                }
            }

            // Create result vertex and write to context
            VertexWritable result = new VertexWritable(key.get(), adjacent, centroid, minDepth);
            context.write(key, result);
        }
    }

    // Mapper class for calculating cluster sizes
    public static class ClusterSizeMapper extends Mapper<LongWritable, VertexWritable, LongWritable, IntWritable> {
        // Map method to count vertices belonging to each centroid
        public void map(LongWritable key, VertexWritable value, Context context) throws IOException, InterruptedException {
            if (value.centroid > 0) {
                context.write(new LongWritable(value.centroid), new IntWritable(1)); // Emit centroid and count
            }
        }
    }

    // Reducer class for aggregating cluster sizes
    public static class ClusterSizeReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
        // Reduce method to sum up counts for each centroid
        public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get(); // Aggregate counts
            }
            context.write(key, new IntWritable(count)); // Emit centroid and total count
        }
    }

    // Main method to run the MapReduce jobs
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        // First job to read graph data
        Job job = Job.getInstance(conf, "Graph Partition - Read Graph");
        job.setJarByClass(GraphPartition.class);
        job.setMapperClass(GraphReaderMapper.class);
        job.setReducerClass(BFSReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(VertexWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Set input and output paths
        TextInputFormat.addInputPath(job, new Path(args[0])); // Input graph file
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1] + "/i0")); // Intermediate output
        job.waitForCompletion(true); // Wait for job completion

        // BFS iterations (second Map-Reduce job)
        for (short i = 0; i < max_depth; i++) {
            BFS_depth++;
            job = Job.getInstance(conf, "Graph Partition - BFS Iteration " + i);
            job.setJarByClass(GraphPartition.class);
            job.setMapperClass(BFSMapper.class);
            job.setReducerClass(BFSReducer.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(VertexWritable.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            SequenceFileInputFormat.addInputPath(job, new Path(args[1] + "/i" + i)); // Intermediate input
            SequenceFileOutputFormat.setOutputPath(job, new Path(args[1] + "/i" + (i + 1))); // Next output
            job.waitForCompletion(true); // Wait for job completion
        }

        // Final job to calculate cluster sizes
        job = Job.getInstance(conf, "Graph Partition - Calculate Cluster Sizes");
        job.setJarByClass(GraphPartition.class);
        job.setMapperClass(ClusterSizeMapper.class);
        job.setReducerClass(ClusterSizeReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job, new Path(args[1] + "/i" + max_depth)); // Last intermediate output
        TextOutputFormat.setOutputPath(job, new Path(args[2])); // Final output path
        job.waitForCompletion(true); // Wait for job completion

        // Print final output (cluster sizes)
        System.out.println("Cluster sizes:");
        // Read the output from the final job and print it (this should be done after job completion)
        // Note: This requires additional code to read the output files and print the results.
        
        return 0; // Return success
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new GraphPartition(), args);
        System.exit(res);
    }
}
