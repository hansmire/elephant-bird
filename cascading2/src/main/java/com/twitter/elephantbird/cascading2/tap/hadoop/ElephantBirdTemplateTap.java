package com.twitter.elephantbird.cascading2.tap.hadoop;

import java.io.Closeable;
import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.TemplateTap;
import cascading.tap.hadoop.util.Hadoop18TapUtil;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntrySchemeCollector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: mhansmire
 * @since: 7/2/13
 */
public class ElephantBirdTemplateTap extends TemplateTap {

    private static final Logger LOG = LoggerFactory.getLogger(ElephantBirdTemplateTap.class);


    public ElephantBirdTemplateTap(Hfs parent, String pathTemplate) {
        super(parent, pathTemplate);
    }

    public ElephantBirdTemplateTap(Hfs parent, String pathTemplate, int openTapsThreshold) {
        super(parent, pathTemplate, openTapsThreshold);
    }

    public ElephantBirdTemplateTap(Hfs parent, String pathTemplate, SinkMode sinkMode) {
        super(parent, pathTemplate, sinkMode);
    }

    public ElephantBirdTemplateTap(Hfs parent, String pathTemplate, SinkMode sinkMode, boolean keepParentOnDelete) {
        super(parent, pathTemplate, sinkMode, keepParentOnDelete);
    }

    public ElephantBirdTemplateTap(Hfs parent, String pathTemplate, SinkMode sinkMode, boolean keepParentOnDelete, int openTapsThreshold) {
        super(parent, pathTemplate, sinkMode, keepParentOnDelete, openTapsThreshold);
    }

    public ElephantBirdTemplateTap(Hfs parent, String pathTemplate, Fields pathFields) {
        super(parent, pathTemplate, pathFields);
    }

    public ElephantBirdTemplateTap(Hfs parent, String pathTemplate, Fields pathFields, int openTapsThreshold) {
        super(parent, pathTemplate, pathFields, openTapsThreshold);
    }

    public ElephantBirdTemplateTap(Hfs parent, String pathTemplate, Fields pathFields, SinkMode sinkMode) {
        super(parent, pathTemplate, pathFields, sinkMode);
    }

    public ElephantBirdTemplateTap(Hfs parent, String pathTemplate, Fields pathFields, SinkMode sinkMode, boolean keepParentOnDelete) {
        super(parent, pathTemplate, pathFields, sinkMode, keepParentOnDelete);
    }

    public ElephantBirdTemplateTap(Hfs parent, String pathTemplate, Fields pathFields, SinkMode sinkMode, boolean keepParentOnDelete, int openTapsThreshold) {
        super(parent, pathTemplate, pathFields, sinkMode, keepParentOnDelete, openTapsThreshold);
    }

    @Override
    protected TupleEntrySchemeCollector createTupleEntrySchemeCollector(final FlowProcess<JobConf> flowProcess, final Tap parent, final String path) throws IOException
    {
        JobConf conf = flowProcess.getConfigCopy();
        String filenamePattern = conf.get("cascading.tapcollector.partname", "%s%spart");
        String prefix = path == null || path.length() == 0 ? null : path;

        String name;
        if (prefix != null)
            name = String.format(filenamePattern, prefix, "/");
        else
            name = String.format(filenamePattern, "", "");

        LOG.info("Setting name to " + name);

        conf.set("mapreduce.output.basename", name);
        conf.setOutputFormat(FileOutputFormat.class);
        TapOutputCollector outputCollector = new TapOutputCollector(flowProcess.copyWith(conf), parent, path);
        return new TupleEntrySchemeCollector<JobConf, OutputCollector>(flowProcess, parent, outputCollector);
    }

    public static class TapOutputCollector implements OutputCollector, Closeable
    {
        private static final Logger LOG = LoggerFactory.getLogger( TapOutputCollector.class );

        /** Field conf */
        private JobConf conf;
        /** Field writer */
        private RecordWriter writer;
        /** Field filenamePattern */
        private String filenamePattern = "%s%spart-%05d";
        /** Field filename */
        private String filename;
        /** Field tap */
        private Tap<JobConf, RecordReader, OutputCollector> tap;
        /** Field prefix */
        private String prefix;
        /** Field isFileOutputFormat */
        private boolean isFileOutputFormat;
        /** Field reporter */
        private final Reporter reporter = Reporter.NULL;
        private final FlowProcess<JobConf> flowProcess;

        public TapOutputCollector( FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap ) throws IOException
        {
            this( flowProcess, tap, null );
        }

        public TapOutputCollector( FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, String prefix ) throws IOException
        {
            this.tap = tap;
            this.prefix = prefix == null || prefix.length() == 0 ? null : prefix;
            this.flowProcess = flowProcess;
            this.conf = this.flowProcess.getConfigCopy();
            this.filenamePattern = this.conf.get( "cascading.tapcollector.partname", this.filenamePattern );

            initialize();
        }

        protected void initialize() throws IOException
        {
            tap.sinkConfInit( flowProcess, conf );

            OutputFormat outputFormat = conf.getOutputFormat();

            isFileOutputFormat = true;

            if( isFileOutputFormat )
            {
                Hadoop18TapUtil.setupJob( conf );

                if( prefix != null )
                    filename = String.format( filenamePattern, prefix, "/", conf.getInt( "mapred.task.partition", 0 ) );
                else
                    filename = String.format( filenamePattern, "", "", conf.getInt( "mapred.task.partition", 0 ) );

                Hadoop18TapUtil.setupTask( conf );
            }

            writer = outputFormat.getRecordWriter( null, conf, filename, Reporter.NULL );
        }

        /**
         * Method collect writes the given values to the {@link Tap} this instance encapsulates.
         *
         * @param writableComparable of type WritableComparable
         * @param writable           of type Writable
         * @throws IOException when
         */
        public void collect( Object writableComparable, Object writable ) throws IOException
        {
            flowProcess.keepAlive();
            writer.write( writableComparable, writable );
        }

        public void close()
        {
            try
            {
                if( isFileOutputFormat )
                    LOG.info( "closing tap collector for: {}", new Path( tap.getIdentifier(), filename ) );
                else
                    LOG.info( "closing tap collector for: {}", tap );

                try
                {
                    writer.close( reporter );
                }
                finally
                {
                    if( isFileOutputFormat )
                    {
                        if( Hadoop18TapUtil.needsTaskCommit( conf ) )
                            Hadoop18TapUtil.commitTask( conf );

                        Hadoop18TapUtil.cleanupJob( conf );
                    }
                }
            }
            catch( IOException exception )
            {
                LOG.warn( "exception closing: {}", filename, exception );
                throw new TapException( "exception closing: " + filename, exception );
            }
        }

    }


}

