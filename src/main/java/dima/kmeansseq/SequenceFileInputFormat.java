package dima.kmeansseq;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.InputMismatchException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;

import dima.kmeansseq.SequenceFile.Metadata;
import dima.kmeansseq.SequenceFile.Reader;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.fs.hdfs.DistributedDataInputStream;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;

/**
 * Class to read Hadoop Sequencefiles with Stratosphere.
 * 
 * Classes for for Deserialisation must be supplied.
 * 
 * @author Michael Huelfenhaus
 * 
 */
public class SequenceFileInputFormat extends FileInputFormat {

    private static final Log LOG = LogFactory
            .getLog(ReadingMahoutVectors.class);

    // parameter names to read class of key and value from configuration
    public static final String KEY_CLASS_PARAMETER = "keyClass";
    public static final String VALUE_CLASS_PARAMETER = "valueClass";

    private static class UncompressedBytes implements ValueBytes {
        private int dataSize;
        private byte[] data;

        private UncompressedBytes() {
            data = null;
            dataSize = 0;
        }

        private void reset(DataInputStream in, int length) throws IOException {
            data = new byte[length];
            dataSize = -1;

            in.readFully(data);
            dataSize = data.length;
        }

        public byte[] getData() {
            return data;
        }

        public int getSize() {
            return dataSize;
        }

        public void writeUncompressedBytes(DataOutputStream outStream)
                throws IOException {
            outStream.write(data, 0, dataSize);
        }

        public void writeCompressedBytes(DataOutputStream outStream)
                throws IllegalArgumentException, IOException {
            throw new IllegalArgumentException(
                    "UncompressedBytes cannot be compressed!");
        }

    } // UncompressedBytes

    private static class CompressedBytes implements ValueBytes {
        private int dataSize;
        private byte[] data;

        DataInputBuffer rawData = null;
        CompressionCodec codec = null;
        CompressionInputStream decompressedStream = null;

        public byte[] getData() {
            return data;
        }

        private CompressedBytes(CompressionCodec codec) {
            data = null;
            dataSize = 0;
            this.codec = codec;
        }

        private void reset(DataInputStream in, int length) throws IOException {
            data = new byte[length];
            dataSize = -1;

            in.readFully(data);
            dataSize = data.length;
        }

        public int getSize() {
            return dataSize;
        }

        public void writeUncompressedBytes(DataOutputStream outStream)
                throws IOException {
            if (decompressedStream == null) {
                rawData = new DataInputBuffer();
                decompressedStream = codec.createInputStream(rawData);
            } else {
                decompressedStream.resetState();
            }
            rawData.reset(data, 0, dataSize);

            byte[] buffer = new byte[8192];
            int bytesRead = 0;
            while ((bytesRead = decompressedStream.read(buffer, 0, 8192)) != -1) {
                outStream.write(buffer, 0, bytesRead);
            }
        }

        public void writeCompressedBytes(DataOutputStream outStream)
                throws IllegalArgumentException, IOException {
            outStream.write(data, 0, dataSize);
        }

    } // CompressedBytes

    /**
     * Creates a configuration builder that can be used to set the input
     * format's parameters to the config in a fluent fashion.
     * 
     * @return A config builder for setting parameters.
     */
    public static ConfigBuilder configureSequenceFormat(FileDataSource source) {
        return new ConfigBuilder(source.getParameters());
    }

    /**
     * Abstract builder used to set parameters to the input format's
     * configuration in a fluent way.
     */
    protected static abstract class AbstractConfigBuilder<T> extends
            FileOutputFormat.AbstractConfigBuilder<T> {

        /**
         * Creates a new builder for the given configuration.
         * 
         * @param targetConfig
         *            The configuration into which the parameters will be
         *            written.
         */
        protected AbstractConfigBuilder(Configuration config) {
            super(config);
        }

        /**
         * Sets the class of key and value in the configuration of the input
         * format. The read methods of the classes are used for deserialisation.
         * 
         * @param keyClass
         *            the class of the key
         * @param valueClass
         *            the class of the value
         * @return The builder itself.
         */
        public T setClasses(Class<? extends Key> keyClass,
                Class<? extends Value> valueClass) {

            this.config.setClass(KEY_CLASS_PARAMETER, keyClass);
            this.config.setClass(VALUE_CLASS_PARAMETER, valueClass);

            @SuppressWarnings("unchecked")
            T ret = (T) this;
            return ret;
        }
    }

    /**
     * A builder used to set parameters to the input format's configuration in a
     * fluent way.
     */
    public static final class ConfigBuilder extends
            AbstractConfigBuilder<ConfigBuilder> {
        /**
         * Creates a new builder for the given configuration.
         * 
         * @param targetConfig
         *            The configuration into which the parameters will be
         *            written.
         */
        protected ConfigBuilder(Configuration targetConfig) {
            super(targetConfig);
        }
    }

    /**
     * Extension of the ByteStream that allows to change the internal buffer
     * without creation of a new object.
     * 
     * @author Michael Huelfenhaus
     */
    private static class ChangeableInput extends ByteArrayInputStream {

        public ChangeableInput(byte[] buf) {
            super(buf);
        }

        /**
         * Set the internal buffer to the given byte array and reset all
         * position markers.
         * 
         * @param buf
         *            new value for the internal buffer
         */
        public void setBuffer(byte[] buf) {
            this.buf = buf;
            this.pos = 0;
            this.count = buf.length;
        }

    }

    private static final byte BLOCK_COMPRESS_VERSION = (byte) 4;
    private static final byte CUSTOM_COMPRESS_VERSION = (byte) 5;
    private static final byte VERSION_WITH_METADATA = (byte) 6;
    private static byte[] VERSION = new byte[] { (byte) 'S', (byte) 'E',
            (byte) 'Q', VERSION_WITH_METADATA };

    private static final int SYNC_ESCAPE = -1; // "length" of sync entries
    private static final int SYNC_HASH_SIZE = 16; // number of bytes in hash
    private static final int SYNC_SIZE = 4 + SYNC_HASH_SIZE; // escape +
                                                             // hash

    /** The number of bytes between sync points. */
    public static final int SYNC_INTERVAL = 100 * SYNC_SIZE;

    private Path file;

    private byte version;

    private Class keyClass;
    private Class valClass;

    private org.apache.hadoop.conf.Configuration conf;

    private boolean blockCompressed;

    private boolean decompress;
    private boolean lazyDecompress = true;
    private boolean valuesDecompressed = true;
    // private boolean syncSeen;

    private byte[] sync = new byte[SYNC_HASH_SIZE];
    private byte[] syncCheck = new byte[SYNC_HASH_SIZE];

    private int noBufferedKeys = 0;
    // private int noBufferedValues = 0;
    private int noBufferedRecords = 0;

    private long end;
    // private int keyLength;
    // private int recordLength;

    private DataInputBuffer keyLenBuffer = null;
    private CompressionInputStream keyLenInFilter = null;
    private DataInputStream keyLenIn = null;
    private Decompressor keyLenDecompressor = null;
    private DataInputBuffer keyBuffer = null;
    private CompressionInputStream keyInFilter = null;
    private DataInputStream keyIn = null;
    private Decompressor keyDecompressor = null;

    private DataInputBuffer valLenBuffer = null;
    private CompressionInputStream valLenInFilter = null;
    private DataInputStream valLenIn = null;
    private Decompressor valLenDecompressor = null;
    private DataInputBuffer valBuffer = null;
    private CompressionInputStream valInFilter = null;
    private DataInputStream valIn = null;
    private Decompressor valDecompressor = null;

    /** */
    private org.apache.hadoop.fs.FSDataInputStream hadoopIn;

    // private DataOutputBuffer outBuf = new DataOutputBuffer();

    private CompressionCodec codec = null;
    private Metadata metadata = null;

    private String keyClassName;
    private String valClassName;

    // in these streams the byte data from the files is written
    private DataInputStream keyDataStream = null;
    private DataInputStream valueDataStream = null;

    private ChangeableInput keyInStream = null;
    private ChangeableInput valueInStream = null;

    // for intermediate step in reading values
    DataOutputBuffer keyBytes = null;
    ValueBytes valBytes = null;

    // // true if file header was read and reader initialised
    // private boolean initDone = false;

    private int counter = 0;

    private Value value = null;

    private Value key = null;

    private boolean readWorked;

    public SequenceFileInputFormat() {
        conf = new org.apache.hadoop.conf.Configuration();

        // valIn = new DataInputBuffer();
        keyBytes = new DataOutputBuffer();

        keyInStream = new ChangeableInput(new byte[0]);
        keyDataStream = new DataInputStream(keyInStream);

        valueInStream = new ChangeableInput(new byte[0]);
        valueDataStream = new DataInputStream(valueInStream);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);

        // read key class
        this.keyClass = parameters.getClass(KEY_CLASS_PARAMETER, null);
        // make instance of key class
        try {
            key = (Value) keyClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        // read value class
        this.valClass = parameters.getClass(VALUE_CLASS_PARAMETER, null);
        // make instance of value class
        try {
            value = (Value) valClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Tell whether the end of the file split was reached.
     */
    public boolean reachedEnd() throws IOException {
        if (hadoopIn.getPos() >= end) {
            return true;
        }

        // read bytes for key and value from file into global buffers
        nextRaw();

        // System.out.println(counter);
        // try{
        //
        // // deserialize key
        // key.read(keyDataStream);
        // } catch (Exception e) {
        // System.out.println(counter);
        // // TODO: handle exception
        // }
        try {
            readWorked = true;
            // deserialize key
            key.read(keyDataStream);

            // deserialize value
            value.read(valueDataStream);

        } catch (Exception e) {
//            readWorked = false;
            return true;
        }

        return false;
    }

    /**
     * Reads the next key value pair from the sequence file in to the Pact
     * record.
     * 
     * Key and value are deserialised with the methods provided by the class of
     * key and value.
     * 
     * The key is saved in field 0 The value is saved in field 1.
     * 
     * @param record
     *            key and value are set in this record in fields 0 and 1
     * @return true is reading was successful, false otherwise
     */
    public boolean nextRecord(PactRecord record) throws IOException {
        // if (this.reachedEnd()) {
        // return false;
        // }

        // // read bytes for key and value from file into global buffers
        // nextRaw();
        //
        // // System.out.println(counter);
        // // try{
        // //
        // // // deserialize key
        // // key.read(keyDataStream);
        // // } catch (Exception e) {
        // // System.out.println(counter);
        // // // TODO: handle exception
        // // }
        //
        // // deserialize key
        // key.read(keyDataStream);
        //
        // // deserialize value
        // value.read(valueDataStream);

        
        // set field of pact record: 0 for key, 1 for value
        record.setField(0, key);
        record.setField(1, value);

        counter++;
//        System.out.println(counter + " " + key);// + " || " + value);

        return true;
    }

    @Override
    public void open(FileInputSplit split) throws IOException {
        super.open(split);
        file = split.getPath();

        if (stream instanceof DistributedDataInputStream) {
            hadoopIn = ((DistributedDataInputStream) stream)
                    .getHadoopInputStream();
        } else {
            // TODO check exception type
            throw new InputMismatchException("Only hdfs paths valid");
        }
        // calculate endpostion of split
        end = split.getStart() + split.getLength();

        // Initialise sequence file by reading header
        init();

    }

    /**
     * Initialize the {@link Reader}
     * 
     * @param tmpReader
     *            <code>true</code> if we are constructing a temporary reader
     *            {@link SequenceFile.Sorter.cloneFileAttributes}, and hence do
     *            not initialize every component; <code>false</code> otherwise.
     * @throws IOException
     */
    @SuppressWarnings("deprecation")
    private void init() throws IOException {
        // save position in input stream
        long initialReadingPos = hadoopIn.getPos();
        // go to begin of file to read header
        hadoopIn.seek(0);

        byte[] versionBlock = new byte[VERSION.length];
        hadoopIn.readFully(versionBlock);

        if ((versionBlock[0] != VERSION[0]) || (versionBlock[1] != VERSION[1])
                || (versionBlock[2] != VERSION[2]))
            throw new IOException(file + " not a SequenceFile");

        // Set 'version'
        version = versionBlock[3];
        if (version > VERSION[3])
            throw new VersionMismatchException(VERSION[3], version);

        if (version < BLOCK_COMPRESS_VERSION) {
            UTF8 className = new UTF8();

            className.readFields(hadoopIn);
            keyClassName = className.toString(); // key class name

            className.readFields(hadoopIn);
            valClassName = className.toString(); // val class name
        } else {
            keyClassName = Text.readString(hadoopIn);
            valClassName = Text.readString(hadoopIn);
        }

        if (version > 2) { // if version > 2
            this.decompress = hadoopIn.readBoolean(); // is compressed?
        } else {
            decompress = false;
        }

        if (version >= BLOCK_COMPRESS_VERSION) { // if version >= 4
            this.blockCompressed = hadoopIn.readBoolean(); // is
                                                           // block-compressed?
        } else {
            blockCompressed = false;
        }

        // if version >= 5
        // setup the compression codec
        if (decompress) {
            if (version >= CUSTOM_COMPRESS_VERSION) {
                String codecClassname = Text.readString(hadoopIn);
                try {
                    Class<? extends CompressionCodec> codecClass = conf
                            .getClassByName(codecClassname).asSubclass(
                                    CompressionCodec.class);
                    this.codec = ReflectionUtils.newInstance(codecClass, conf);
                } catch (ClassNotFoundException cnfe) {
                    throw new IllegalArgumentException("Unknown codec: "
                            + codecClassname, cnfe);
                }
            } else {
                codec = new DefaultCodec();
                ((Configurable) codec).setConf(conf);
            }
        }

        this.metadata = new Metadata();
        if (version >= VERSION_WITH_METADATA) { // if version >= 6
            this.metadata.readFields(hadoopIn);
        }

        if (version > 1) { // if version > 1
            hadoopIn.readFully(sync); // read sync bytes
        }

        valBuffer = new DataInputBuffer();
        if (decompress) {
            valDecompressor = CodecPool.getDecompressor(codec);
            valInFilter = codec.createInputStream(valBuffer, valDecompressor);
            valIn = new DataInputStream(valInFilter);
        } else {
            valIn = valBuffer;
        }

        if (blockCompressed) {
            keyLenBuffer = new DataInputBuffer();
            keyBuffer = new DataInputBuffer();
            valLenBuffer = new DataInputBuffer();

            keyLenDecompressor = CodecPool.getDecompressor(codec);
            keyLenInFilter = codec.createInputStream(keyLenBuffer,
                    keyLenDecompressor);
            keyLenIn = new DataInputStream(keyLenInFilter);

            keyDecompressor = CodecPool.getDecompressor(codec);
            keyInFilter = codec.createInputStream(keyBuffer, keyDecompressor);
            keyIn = new DataInputStream(keyInFilter);

            valLenDecompressor = CodecPool.getDecompressor(codec);
            valLenInFilter = codec.createInputStream(valLenBuffer,
                    valLenDecompressor);
            valLenIn = new DataInputStream(valLenInFilter);
        }
        // check if initial reading position was the file start
        if (initialReadingPos != 0) {
            // go back to initial Position at split start
            sync(splitStart);
        }

        // creating intermediate buffer for value bytes
        // moved from nextRaw to reduce object creation
        if (decompress) {
            valBytes = new CompressedBytes(codec);
        } else {
            valBytes = new UncompressedBytes();
        }
    }

    /** Read a compressed buffer */
    private synchronized void readBuffer(DataInputBuffer buffer,
            CompressionInputStream filter) throws IOException {
        // Read data into a temporary buffer
        DataOutputBuffer dataBuffer = new DataOutputBuffer();

        try {
            int dataBufferLength = WritableUtils.readVInt((DataInput) stream);
            dataBuffer.write((DataInput) stream, dataBufferLength);

            // Set up 'buffer' connected to the input-stream
            buffer.reset(dataBuffer.getData(), 0, dataBuffer.getLength());
        } finally {
            dataBuffer.close();
        }

        // Reset the codec
        filter.resetState();
    }

    /** Read the next 'compressed' block */
    private synchronized void readBlock() throws IOException {
        // Check if we need to throw away a whole block of
        // 'values' due to 'lazy decompression'
        // DataInput in = (DataInput) stream;

        if (lazyDecompress && !valuesDecompressed) {
            hadoopIn.seek(WritableUtils.readVInt(hadoopIn) + hadoopIn.getPos());
            hadoopIn.seek(WritableUtils.readVInt(hadoopIn) + hadoopIn.getPos());
        }

        // Reset internal states
        noBufferedKeys = 0;
        // noBufferedValues = 0;
        noBufferedRecords = 0;
        valuesDecompressed = false;

        // Process sync
        if (sync != null) {
            hadoopIn.readInt();
            hadoopIn.readFully(syncCheck); // read syncCheck
            if (!Arrays.equals(sync, syncCheck)) // check it
                throw new IOException("File is corrupt!");
        }
        // syncSeen = true;

        // Read number of records in this block
        noBufferedRecords = WritableUtils.readVInt(hadoopIn);

        // Read key lengths and keys
        readBuffer(keyLenBuffer, keyLenInFilter);
        readBuffer(keyBuffer, keyInFilter);
        noBufferedKeys = noBufferedRecords;

        // Read value lengths and values
        if (!lazyDecompress) {
            readBuffer(valLenBuffer, valLenInFilter);
            readBuffer(valBuffer, valInFilter);
            // noBufferedValues = noBufferedRecords;
            valuesDecompressed = true;
        }
    }

    /**
     * Set the current byte position in the input file.
     * 
     * <p>
     * The position passed must be a position returned by
     * {@link SequenceFile.Writer#getLength()} when writing this file. To seek
     * to an arbitrary position, use {@link SequenceFile.Reader#sync(long)}.
     */
    public synchronized void seek(long position) throws IOException {
        hadoopIn.seek(position);
        if (blockCompressed) { // trigger block read
            noBufferedKeys = 0;
            valuesDecompressed = true;
        }
    }

    /** Seek to the next sync mark past a given position. */
    public synchronized void sync(long position) throws IOException {
        if (position + SYNC_SIZE >= end) {
            seek(end);
            return;
        }

        try {
            seek(position + 4); // skip escape
            hadoopIn.readFully(syncCheck);
            int syncLen = sync.length;
            for (int i = 0; hadoopIn.getPos() < end; i++) {
                int j = 0;
                for (; j < syncLen; j++) {
                    if (sync[j] != syncCheck[(i + j) % syncLen])
                        break;
                }
                if (j == syncLen) {
                    hadoopIn.seek(hadoopIn.getPos() - SYNC_SIZE); // position
                                                                  // before
                    // sync
                    return;
                }
                syncCheck[i % syncLen] = hadoopIn.readByte();
            }
        } catch (ChecksumException e) { // checksum failure
            handleChecksumException(e);
        }
    }

    private void handleChecksumException(ChecksumException e)
            throws IOException {
        if (this.conf.getBoolean("io.skip.checksum.errors", false)) {
            LOG.warn("Bad checksum at " + hadoopIn.getPos()
                    + ". Skipping entries.");
            sync(hadoopIn.getPos()
                    + this.conf.getInt("io.bytes.per.checksum", 512));
        } else {
            throw e;
        }
    }

    /**
     * Read 'raw' records.
     * 
     * @param key
     *            - The buffer into which the key is read
     * @param val
     *            - The 'raw' value
     * @return Returns the total record length or -1 for end of file
     * @throws IOException
     */
    public synchronized int nextRaw()
    // public synchronized int nextRaw(DataInputStream keyStream,
    // final DataInputStream valueStream)// , ValueBytes val)

            // public synchronized int nextRaw( DataInput keyStream,
            // ValueBytes val)
            // public synchronized int nextRaw(ByteArrayOutputStream key,
            // ValueBytes
            // val)
            throws IOException {

        if (!blockCompressed) {
            int length = readRecordLength();
            if (length == -1) {
                return -1;
            }

            // read key and value length
            int keyLength = hadoopIn.readInt();
            int valLength = length - keyLength;

            // read the key

            // reset key output buffer
            keyBytes.reset();

            // read bytes of the key
            keyBytes.write(hadoopIn, keyLength);

            // set bytes from output buffer in the inputstream without creating
            // new objects
            keyInStream.setBuffer(keyBytes.getData());

            // read the value
            if (decompress) {
                CompressedBytes value = (CompressedBytes) valBytes;
                value.reset(hadoopIn, valLength);
                valueInStream.setBuffer(value.getData());
            } else {
                UncompressedBytes value = (UncompressedBytes) valBytes;
                value.reset(hadoopIn, valLength);
                valueInStream.setBuffer(value.getData());
            }

            return length;
        } else {
            // TODO not working
            return -1;
            //
            // // Reset syncSeen
            // // syncSeen = false;
            //
            // // Read 'key'
            // if (noBufferedKeys == 0) {
            // if (hadoopIn.getPos() >= end)
            // return -1;
            //
            // try {
            // readBlock();
            // } catch (EOFException eof) {
            // return -1;
            // }
            // }
            // int keyLength = WritableUtils.readVInt(keyLenIn);
            // if (keyLength < 0) {
            // throw new IOException("zero length key found!");
            // }
            //
            // // reset key output buffer
            // keyBytes.reset();
            //
            // // // read bytes of the key
            // keyBytes.write(hadoopIn, keyLength);
            //
            // // set bytes from output buffer in the inputstream without
            // creating
            // // new objects
            // keyInStream.setBuffer(keyBytes.getData());
            //
            // --noBufferedKeys;
            //
            //
            //
            // // Read raw 'value'
            // // seekToCurrentValue();
            // int valLength = WritableUtils.readVInt(valLenIn);
            // UncompressedBytes rawValue = (UncompressedBytes) valBytes;
            // // rawValue.reset(valIn, valLength);
            // // --noBufferedValues;
            //
            // valueDataStream = new DataInputStream(new ByteArrayInputStream(
            // rawValue.getData()));
            //
            // // when -1 reachedEnd True
            // return (keyLength + valLength);

        }

    }

    /**
     * Read and return the next record length, potentially skipping over a sync
     * block.
     * 
     * @return the length of the next record or -1 if there is no next record
     * @throws IOException
     */
    private synchronized int readRecordLength() throws IOException {
        if (hadoopIn.getPos() >= end) {
            return -1;
        }
        int length = hadoopIn.readInt();
        if (version > 1 && sync != null && length == SYNC_ESCAPE) { // process
                                                                    // a
                                                                    // sync
                                                                    // entry
            hadoopIn.readFully(syncCheck); // read syncCheck
            if (!Arrays.equals(sync, syncCheck)) // check it
                throw new IOException("File is corrupt!");
            // syncSeen = true;
            if (hadoopIn.getPos() >= end) {
                return -1;
            }
            length = hadoopIn.readInt(); // re-read length
        } else {
            // syncSeen = false;
        }

        return length;
    }

}
