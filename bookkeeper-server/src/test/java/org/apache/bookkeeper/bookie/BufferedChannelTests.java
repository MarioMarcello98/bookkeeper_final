package org.apache.bookkeeper.bookie;

import edu.emory.mathcs.backport.java.util.Arrays;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.hamcrest.number.IsNaN;
import org.junit.Assert;

import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;


@RunWith(value= Enclosed.class)
public class BufferedChannelTests {

    protected static int WRITE_CAPACITY = 512;
    protected static int READ_CAPACITY = 512;

    @RunWith(Parameterized.class)
    public static class writeTest {

        private ByteBuf byteBuf;

        private BufferedChannel buffChannel;
        private long expectedPosition;

        @Parameterized.Parameters
        public static Collection<Object[][]> getParameter() {
            ByteBuf byteBufferMock = Mockito.mock(ByteBuf.class);
            //violo condizione readerIndex<=writerIndex
            byteBufferMock.writerIndex(WRITE_CAPACITY);
            byteBufferMock.readerIndex(WRITE_CAPACITY + 1);

            return Arrays.asList(new Object[][]{
                    //Unidimensional approach, I put in each test suite an invalid value
                    //BYTE BUFFER                                        EXPECTED_POSITION
                    {byteBufferMock, 0},
                    {Unpooled.wrappedBuffer(new byte[WRITE_CAPACITY]), WRITE_CAPACITY},

            });
        }

        //constructor
        public writeTest(ByteBuf b, long expPos) throws IOException {

            File tempFile = File.createTempFile("file", "log");
            tempFile.deleteOnExit();
            FileChannel fileChannel = new RandomAccessFile(tempFile, "rw").getChannel();

            this.buffChannel = new BufferedChannel(ByteBufAllocator.DEFAULT, fileChannel, WRITE_CAPACITY);

            this.byteBuf = b;
            this.expectedPosition = expPos;

        }

        @Test
        public void writeTest() throws IOException {

            buffChannel.write(this.byteBuf);
            Assert.assertEquals(this.expectedPosition, buffChannel.position());


        }


    }

    @RunWith(Parameterized.class)
    public static class readTest {


        private FileChannel fileChannel;
        private int length;
        private int expectedResult;
        private ByteBuf byteBuf;

        private BufferedChannel buffChannel;

        private long position;
        private boolean expectedException;

        @Parameterized.Parameters
        public static Collection<Object[][]> getParameter() throws IOException {


            return Arrays.asList(new Object[][]{

                    //Unidimensional approach, I put in each test suite an invalid value
                    //BYTE_BUFFER                                            POSITION             LENGTH              EXPECTED_EXCEPTION

                    {Unpooled.buffer(0), 0L, 0, false},
                    {Unpooled.buffer(WRITE_CAPACITY), 0L, WRITE_CAPACITY, false},
                    {Unpooled.buffer(WRITE_CAPACITY), 0L, WRITE_CAPACITY + 1, true},
                    {Unpooled.buffer(WRITE_CAPACITY), -1L, WRITE_CAPACITY, true},
                    {Unpooled.buffer(WRITE_CAPACITY), 1L, WRITE_CAPACITY - 1, false},


            });
        }

        @Before
        public void writeOperation() throws IOException {
            File tempFile = File.createTempFile("file", "log");
            tempFile.deleteOnExit();
            fileChannel = new RandomAccessFile(tempFile, "rw").getChannel();
            buffChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fileChannel, WRITE_CAPACITY, READ_CAPACITY);
            ByteBuf writeBuf = Unpooled.buffer(WRITE_CAPACITY);
            byte[] data = new byte[WRITE_CAPACITY];
            Random random = new Random();
            random.nextBytes(data);
            writeBuf.writeBytes(data);
            buffChannel.write(writeBuf);
        }

        @After
        public void clean() throws IOException {

            buffChannel.clear();
            buffChannel.close();
        }

        //constructor
        public readTest(ByteBuf b, long pos, int length, boolean e) throws IOException {
            //init the write buffer


            this.byteBuf = b;

            this.position = pos;
            this.length = length;
            if (length > 0) {
                this.expectedResult = length;
            } else {
                this.expectedResult = 0;
            }

            this.expectedException = e;


        }

        @Test
        public void readTest() {
            try {

                Assert.assertEquals(this.expectedResult, buffChannel.read(this.byteBuf, this.position, this.length));
            } catch (IOException e) {
                Assert.assertTrue(this.expectedException);
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(this.expectedException);

            }


        }

    }


    //TEST CON APPROCCIO WHITE BOX
    @RunWith(Parameterized.class)
    public static class whiteboxReadTest {


        private FileChannel fileChannel;
        private int length;
        private int expectedResult;
        private boolean readNegative;
        private ByteBuf byteBuf;

        private BufferedChannel buffChannel;

        private long position;
        private boolean expectedException;

        @Parameterized.Parameters
        public static Collection<Object[][]> getParameter() throws IOException {


            return Arrays.asList(new Object[][]{


                    //nei precedenti casi di test ho preso un buffer con la stessa dimensione di byteBuf, ora lo prendo sia più piccolo che più grande

                    //READ_CAPACITY             BYTE_BUFFER                                         POS                     LENGHT                     EXPECTED_RESULT      EXPECTED_EXCEPTION
                    {false, Unpooled.buffer(WRITE_CAPACITY / 2), 0L, WRITE_CAPACITY, WRITE_CAPACITY, true},

                    {true, Unpooled.buffer(WRITE_CAPACITY / 2), 0L, WRITE_CAPACITY / 2, WRITE_CAPACITY / 2, true},
                    {false, Unpooled.buffer(WRITE_CAPACITY / 2), 0L, WRITE_CAPACITY, 0, true},
                    {false, Unpooled.buffer(WRITE_CAPACITY * 2), 0L, WRITE_CAPACITY, WRITE_CAPACITY, false},
                    {false, Unpooled.buffer(WRITE_CAPACITY * 2), 0L, WRITE_CAPACITY * 2, 0, true},
                    {false, Unpooled.buffer(WRITE_CAPACITY / 2), 0L, WRITE_CAPACITY / 4, WRITE_CAPACITY / 2, false}
            });
        }


        @After
        public void clean() throws IOException {
            if (!this.readNegative) {
                buffChannel.clear();
                buffChannel.close();
            }
        }

        //constructor
        public whiteboxReadTest(boolean readNegative, ByteBuf b, long pos, int length, int expected_result, boolean e) throws IOException {
            //init the write buffer

            this.readNegative = readNegative;
            this.byteBuf = b;

            this.position = pos;
            this.length = length;
            this.expectedResult = expected_result;


            this.expectedException = e;
            if (!readNegative) {

                File tempFile = File.createTempFile("file", "log");
                tempFile.deleteOnExit();
                FileChannel fileChannel = new RandomAccessFile(tempFile, "rw").getChannel();
                buffChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fileChannel, WRITE_CAPACITY, READ_CAPACITY);
                ByteBuf writeBuf = Unpooled.buffer(WRITE_CAPACITY);
                byte[] data = new byte[WRITE_CAPACITY];
                Random random = new Random();
                random.nextBytes(data);
                writeBuf.writeBytes(data);
                buffChannel.write(writeBuf);
                this.buffChannel.writeBufferStartPosition.set(0);
                this.buffChannel.writeBuffer.writerIndex(WRITE_CAPACITY);
                this.byteBuf.ensureWritable(byteBuf.capacity());
            } else {
                fileChannel = Mockito.mock(FileChannel.class);
                buffChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fileChannel, WRITE_CAPACITY, READ_CAPACITY);
                Mockito.when(fileChannel.read(ArgumentMatchers.anyObject(), ArgumentMatchers.anyLong())).thenReturn(-1);
                this.buffChannel.writeBufferStartPosition.set(pos + 1);


            }


        }

        @Test
        public void whiteboxReadTest() {
            try {
                Assert.assertEquals(this.expectedResult, buffChannel.read(this.byteBuf, this.position, this.length));
            } catch (IOException e) {
                Assert.assertTrue(this.expectedException);
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(this.expectedException);

            }


        }
    }

    @RunWith(Parameterized.class)
    public static  class whiteboxWriteTests{

        private final AtomicLong expectedUnpersistedBytes;
        private ByteBuf byteBuf;

        private BufferedChannel buffChannel;
        private long expectedPosition;
        private boolean expectedException;
        @Before
        public void init() throws IOException{
            File tempFile = File.createTempFile("file", "log");
            tempFile.deleteOnExit();
            FileChannel fileChannel = new RandomAccessFile(tempFile, "rw").getChannel();
            buffChannel=  new BufferedChannel(ByteBufAllocator.DEFAULT, fileChannel, WRITE_CAPACITY, READ_CAPACITY, 100);
        }
        @After
        public void clear() throws IOException{
            buffChannel.clear();
            buffChannel.close();
        }
        @Parameterized.Parameters
        public static Collection<Object[][]> getParameter()  throws IOException{





            return Arrays.asList(new Object[][]{
                    //Unidimensional approach
                    // BYTE BUFFER                                     EXPECTED_UNPERSISTED_BYTES

                    //visto che UnpersistedBytesBound=100, doRegularFlushes=true
                    {      Unpooled.wrappedBuffer(new byte[99]),            new AtomicLong(99)},
                    {   Unpooled.wrappedBuffer(new byte[100]),           new AtomicLong(0)},
                    {     Unpooled.wrappedBuffer(new byte[300]),         new AtomicLong(0)}


            });
        }
        //constructor
        public whiteboxWriteTests(ByteBuf b, AtomicLong expUnpBytes ) throws IOException{

            this.byteBuf=b;

            this.expectedUnpersistedBytes=expUnpBytes;

        }
        @Test
        public void whiteboxWriteTest() {


            try {
                buffChannel.write(this.byteBuf);
                Assert.assertEquals(this.expectedUnpersistedBytes.get(), buffChannel.unpersistedBytes.get());
            }catch(IOException e){
                Assert.assertTrue(this.expectedException);
            }

        }


    }


}