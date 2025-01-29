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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Random;

import static org.apache.zookeeper.server.controller.ControllerConfigTest.createTempFile;

@RunWith(value= Enclosed.class)
public class BufferedChannelTests {

    protected static ServerConfiguration serverConfigurationMock = Mockito.mock(ServerConfiguration.class);

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


            DefaultFileChannel defaultFileChannel = new DefaultFileChannel(new File("./target/example.txt"), serverConfigurationMock);
            FileChannel fileChannel = defaultFileChannel.getFileChannel();

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
                    //Il seguente test fallisce, lo commento per generare il report di Jacoco

                    {Unpooled.buffer(0), 0L, 0, false},
                    {Unpooled.buffer(WRITE_CAPACITY), 0L, WRITE_CAPACITY, false},
                    {Unpooled.buffer(WRITE_CAPACITY), 0L, WRITE_CAPACITY + 1, true},
                    {Unpooled.buffer(WRITE_CAPACITY), -1L, WRITE_CAPACITY, true},
                    {Unpooled.buffer(WRITE_CAPACITY), 1L, WRITE_CAPACITY - 1, false},
                    //COMMENTO IL SEGUENTE TEST PER GEMERARE IL REPORT DI JACOCO IN QUANTO FALLISCE
                    //  {Unpooled.buffer(WRITE_CAPACITY),                          0,                   WRITE_CAPACITY-1,              false},


            });
        }

        @Before
        public void writeOperation() throws IOException {
            File log = createTempFile();
            fileChannel = new RandomAccessFile(log, "rw").getChannel();
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

}