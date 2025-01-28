package org.apache.bookkeeper.bookie;

import edu.emory.mathcs.backport.java.util.Arrays;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.Assert;

import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Collection;

@RunWith(value= Enclosed.class)
public class BufferedChannelTests {

    protected static ServerConfiguration serverConfigurationMock= Mockito.mock(ServerConfiguration.class);

    protected static int WRITE_CAPACITY=2048;
    protected static int READ_CAPACITY=2048;
    @RunWith(Parameterized.class)
    public static  class writeTest{

        private ByteBuf byteBuf;

        private BufferedChannel buffChannel;
        private long expectedPosition;
        private long expectedFileChannelPosition;
        @Parameterized.Parameters
        public static Collection<Object[][]> getParameter(){

            return Arrays.asList(new Object[][]{
                    //Unidimensional approach, I put in each test suite an invalid value
                    //BYTE BUFFER                                             EXPECTED_FILECHANNEL_POSITION    EXPECTED_POSITION
                    {Unpooled.wrappedBuffer(new byte[WRITE_CAPACITY*2]),      WRITE_CAPACITY*2    ,            WRITE_CAPACITY*2},
                    //la posizione del filechannel sarebbe la posizione iniziale quindi se supero la grandezza del buffer
                    // è pari al primo indice oltre il limite, se invece non la supero è 0
                    {Unpooled.wrappedBuffer(new byte[WRITE_CAPACITY-1]),      0     ,                          WRITE_CAPACITY-1}


            });
        }
        //constructor
        public writeTest(ByteBuf b, long expFCPos, long expPos) throws IOException{


            DefaultFileChannel defaultFileChannel= new DefaultFileChannel(new File("./target/example.txt"),serverConfigurationMock);
            FileChannel fileChannel=defaultFileChannel.getFileChannel();
            this.buffChannel = new BufferedChannel(ByteBufAllocator.DEFAULT, fileChannel, WRITE_CAPACITY);

            this.byteBuf=b;
            this.expectedFileChannelPosition=expFCPos;
            this.expectedPosition=expPos;

        }
        @Test
        public void writeTest()  {
            try {
                buffChannel.write(this.byteBuf);
                Assert.assertEquals( this.expectedPosition,buffChannel.position());
                Assert.assertEquals( this.expectedFileChannelPosition,buffChannel.getFileChannelPosition());
            }catch(IOException e){
                Assert.assertEquals(0,buffChannel.position());
                Assert.assertEquals( 0,buffChannel.getFileChannelPosition());
            }

        }


    }
    @RunWith(Parameterized.class)
    public static class readTest{


        private ByteBuf byteBuf;

        private  BufferedChannel buffChannel;

        private long expectedFileChannelPosition;
        private long position;
        private Class<? extends Exception> expectedException;

        @Parameterized.Parameters
        public static Collection<Object[][]> getParameter() throws IOException{

            return Arrays.asList(new Object[][]{
                    //Unidimensional approach, I put in each test suite an invalid value
                    //capacity        BYTE BUFFER                                             POSITION                           EXPECTED_POSITION
                    { WRITE_CAPACITY,              Unpooled.wrappedBuffer(new byte[READ_CAPACITY*2]),      READ_CAPACITY*2-1       ,          READ_CAPACITY*2 ,        IllegalArgumentException.class},

                    {  WRITE_CAPACITY,    Unpooled.wrappedBuffer(new byte[READ_CAPACITY]),            0,                               READ_CAPACITY,                null},
                    //Buffer di scrittura vuoto porta IOException
                    {  3,    Unpooled.wrappedBuffer(new byte[READ_CAPACITY]),            0,                               0 ,                                 IOException.class}


            });
        }
        //constructor
        public readTest(int capacity, ByteBuf b, long pos, long expPos,  Class<? extends Exception> e) throws IOException{
            DefaultFileChannel defaultFileChannel= new DefaultFileChannel(new File("./target/example.txt"),serverConfigurationMock);
            FileChannel fileChannel=defaultFileChannel.getFileChannel();

            buffChannel = new BufferedChannel(ByteBufAllocator.DEFAULT, fileChannel,  WRITE_CAPACITY);
            byte[] array= new byte[capacity];
            Arrays.fill(array, (byte)0);
            ByteBuf writeBuf=Unpooled.wrappedBuffer(array);

            buffChannel.write(writeBuf);


            this.byteBuf=b;
            this.byteBuf.writerIndex(0);
            this.byteBuf.ensureWritable(READ_CAPACITY);
            this.position=pos;
            this.expectedFileChannelPosition=expPos;
            this.expectedException=e;



        }

        @Test
        public void readTest()  {
            try {
                buffChannel.read(this.byteBuf, this.position, READ_CAPACITY);
                Assert.assertEquals( this.expectedFileChannelPosition,buffChannel.getFileChannelPosition());

            }catch(IOException e ){
                e.printStackTrace();
                Assert.assertEquals( this.expectedException,e.getClass());
            }catch (IllegalArgumentException e){
                e.printStackTrace();
                Assert.assertEquals( this.expectedException,e.getClass());
            }


        }

    }





}