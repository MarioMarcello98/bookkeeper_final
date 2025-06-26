package org.apache.bookkeeper.bookie.storage.ldb;

import edu.emory.mathcs.backport.java.util.Arrays;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.metastore.MSException;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.mockito.ArgumentMatchers;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Collection;

@RunWith(value = Enclosed.class)
public class WriteCacheTests {
    protected static WriteCache writeCache;
    //CACHE CONFIGURATION PARAMETERS

    private static final int CACHE_SIZE = 1024;


    @RunWith(Parameterized.class)
    public static class putTest {

        //TEST PARAMETERS TO INVOKE put OPERATION
        private long ledgerId;

        private long entryId;
        private ByteBuf byteBuffer;
        private boolean expectedResult;
        private Class<? extends Exception>  expectedException;

        @Before
        public void setupCache() {
            //setup della cache prima di eseguire tutti i test
            writeCache = new WriteCache(ByteBufAllocator.DEFAULT, CACHE_SIZE);
        }

        @After
        public void clean() {
            //pulizia della cache dopo aver eseguito tutti i test
            writeCache.clear();
            writeCache.close();
        }

        @Parameterized.Parameters
        public static Collection<Object[][]> getParams() {


            return Arrays.asList(new Object[][]{
                    //LEDGER_ID         ENTRY_ID       ENTRY                                                EXPECTED_EXCEPTION                                      EXPECTED_RESULT
                    {-1L, 1L, Unpooled.wrappedBuffer(new byte[1]),                                          IllegalArgumentException.class,                       false},
                    {1L, 1L, Unpooled.wrappedBuffer(new byte[2 * CACHE_SIZE]),                              null,                                                   false},
                    {1L, -1L, Unpooled.wrappedBuffer(new byte[1]),                                          IllegalArgumentException.class,                       false},
                    {1L, 1L, Unpooled.wrappedBuffer(new byte[1]),                                           null,                                                   true},
                    {1L,1L,Unpooled.wrappedBuffer(new byte[CACHE_SIZE]),                                    null,                                                   true},
                    {1L, 0L, Unpooled.wrappedBuffer(new byte[1]),                                           null,                                                   true},
                    {0L, 1L, Unpooled.wrappedBuffer(new byte[1]),                                           null,                                                   true},
            });
        }

        public putTest(long ledgerId, long entryId, ByteBuf byteBuffer,  Class<? extends Exception> expectedExc, boolean expectedRes) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.byteBuffer = byteBuffer;
            this.expectedResult = expectedRes;
            this.expectedException = expectedExc;

        }

        @Test
        public void putTest() {

            try {
                Assert.assertEquals(this.expectedResult, writeCache.put(this.ledgerId, this.entryId, this.byteBuffer));
            } catch (IllegalArgumentException e) {
                Assert.assertEquals(expectedException,e.getClass() );
            }


        }


    }

    @RunWith(Parameterized.class)
    public static class getTest {
        long ledgerId;
        long entryId;
        private Class<? extends Exception>  expException;
        ByteBuf expectedResult;

        @BeforeClass
        public static void setupCache() {
            //della cache prima di eseguire tutti i test
            writeCache = new WriteCache(ByteBufAllocator.DEFAULT, CACHE_SIZE);
            writeCache.put(1, 1, Unpooled.wrappedBuffer(new byte[1]));

        }

        @AfterClass
        public static void clean() {
            //pulizia della cache dopo aver eseguito tutti i test
            writeCache.clear();
            writeCache.close();
        }

        @Parameterized.Parameters
        public static Collection<Object[][]> getParams() {


            return Arrays.asList(new Object[][]{

                    //LEDGER_ID   ENTRY_ID   EXPECTED_EXCEPTION   EXPECTED_RESULT
                    {-1L, 1L, IllegalArgumentException.class, null},
                    {1L, -1L, IllegalArgumentException.class,                                   null},
                    {1L, 1L, null, Unpooled.wrappedBuffer(new byte[1])},
                    {0L, 0L, null, null},
            });
        }

        public getTest(long ledgerId, long entry,Class<? extends Exception>  expExc, ByteBuf expRes) {
            this.ledgerId = ledgerId;
            this.entryId = entry;
            this.expectedResult = expRes;
            this.expException = expExc;
        }

        @Test
        public void getTest() {

            try {
                Assert.assertEquals(expectedResult, writeCache.get(this.ledgerId, this.entryId));
            } catch (IllegalArgumentException e) {
                Assert.assertEquals(this.expException,e.getClass());
            }
        }


    }

    @RunWith(Parameterized.class)
    public static class hasEntryTest {
        private Class<? extends Exception>  expectedException;
        long ledgerId;
        long entryId;
        boolean expectedResult;

        @BeforeClass
        public static void setupCache() {
            //setup della cache prima di eseguire tutti i test
            writeCache = new WriteCache(ByteBufAllocator.DEFAULT, CACHE_SIZE);
            writeCache.put(1, 1, Unpooled.wrappedBuffer(new byte[1]));

        }

        @AfterClass
        public static void clean() {
            //faccio la pulizia della cache dopo aver eseguito tutti i test
            writeCache.clear();
            writeCache.close();
        }

        @Parameterized.Parameters
        public static Collection<Object[][]> getParams() {

            return Arrays.asList(new Object[][]{
                    //LEDGER_ID  ENTRY_ID  EXPECTED_RESULT  EXPECTED_EXCEPTION
                    {-1L, 1L, false, IllegalArgumentException.class},
                    {1L, -1L, false, IllegalArgumentException.class},
                    {1L, 1L, true, null},
                    {0L, 0L, false, null}

            });
        }

        public hasEntryTest(long ledgerId, long entry, boolean expRes,  Class<? extends Exception>  expExc) {
            this.ledgerId = ledgerId;
            this.entryId = entry;
            this.expectedResult = expRes;
            this.expectedException=expExc;

        }

        @Test
        public void hasEntryTest() {
            try{

                Assert.assertEquals(expectedResult, writeCache.hasEntry(this.ledgerId, this.entryId));}
            catch(IllegalArgumentException e){
                Assert.assertEquals(this.expectedException, e.getClass());
            }
        }


    }

    @RunWith(Parameterized.class)
    public static class getLastEntryTest {
        long ledgerId;
        ByteBuf expectedResult;
        private Class<? extends Exception>  expectedException;

        @BeforeClass
        public static void setupCache() {
            //setup della cache prima di eseguire tutti i test
            writeCache = new WriteCache(ByteBufAllocator.DEFAULT, CACHE_SIZE);
            writeCache.put(1, 1, Unpooled.wrappedBuffer(new byte[1]));
        }

        @AfterClass
        public static void clean() {
            //pulizia della cache dopo aver eseguito tutti i test
            writeCache.clear();
            writeCache.close();
        }

        @Parameterized.Parameters
        public static Collection<Object[][]> getParams() {

            return Arrays.asList(new Object[][]{
                    //LEDGER_ID EXPECTED_RESULT    EXPECTED_EXCEPTION
                    {-1L, null, IllegalArgumentException.class},
                    {1L, Unpooled.wrappedBuffer(new byte[1]),null},
                    {0L, null, null}

            });
        }

        public getLastEntryTest(long ledgerId, ByteBuf expRes, Class<? extends Exception>  expExc) {
            this.ledgerId = ledgerId;
            this.expectedResult = expRes;
            this.expectedException=expExc;
        }

        @Test
        public void getLastEntryTest() {

            try {
                Assert.assertEquals(this.expectedResult, writeCache.getLastEntry(this.ledgerId));
            }catch(IllegalArgumentException e){
                Assert.assertEquals(this.expectedException,e.getClass());
            }
        }

    }

    @RunWith(Parameterized.class)
    public static class whiteboxPutTests{
        private  ByteBuf entry;
        private  boolean expRes;

        private long ledgerId;
        private long entryid;
        @Before
        public  void setupCache() {
            //setup della cache prima di eseguire tutti i test
            writeCache = new WriteCache(ByteBufAllocator.DEFAULT, CACHE_SIZE,512);

        }

        @After
        public  void clean() {
            //pulizia della cache dopo aver eseguito tutti i test
            writeCache.clear();
            writeCache.close();
        }

        @Parameterized.Parameters
        public static Collection<Object[][]> getParameters(){
            return Arrays.asList(new Object[][]{
                    //segment_size = 512
                    //LEDGER_ID  ENTRY_ID  ENTRY EXPECTED_RESULT
                    {0L, 0L, Unpooled.wrappedBuffer(new byte[1000]),false },
                    {0L, 1L, Unpooled.wrappedBuffer(new byte[512]), true},
                    {1L, 0L, Unpooled.wrappedBuffer(new byte[20]), true}
            });
        }

        //constructor
        public whiteboxPutTests( long ledgerId, long entryId, ByteBuf entry, boolean expectedRes){

            this.ledgerId=ledgerId;
            this.entryid=entryId;
            this.entry=entry;
            this.expRes=expectedRes;

        }
        @Test
        public void whiteboxPutTests(){

            if(expRes) {
                Assert.assertTrue(writeCache.put(this.ledgerId, this.entryid, this.entry));
            }else{
                Assert.assertFalse(writeCache.put(this.ledgerId, this.entryid, this.entry));
            }


        }
    }
    @RunWith(Parameterized.class)
    public static class whiteboxGetTests{
        long ledgerId;
        long entryId;
        private Class<? extends Exception>  expException;
        ByteBuf expectedResult;

        @BeforeClass
        public static void init(){
            writeCache=new WriteCache(ByteBufAllocator.DEFAULT,CACHE_SIZE, 512);
            writeCache.put(1L,1L, Unpooled.wrappedBuffer(new byte[1]));
        }
        @AfterClass
        public static void clean(){
            writeCache.clear();
            writeCache.close();
        }

        @Parameterized.Parameters
        public static Collection<Object[][]> getParams() {


            return Arrays.asList(new Object[][]{
                    //LEDGER_ID  ENTRY_ID  EXPECTED_EXCEPTION  EXPECTED_RESULT
                    {-1L,1L,IllegalArgumentException.class,null},
                    {1L,-1L,IllegalArgumentException.class,null},
                    {1L,1L,null,Unpooled.wrappedBuffer(new byte[1])},
                    {0L,0L,null,null},


            });
        }
        //constructor
        public whiteboxGetTests(long ledgerId, long entry,Class<? extends Exception>  expExc, ByteBuf expRes) {
            this.ledgerId = ledgerId;
            this.entryId = entry;
            this.expectedResult = expRes;
            this.expException = expExc;
        }
        @Test
        public void whiteboxGetTest() {

            try {
                Assert.assertEquals(expectedResult, writeCache.get(this.ledgerId, this.entryId));
            } catch (IllegalArgumentException e) {
                Assert.assertEquals(this.expException,e.getClass());
            }
        }
    }

}