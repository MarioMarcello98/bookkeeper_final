package org.apache.bookkeeper.bookie.storage.ldb;

import edu.emory.mathcs.backport.java.util.Arrays;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.Collection;

@RunWith(value = Enclosed.class)
public class WriteCacheTests {
    protected static WriteCache writeCache;
    //CACHE CONFIGURATION PARAMETERS
    private static final int ENTRY_SIZE = 1024;
    private static final int MAX_ENTRIES = 10;
    private static final int CACHE_SIZE = ENTRY_SIZE * MAX_ENTRIES;
    private static final int MAX_SEGMENT_SIZE = 1024 * 1024 * 1024;


    @RunWith(Parameterized.class)
    public static class putTest {

        //TEST PARAMETERS TO INVOKE put OPERATION
        private long ledgerId;

        private long entryId;
        private ByteBuf byteBuffer;
        private boolean expectedResult;
        private final boolean expectedException;

        @Before
        public void setupCache() {
            //faccio il setup della cache prima di eseguire tutti i test
            writeCache = new WriteCache(ByteBufAllocator.DEFAULT, CACHE_SIZE, MAX_SEGMENT_SIZE);
        }

        @After
        public void clean() {
            //faccio la pulizia della cache dopo aver eseguito tutti i test
            writeCache.clear();
            writeCache.close();
        }

        @Parameterized.Parameters
        public static Collection<Object[][]> getParams() {


            return Arrays.asList(new Object[][]{
                    //Unidimensional approach, I put in each test suite an invalid value
                    //LEDGER_ID         ENTRY_ID       ENTRY                                                EXPECTED_EXCEPTION          EXPECTED_RESULT
                    {-1L, 1L, Unpooled.wrappedBuffer(new byte[1]), true, false},
                    {1L, 1L, Unpooled.wrappedBuffer(new byte[2 * CACHE_SIZE]), false, false},
                    {1L, -1L, Unpooled.wrappedBuffer(new byte[1]), true, false},
                    {1L, 1L, Unpooled.wrappedBuffer(new byte[1]), false, true},

            });
        }

        public putTest(long ledgerId, long entryId, ByteBuf byteBuffer, boolean expectedExc, boolean expectedRes) {
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
                Assert.assertTrue(expectedException);
            }


        }


    }

    @RunWith(Parameterized.class)
    public static class getTest {
        long ledgerId;
        long entryId;
        boolean expException;
        ByteBuf expectedResult;

        @BeforeClass
        public static void setupCache() {
            //faccio il setup della cache prima di eseguire tutti i test
            writeCache = new WriteCache(ByteBufAllocator.DEFAULT, CACHE_SIZE, MAX_SEGMENT_SIZE);
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
                    //Unidimensional approach, I put in each test suite an invalid value
                    //LEDGER_ID         ENTRY_ID            EXPECTED_EXCEPTION          EXPECTED_RESULT
                    {-1L, 1L, true, null},
                    {1L, -1L, true, null},
                    {1L, 1L, false, Unpooled.wrappedBuffer(new byte[1])},

            });
        }

        public getTest(long ledgerId, long entry, boolean expExc, ByteBuf expRes) {
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
                Assert.assertTrue(this.expException);
            }
        }


    }

    @RunWith(Parameterized.class)
    public static class hasEntryTest {
        private final boolean expectedException;
        long ledgerId;
        long entryId;
        boolean expectedResult;

        @BeforeClass
        public static void setupCache() {
            //faccio il setup della cache prima di eseguire tutti i test
            writeCache = new WriteCache(ByteBufAllocator.DEFAULT, CACHE_SIZE, MAX_SEGMENT_SIZE);
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
                    //Unidimensional approach, I put in each test suite an invalid value
                    //LEDGER_ID         ENTRY_ID                   EXPECTED_RESULT      EXPECTED_EXCEPTION
                    {-1L,                1L,                       false,                true},
                    {1L,                -1L,                       false,                true},
                    {1L,                1L,                        true,                  false},
                    {0L,                1L,                        false,                   false}

            });
        }

        public hasEntryTest(long ledgerId, long entry, boolean expRes, boolean expExc) {
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
                Assert.assertTrue(this.expectedException);
            }
        }


    }

    @RunWith(Parameterized.class)
    public static class getLastEntryTest {
        long ledgerId;
        ByteBuf expectedResult;
        private boolean expectedException;

        @BeforeClass
        public static void setupCache() {
            //faccio il setup della cache prima di eseguire tutti i test
            writeCache = new WriteCache(ByteBufAllocator.DEFAULT, CACHE_SIZE, MAX_SEGMENT_SIZE);
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
                    //Unidimensional approach, I put in each test suite an invalid value
                    //LEDGER_ID             EXPECTED_RESULT                         EXPECTED_EXCEPTION
                    {-1L,                   null,                                    true},
                    {1L,                    Unpooled.wrappedBuffer(new byte[1])    ,false},
                    {0L,                   null,                                   false}

            });
        }

        public getLastEntryTest(long ledgerId, ByteBuf expRes, boolean expExc) {
            this.ledgerId = ledgerId;
            this.expectedResult = expRes;
            this.expectedException=expExc;
        }

        @Test
        public void getLastEntryTest() {

            try {
                Assert.assertEquals(this.expectedResult, writeCache.getLastEntry(this.ledgerId));
            }catch(IllegalArgumentException e){
                Assert.assertTrue(this.expectedException);
            }
        }

    }

}