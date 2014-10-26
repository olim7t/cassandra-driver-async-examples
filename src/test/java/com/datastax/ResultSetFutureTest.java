package com.datastax;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;

public class ResultSetFutureTest extends TestBase {

    private static final Logger logger = LoggerFactory.getLogger(ResultSetFutureTest.class);

    /**
     * Naive example to highlight that the caller thread is not blocked while an async executes.
     * (not an actual test, it doesn't even have assertions!)
     */
    @Test
    public void should_complete_asynchronously() throws ExecutionException, InterruptedException {
        ResultSetFuture future = session.executeAsync("SELECT release_version FROM system.local");

        // For demonstration purpose only (don't do this in your code)
        while (!future.isDone()) {
            logger.debug("Waiting for request to complete");
        }

        ResultSet rs = future.get();
        logger.debug("Got response: {}", rs.one().getString("release_version"));
    }

    @Test
    public void should_add_a_callback() {
        MockGui gui = new MockGui();

        ResultSetFuture future = session.executeAsync("SELECT release_version FROM system.local");
        Futures.addCallback(future,
            new FutureCallback<ResultSet>() {
                @Override public void onSuccess(ResultSet result) {
                    gui.setMessage("Cassandra version is " + result.one().getString("release_version"));
                }

                @Override public void onFailure(Throwable t) {
                    gui.setMessage("Error while reading Cassandra version: " + t.getMessage());
                }
            },
            MoreExecutors.sameThreadExecutor()
        );

        gui.waitForRefresh();
        assertThat(gui.message).matches("Cassandra version is .*");
    }

    /** Simulate a graphical user interface that would get updated asynchronously */
    static class MockGui {
        volatile String message;
        private CountDownLatch latch = new CountDownLatch(1);

        void setMessage(String message) {
            this.message = message;
            latch.countDown();
        }

        void waitForRefresh() {
            try {
                boolean refreshed = latch.await(100, MILLISECONDS);
                assertThat(refreshed).isTrue()
                    .as("Expected refresh in less than 100 milliseconds");
            } catch (InterruptedException e) {
                fail("Interrupted while waiting for refresh");
            }
        }
    }
}
