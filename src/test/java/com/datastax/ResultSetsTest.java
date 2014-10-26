package com.datastax;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.Test;
import rx.Observable;
import rx.Observer;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.ResultSet;

public class ResultSetsTest extends TestBase {
    Object[] uuids = {
        UUID.fromString("e6af74a8-4711-4609-a94f-2cbfab9695e5"),
        UUID.fromString("281336f4-2a52-4535-847c-11a4d3682ec1"),
        UUID.fromString("c32b8d37-89bd-4dfe-a7d5-5f0258692d05"),
        UUID.fromString("973fe99f-5715-4dfd-a28d-5b3751b26ab5"),
        UUID.fromString("0aabb840-bab6-474b-9f08-c18527a2b47f") };

    @Test
    public void should_query_partitions_as_list() throws Exception {
        Future<List<ResultSet>> future = ResultSets.queryAllAsList(session,
            "SELECT * FROM users WHERE id = ?",
            uuids);

        for (ResultSet rs : future.get()) {

        }

        List<String> names = Lists.transform(future.get(100, MILLISECONDS),
            (ResultSet rs) -> rs.one().getString("name"));

        assertThat(names).containsOnly("user1", "user2", "user3", "user4", "user5");
    }

    @Test
    public void should_query_partitions_with_futures_in_order() throws Exception {
        List<ListenableFuture<ResultSet>> futures = ResultSets.queryAll(session,
            "SELECT * FROM users WHERE id = ?",
            uuids);

        List<String> names = Lists.newArrayList();
        for (ListenableFuture<ResultSet> future : futures) {
            ResultSet rs = future.get(100, MILLISECONDS);
            names.add(rs.one().getString("name"));
        }

        assertThat(names).containsOnly("user1", "user2", "user3", "user4", "user5");
    }

    /**
     * To test the observable, we register an observer that collects the emitted values, and block
     * the main thread until it has finished.
     * Of course this is not how we use an observable in real life, the values would be consumed as
     * they are emitted in a reactive manner.
     */
    @Test
    public void should_query_partitions_as_observable() throws Exception {
        Observable<ResultSet> results = ResultSets.queryAllAsObservable(session,
            "SELECT * FROM users WHERE id = ?",
            uuids);

        Observable<String> names = results.map((ResultSet rs) -> rs.one().getString("name"));

        CountDownLatch latch = new CountDownLatch(1);
        CollectingObserver<String> collector = new CollectingObserver<>(latch);
        names.subscribe(collector);

        boolean completed = latch.await(100, MILLISECONDS);
        assertThat(completed).isTrue()
            .as("Expected observable to complete in less than 100 milliseconds");
        assertThat(collector.values).containsOnly("user1", "user2", "user3", "user4", "user5");

    }

    class CollectingObserver<T> implements Observer<T> {

        private final CountDownLatch latch;
        private final List<T> values = Lists.newCopyOnWriteArrayList();
        private final List<Throwable> errors = Lists.newCopyOnWriteArrayList();

        CollectingObserver(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override public void onNext(T t) {
            values.add(t);
        }

        @Override public void onError(Throwable throwable) {
            errors.add(throwable);
        }

        @Override public void onCompleted() {
            latch.countDown();
        }
    }
}