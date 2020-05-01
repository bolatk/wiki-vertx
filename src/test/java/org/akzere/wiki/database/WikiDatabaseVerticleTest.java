package org.akzere.wiki.database;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opentest4j.AssertionFailedError;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
public class WikiDatabaseVerticleTest {

    private WikiDatabaseService service;

    @BeforeEach
    void prepare(Vertx vertx, VertxTestContext testContext) {
        JsonObject conf = new JsonObject()
            .put(WikiDatabaseVerticle.CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:mem:testdb;shutdown=true")
            .put(WikiDatabaseVerticle.CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 4);

        vertx.deployVerticle(new WikiDatabaseVerticle(), new DeploymentOptions().setConfig(conf),
            testContext.succeeding(id -> {
                service = WikiDatabaseService.createProxy(vertx, WikiDatabaseVerticle.CONFIG_WIKIDB_QUEUE);
                testContext.completeNow();
            }));
    }

    @AfterEach
    void finish(Vertx vertx) {
        vertx.close();
    }

    @Test
//    @Timeout(2000)
    /*
     * There are two options to complete test context when assertion fails:
     * 1. Catch AssertionFailedError and do testContext.failNow(e);
     * 2. Set proper test timeout using JUnit5 Annotation
     */
    public void crud_operations(VertxTestContext testContext) {

        Checkpoint pageCreated = testContext.checkpoint();
        Checkpoint pageFetched = testContext.checkpoint();
        Checkpoint pageSaved = testContext.checkpoint();
        Checkpoint allPagesFetched = testContext.checkpoint(2);
        Checkpoint newPageFetched = testContext.checkpoint();
        Checkpoint pageDeleted = testContext.checkpoint();


        service.createPage("Test", "Some content", testContext.succeeding(v1 -> {

            pageCreated.flag();

            service.fetchPage("Test", testContext.succeeding(json1 -> testContext.verify(()-> {
                try {
                    assertThat(json1.getBoolean("found"));
                    assertThat(json1.containsKey("id"));
                    assertThat("Some content").isEqualTo(json1.getString("rawContent"));

                    pageFetched.flag();
                } catch (AssertionFailedError e) {
                    testContext.failNow(e);
                }

                service.savePage(json1.getInteger("id"), "Yo!", testContext.succeeding(v2 -> {

                    pageSaved.flag();

                    service.fetchAllPages(testContext.succeeding(array1 -> {

                        assertThat(1).isEqualTo(array1.size());
                        allPagesFetched.flag();

                        service.fetchPage("Test", testContext.succeeding(json2 -> {
                            try {
                                assertThat("Yo!").isEqualTo(json2.getString("rawContent"));
                                newPageFetched.flag();
                            } catch (AssertionFailedError e) {
                                testContext.failNow(e);
                            }

                            service.deletePage(json1.getInteger("id"), v3 -> {
                                pageDeleted.flag();

                                service.fetchAllPages(testContext.succeeding(array2 -> {
                                    try {
                                        assertThat(array2.isEmpty());
                                        allPagesFetched.flag();
                                    } catch (AssertionFailedError e) {
                                        testContext.failNow(e);
                                    }
                                }));
                            });
                        }));
                    }));
                }));
            })));
        }));
    }

}
