package org.janusgraph.experience.buildin;

import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.graphdb.database.management.ManagementSystem;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * Use hbase as storage backend.
 */
@Slf4j
public class JanusGraphWriteMain {

    public static void main(String[] args) throws Exception {
        String confPath = Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                        .getResource("janusgraph-hbase.properties"))
                .getPath();
        JanusGraph graph = JanusGraphFactory.open(confPath);
        addCompositeIndex(graph);
        addVertexAndEdge(graph);
        traversalGraph(graph);
        removeAllVertices(graph);
        traversalGraph(graph);
        graph.close();
    }

    /**
     * Add and update composite index.
     */
    public static void addCompositeIndex(JanusGraph graph) throws InterruptedException, ExecutionException {
        log.info("================ Add composite index ================");
        JanusGraphManagement management = graph.openManagement();
        management.buildIndex("biz_id_idx", Vertex.class)
                .addKey(management.makePropertyKey("id").dataType(String.class).make())
                .unique()
                .buildCompositeIndex();
        management.commit();

        ManagementSystem.awaitGraphIndexStatus(graph, "biz_id_idx").call();

        management = graph.openManagement();
        management.updateIndex(management.getGraphIndex("biz_id_idx"), SchemaAction.REINDEX).get();
        management.commit();
    }

    /**
     * Add vertex and edge.
     */
    public static void addVertexAndEdge(JanusGraph graph) {
        log.info("================ Add vertex and edge ================");
        try (JanusGraphTransaction transaction = graph.newTransaction()) {
            JanusGraphVertex uuid = transaction.addVertex(T.label, "uuid", "id", "uuid_something");
            Vertex userId = transaction.addVertex(T.label, "user_id", "id", "user_id_nothing");
            uuid.addEdge("related", userId);
            transaction.commit();
        } catch (Exception e) {
            log.error("Add vertex/edge failed", e);
        }
    }

    /**
     * query.
     */
    public static void traversalGraph(JanusGraph graph) {
        log.info("================ traversal graph ================");
        GraphTraversal<Vertex, Vertex> vertices = graph
                .traversal()
                .V()
                .hasLabel("uuid");
        vertices.forEachRemaining(System.out::println);
    }

    /**
     * remove all vertices.
     */
    public static void removeAllVertices(JanusGraph graph) {
        log.info("================ remove all vertices ================");
        try (JanusGraphTransaction transaction = graph.newTransaction()) {
            graph.traversal().V().forEachRemaining(Element::remove);
            transaction.commit();
        } catch (Exception e) {
            log.error("Remove all vertices", e);
        }
    }
}
