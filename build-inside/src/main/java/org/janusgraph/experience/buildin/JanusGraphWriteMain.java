package org.janusgraph.experience.buildin;

import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.graphdb.database.management.ManagementSystem;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

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
        addVertexAndEdge(graph);
        addCompositeIndex(graph);
        addEdgeIndex(graph);
        addPropertyIndex(graph);
        traversalGraph(graph);
        removeAllVertices(graph);
        dropVertexCompositeIndex(graph);
        graph.close();
    }

    /**
     * Add vertex and edge.
     */
    public static void addVertexAndEdge(JanusGraph graph) {
        log.info("================ Add vertex and edge ================");
        try (JanusGraphTransaction transaction = graph.newTransaction()) {
            JanusGraphVertex uuid = transaction.addVertex(
                    T.label,
                    "uuid",
                    "ident", "uuid_ident",
                    "create_at", "uuid_create_time"
            );
            Vertex userId = transaction.addVertex(
                    T.label,
                    "user_id",
                    "ident", "user_id_ident",
                    "create_at", "user_id_create_time");
            uuid.addEdge("related", userId, "create_at", "just_now");
            transaction.commit();
        } catch (Exception e) {
            log.error("Add vertex/edge failed", e);
        }
    }

    /**
     * Add and update composite index.
     */
    public static void addCompositeIndex(JanusGraph graph) throws InterruptedException, ExecutionException {
        log.info("================ Add composite index ================");
        JanusGraphManagement management = graph.openManagement();
        PropertyKey identProp = management.getPropertyKey("ident");
        if (identProp == null) {
            identProp = management.makePropertyKey("ident").dataType(String.class).make();
        }
        management.buildIndex("vertexByIdent", Vertex.class)
                .addKey(identProp)
                .unique()
                .buildCompositeIndex();
        management.commit();

        ManagementSystem.awaitGraphIndexStatus(graph, "vertexByIdent").call();

        management = graph.openManagement();
        management
                .updateIndex(management.getGraphIndex("vertexByIdent"), SchemaAction.REINDEX)
                .get();
        management.commit();
    }

    public static void addEdgeIndex(JanusGraph graph) throws InterruptedException, ExecutionException {
        log.info("================ Add index of edge ================");
        JanusGraphManagement management = graph.openManagement();
        EdgeLabel related = management.getEdgeLabel("related");
        if (related == null) {
            related = management.makeEdgeLabel("related").make();
        }
        PropertyKey createAt = management.getPropertyKey("create_at");
        if (createAt == null) {
            createAt = management
                    .makePropertyKey("create_at")
                    .dataType(String.class)
                    .make();
        }
        management.buildEdgeIndex(related, "relatedByCreateAt", Direction.BOTH, createAt);
        management.commit();

        ManagementSystem.awaitRelationIndexStatus(graph, "relatedByCreateAt", "related").call();

        management = graph.openManagement();
        management
                .updateIndex(
                        management.getRelationIndex(related, "relatedByCreateAt"),
                        SchemaAction.REINDEX)
                .get();
        management.commit();
    }

    public static void addPropertyIndex(JanusGraph graph) throws InterruptedException, ExecutionException {
        log.info("================ Add index of property ================");
        JanusGraphManagement management = graph.openManagement();
        PropertyKey refer = management.getPropertyKey("refer");
        if (refer == null) {
            refer = management.makePropertyKey("refer").dataType(String.class)
                    .cardinality(Cardinality.SET).make();
        }
        PropertyKey createAt = management.getPropertyKey("create_at");
        if (createAt == null) {
            createAt = management.makePropertyKey("create_at").dataType(String.class).make();
        }
        management.buildPropertyIndex(refer, "referByCreateAt", createAt);
        management.commit();

        ManagementSystem.awaitRelationIndexStatus(graph, "referByCreateAt", "refer").call();

        management = graph.openManagement();
        management
                .updateIndex(
                        management.getRelationIndex(refer, "referByCreateAt"),
                        SchemaAction.REINDEX)
                .get();
        management.commit();

        JanusGraphTransaction transaction = graph.newTransaction();
        transaction.traversal().V().forEachRemaining(vertex -> {
            vertex.property("refer", "v-" + ThreadLocalRandom.current().nextInt());
        });
        transaction.traversal().E().forEachRemaining(edge -> {
            edge.properties("refer", "e-" + ThreadLocalRandom.current().nextInt());
        });
        transaction.commit();
    }

    public static void dropVertexCompositeIndex(JanusGraph graph) throws ExecutionException, InterruptedException {
        log.info("================ drop composite index ================");
        JanusGraphManagement management = graph.openManagement();
        management
                .updateIndex(
                        management.getGraphIndex("vertexByIdent"),
                        SchemaAction.DISABLE_INDEX)
                .get();
        ManagementSystem.awaitGraphIndexStatus(graph, "vertexByIdent").call();
        management.commit();
        graph.tx().commit();

        management = graph.openManagement();
        JanusGraphManagement.IndexJobFuture future = management.updateIndex(management.getGraphIndex(
                "vertexByIdent"), SchemaAction.REMOVE_INDEX);
        management.commit();
        graph.tx().commit();
        future.get();
    }

    /**
     * query.
     */
    public static void traversalGraph(JanusGraph graph) {
        log.info("================ traversal graph ================");
        GraphTraversal<Vertex, Vertex> vertices = graph
                .traversal()
                .V()
                .hasLabel("user_id")
                .in("related");
        vertices.forEachRemaining(vertex -> System.out.println(vertex.value("ident").toString()));
    }

    /**
     * remove all vertices.
     */
    public static void removeAllVertices(JanusGraph graph) {
        log.info("================ remove all vertices ================");
        try (JanusGraphTransaction transaction = graph.newTransaction()) {
            transaction.traversal().V().forEachRemaining(Element::remove);
            transaction.commit();
        } catch (Exception e) {
            log.error("Remove all vertices", e);
        }
    }
}
