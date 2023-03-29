package org.janusgraph.experience.remote;

import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Objects;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

/**
 * Hello world!
 */
@Slf4j
public class JanusGraphMain {
    public static void main(String[] args) throws Exception {
        String confPath = Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                        .getResource("remote-graph.properties"))
                .getPath();
        try (GraphTraversalSource g = traversal().withRemote(confPath)) {
            // addVertex(g);
            traversalGraph(g);
        }
    }

    /**
     * Remote graph traversal behavior have some difference from JanusGraph traversal.
     */
    public static void traversalGraph(GraphTraversalSource g) {
        log.info("================ traversal graph ================");
        g.V().hasLabel("user_id")
                .in("related")
                .properties()
                .forEachRemaining(property -> System.out.println(property.key() + ", " + property.value()));
    }

    public static void addVertex(GraphTraversalSource g) {
        log.info("================ Add vertex and edge ================");
        Vertex v1 = g.addV("uuid").property("ident", "uuid_marko").next();
        Vertex v2 = g.addV("user_id").property("ident", "user_id_stephen").next();
        g.V(v1).addE("related").to(v2).property("create_at", "da_nana").iterate();
    }
}
