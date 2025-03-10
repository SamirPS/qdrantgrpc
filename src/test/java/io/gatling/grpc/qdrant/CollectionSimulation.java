package io.gatling.grpc.qdrant;


import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.grpc.GrpcProtocolBuilder;
import io.grpc.Status;
import qdrant.*;
import qdrant.Collections;


import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.grpc.GrpcDsl.*;

public class CollectionSimulation extends Simulation {

    GrpcProtocolBuilder baseGrpcProtocol = grpc.forAddress("4e3ab98f-a64f-4fee-95b7-f2d436382fec.us-east4-0.gcp.cloud.qdrant.io", 6334)
            .asciiHeader("api-key").value("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIn0.4bH5DOPuE24H2LwfGMfIPAgL3xzj3zGQ7iFhftaltqs");

    ScenarioBuilder CollectionsOpti = scenario("LoadTest Collections")
            .exec( session -> {
                        session = session.set("compteur", 0);
                        return session;
                    }
            ).asLongAs(session -> session.get("stop") == null || session.get("stop") == Status.Code.OK).on(
                    exec(
                            grpc("Create a collection")
                                    .unary(CollectionsGrpc.getCreateMethod())
                                    .send(session -> Collections.CreateCollection.newBuilder()
                                            .setCollectionName("test" + session.getInt("compteur"))
                                            .setOnDiskPayload(true)
                                            .setOptimizersConfig(
                                                    Collections.OptimizersConfigDiff.newBuilder()
                                                            .setIndexingThreshold(0).build()
                                            )
                                            .setQuantizationConfig(
                                                    Collections.QuantizationConfig.newBuilder()
                                                            .setBinary(Collections.BinaryQuantization.newBuilder()
                                                                    .setAlwaysRam(true)
                                                                    .build())
                                                            .build()
                                            )
                                            .setVectorsConfig(Collections.VectorsConfig.newBuilder().
                                                    setParams(Collections.VectorParams.newBuilder()
                                                            .setSize(512)
                                                            .setDistance(Collections.Distance.Cosine)
                                                            .build())
                                                    .build())
                                            .build())
                                    .check(
                                            response(Collections.CollectionOperationResponse::getResult).is(true),
                                            statusCode().saveAs("stop")
                                    ))
                            .exec(
                                    session -> {
                                        session = session.set("compteur", session.getInt("compteur") + 1);
                                        return session;
                                    }
                            )
            );

    ScenarioBuilder CollectionsNotOpti = scenario("LoadTest Collections")
            .exec( session -> {
                        session = session.set("compteur", 0);
                        return session;
                    }
            ).asLongAs(session -> session.get("stop") == null || session.get("stop") == Status.Code.OK).on(
                    exec(
                            grpc("Create a collection")
                                    .unary(CollectionsGrpc.getCreateMethod())
                                    .send(session -> Collections.CreateCollection.newBuilder()
                                            .setCollectionName("test" + session.getInt("compteur"))
                                            .setVectorsConfig(Collections.VectorsConfig.newBuilder().
                                                    setParams(Collections.VectorParams.newBuilder()
                                                            .setSize(512)
                                                            .setDistance(Collections.Distance.Cosine)
                                                            .build())
                                                    .build())
                                            .build())
                                    .check(
                                            response(Collections.CollectionOperationResponse::getResult).is(true),
                                            statusCode().saveAs("stop")
                                    ))
                            .exec(
                                    session -> {
                                        session = session.set("compteur", session.getInt("compteur") + 1);
                                        return session;
                                    }
                            )
            );



    {
        String name = System.getProperty("scenario");
        ScenarioBuilder scn;
        if (name == null || name.equals("opti")) {
            scn = CollectionsOpti;
        } else {

            scn = CollectionsNotOpti;

        }

        setUp(scn.injectOpen(atOnceUsers(1))).protocols(baseGrpcProtocol);
    }
}
