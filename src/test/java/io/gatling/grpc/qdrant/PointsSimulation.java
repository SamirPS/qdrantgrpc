package io.gatling.grpc.qdrant;


import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.grpc.GrpcProtocolBuilder;
import io.grpc.Status;
import qdrant.*;

import java.util.*;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.grpc.GrpcDsl.*;

public class PointsSimulation extends Simulation {

    GrpcProtocolBuilder baseGrpcProtocol = grpc.forAddress("4e3ab98f-a64f-4fee-95b7-f2d436382fec.us-east4-0.gcp.cloud.qdrant.io", 6334)
            .asciiHeader("api-key").value("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIn0.4bH5DOPuE24H2LwfGMfIPAgL3xzj3zGQ7iFhftaltqs");


    ScenarioBuilder AddPoints = scenario("Add points")
            .exec(session -> {
                session = session.set("collectionName", "test0");
                session = session.set("sizeofcollection", 512);
                return session;
            }).asLongAs(session -> session.get("stop") == null || session.get("stop") == Status.Code.OK).on(
                    exec(
                            grpc("Add points")
                                    .unary(PointsGrpc.getUpsertMethod())
                                    .send(session -> {
                                        List<Points.PointStruct> pointsList = new ArrayList<>();
                                        Long vectorSize = session.getLong("sizeofcollection");
                                        List<String> cities = Arrays.asList(
                                                "Paris", "Lyon", "Marseille", "Bordeaux",
                                                "Toulouse", "Nantes", "Strasbourg", "Lille",
                                                "Nice", "Rennes", "Montpellier", "Grenoble"
                                        );
                                        for(int i = 0; i < 1000; i++) {
                                            List<Float> vectorValues = new ArrayList<>();
                                            for (int j = 0; j < vectorSize; j++) {
                                                vectorValues.add((float) Math.random());
                                            }

                                            Random random = new Random();
                                            String randomCity = cities.get(random.nextInt(cities.size()));
                                            Points.PointStruct point = Points.PointStruct.newBuilder()
                                                    .setId(Points.PointId.newBuilder()
                                                            .setUuid(UUID.randomUUID().toString())
                                                            .build())
                                                    .setVectors(Points.Vectors.newBuilder()
                                                            .setVector(Points.Vector.newBuilder()
                                                                    .addAllData(vectorValues)
                                                                    .build())
                                                            .build())
                                                    .putPayload("city", JsonWithInt.Value.newBuilder()
                                                            .setStringValue(randomCity).build())
                                                    .build();

                                            pointsList.add(point);
                                        }


                                        return Points.UpsertPoints.newBuilder()
                                                .setCollectionName(session.getString("collectionName"))
                                                .setWait(true)
                                                .addAllPoints(pointsList)
                                                .build();
                                    })
                                    .check(
                                            response(Points.PointsOperationResponse::getResult).transform(Points.UpdateResult::getStatus)
                                                    .is(Points.UpdateStatus.Completed),
                                            statusCode().saveAs("stop")
                                    )

                    )
            );



    {

        setUp(AddPoints.injectOpen(atOnceUsers(1))).protocols(baseGrpcProtocol);
    }
}
