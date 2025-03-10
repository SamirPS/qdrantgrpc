package io.gatling.grpc.qdrant;


import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.grpc.GrpcProtocolBuilder;
import io.grpc.Status;
import qdrant.JsonWithInt;
import qdrant.Points;
import qdrant.PointsGrpc;

import java.util.*;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.grpc.GrpcDsl.*;

public class QuerySimulation extends Simulation {

    GrpcProtocolBuilder baseGrpcProtocol = grpc.forAddress("4e3ab98f-a64f-4fee-95b7-f2d436382fec.us-east4-0.gcp.cloud.qdrant.io", 6334)
            .asciiHeader("api-key").value("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIn0.4bH5DOPuE24H2LwfGMfIPAgL3xzj3zGQ7iFhftaltqs");

    ScenarioBuilder Query = scenario("Query")
            .exec(session -> {
                session = session.set("collectionName", "test0");
                return session;
            }).exec(
                    grpc("Query points without filter")
                            .unary(PointsGrpc.getQueryMethod())
                            .send(session -> Points.QueryPoints.newBuilder()
                                    .setCollectionName(session.getString("collectionName"))
                                    .setLimit(100)
                                    .build())
                            .check(
                                    response(Points.QueryResponse::getResultList)
                                            .transform(res -> res.size() == 100)
                                            .is(true)
                            )

            ).exec(
                    grpc("Query Points with filter ")
                            .unary(PointsGrpc.getScrollMethod())
                            .send(
                                    session -> Points.ScrollPoints.newBuilder()
                                            .setCollectionName(session.getString("collectionName"))
                                            .setLimit(100)
                                            .setFilter(Points.Filter.newBuilder()
                                                    .addMust(Points.Condition.newBuilder()
                                                            .setField(Points.FieldCondition.newBuilder()
                                                                    .setKey("city")
                                                                    .setMatch(Points.Match.newBuilder()
                                                                            .setKeyword("Paris")
                                                                            .build())
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build()
                            )
                            .check(
                                    response(Points.ScrollResponse::getResultList)
                                            .transform(resultList -> resultList.stream()
                                                    .allMatch(point -> {
                                                        Map<String, qdrant.JsonWithInt.Value> payload = point.getPayloadMap();
                                                        qdrant.JsonWithInt.Value cityValue = payload.get("city");
                                                        return cityValue != null && cityValue.hasStringValue() && cityValue.getStringValue().equals("Paris");
                                                    }))
                                            .is(true)
                            )
            );




    {

        setUp(Query.injectOpen(atOnceUsers(1))).protocols(baseGrpcProtocol);
    }
}
