/*
 * Copyright 2016 Red Hat, Inc.
 * <p>
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package rediverson;

import org.springframework.stereotype.Component;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.camel.component.aws.s3.S3Constants;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;

/**
 * A simple Camel REST DSL route that implements the greetings service.
 * 
 */
@Component
public class Route extends RouteBuilder {

    @Override
    public void configure() throws Exception {

        // @formatter:off
        // errorHandler(loggingErrorHandler());
        errorHandler(deadLetterChannel("direct:errorLog"));
        
        from("direct:errorLog")
        .log(LoggingLevel.ERROR, simple("${exception.message}").getText())
        .log(LoggingLevel.ERROR, simple("${exception.stacktrace}").getText());

        from("kafka:my-topic?brokers=my-cluster-kafka-bootstrap.demo.svc.cluster.local:9092").routeId("S3Route")
        .log("Message received from Kafka : ${body}")
        // .log("    on the topic ${headers[kafka.TOPIC]}")
        // .log("    on the partition ${headers[kafka.PARTITION]}")
        // .log("    with the offset ${headers[kafka.OFFSET]}")
        // .log("    with the key ${headers[kafka.KEY]}")
        .setHeader(S3Constants.CONTENT_LENGTH, simple("${in.header.CamelFileLength}"))
        .setHeader(S3Constants.KEY, simple("entry-${date:now}.json"))
        .to("aws-s3://{{awsS3BucketName}}?accessKey={{awsAccessKey}}&secretKey=RAW({{awsAccessSecretKey}})&region={{awsRegion}}")
        .onException(RuntimeException.class).log("Exception");

        from("kafka:my-topic?brokers=my-cluster-kafka-bootstrap.demo.svc.cluster.local:9092").routeId("DDBRoute")
        .log("Message received from Kafka : ${body}")
        // .log("    on the topic ${headers[kafka.TOPIC]}")
        // .log("    on the partition ${headers[kafka.PARTITION]}")
        // .log("    with the offset ${headers[kafka.OFFSET]}")
        // .log("    with the key ${headers[kafka.KEY]}")
        .unmarshal()
        .json(JsonLibrary.Gson, Map.class)
        .process((Exchange exchange) -> {

            Map body = (Map) exchange.getIn().getBody();
            Map<String, AttributeValue> newBody = new HashMap();

            for(Object key : body.keySet()) {
                newBody.put(key.toString(), new AttributeValue(body.get(key).toString()));
            }            
                    
            exchange.getIn().setHeader("CamelAwsDdbItem", newBody);
        })                
        .to("aws-ddb:{{awsDynamoDBTame}}?operation=PutItem&accessKey={{awsAccessKey}}&secretKey=RAW({{awsAccessSecretKey}})&region={{awsRegion}}")
                .onException(RuntimeException.class).log("Exception");
                
    }        
}