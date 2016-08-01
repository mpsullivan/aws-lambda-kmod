/*
 * Copyright 2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ws.salient.aws.lambda;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.invoke.LambdaFunctionException;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.inject.Inject;
import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemHandler;
import org.kie.api.runtime.process.WorkItemManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FunctionHandler implements WorkItemHandler {

    private static final Logger log = LoggerFactory.getLogger(FunctionHandler.class);

    private final AWSLambda lambda;
    private ObjectMapper json;

    @Inject
    public FunctionHandler(AWSLambda lambda, ObjectMapper json) {
        this.lambda = lambda;
        this.json = json;
    }

    @Override
    public void executeWorkItem(WorkItem workItem, WorkItemManager manager) {
        try {
            String functionName = (String) workItem.getParameter("functionName");
            String qualifier = (String) workItem.getParameter("qualifier");
            JsonNode input = json.convertValue(workItem.getParameters(), JsonNode.class);
            InvokeRequest invokeRequest = new InvokeRequest().withFunctionName(functionName).withQualifier(qualifier).withPayload(input.toString());
            InvokeResult invoke = lambda.invoke(invokeRequest);
            String functionError = invoke.getFunctionError();
            if (functionError != null) {
                ObjectNode result = (ObjectNode) json.readTree(invoke.getPayload().array());
                String errorMessage = (result.has("errorMessage") ? result.get("errorMessage").textValue() : "");
                String errorType = (result.has("errorType") ? result.get("errorType").textValue() : null);
                throw new LambdaFunctionException(errorType, true, errorMessage);
            }
            byte[] payload = invoke.getPayload().array();
            Map result = new LinkedHashMap();
            if (payload.length > 0) {
                try {
                    result = json.readValue(invoke.getPayload().array(), Map.class);
                } catch (IOException ex) {
                }
            }
            manager.completeWorkItem(workItem.getId(), result);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void abortWorkItem(WorkItem workItem, WorkItemManager manager) {

    }

}
