package by.losik.repository;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import jakarta.inject.Inject;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.util.List;
import java.util.stream.Collectors;

@Tag(name = "Reactive Elasticsearch Repository", description = "Base repository for reactive CRUD operations with Elasticsearch")
public abstract class ReactiveElasticsearchRepository<T> {
    @Inject
    RestClient restClient;
    protected abstract String getIndexName();
    protected abstract String getId(T entity);
    protected abstract Class<T> getEntityType();

    @Operation(
            summary = "Index a single document",
            description = "Creates or updates a document in Elasticsearch"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Document successfully indexed",
                    content = @Content(schema = @Schema(implementation = Object.class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or indexing failed"
            )
    })
    @RequestBody(
            description = "Elasticsearch query in JSON format",
            content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = Object.class)
            )
    )
    public Uni<T> index(T entity) {
        return Uni.createFrom().emitter(emitter -> {
            try {
                Request request = new Request(
                        "PUT",
                        "/" + getIndexName() + "/_doc/" + getId(entity));
                request.setEntity(new StringEntity(
                        JsonObject.mapFrom(entity).encode(),
                        ContentType.APPLICATION_JSON));

                Response response = restClient.performRequest(request);
                if (response.getStatusLine().getStatusCode() >= 400) {
                    emitter.fail(new RuntimeException("Index failed: " +
                            EntityUtils.toString(response.getEntity())));
                } else {
                    emitter.complete(entity);
                }
            } catch (Exception e) {
                emitter.fail(e);
            }
        });
    }

    @Operation(
            summary = "Bulk index multiple documents",
            description = "Creates or updates multiple documents in Elasticsearch in a single request"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Documents successfully indexed",
                    content = @Content(schema = @Schema(implementation = List.class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or bulk indexing failed"
            )
    })
    @RequestBody(
            description = "Elasticsearch query in JSON format",
            content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = Object.class)
            )
    )
    public Uni<List<T>> bulkIndex(List<T> entities) {
        return Uni.createFrom().emitter(emitter -> {
            try {
                String bulkRequest = entities.parallelStream()
                        .map(entity -> {
                            JsonObject action = new JsonObject()
                                    .put("index", new JsonObject()
                                            .put("_index", getIndexName())
                                            .put("_id", getId(entity)));
                            return action.encode() + "\n" +
                                    JsonObject.mapFrom(entity).encode() + "\n";
                        })
                        .collect(Collectors.joining());

                Request request = new Request("POST", "/_bulk");
                request.setEntity(new StringEntity(
                        bulkRequest,
                        ContentType.create("application/x-ndjson")));

                Response response = restClient.performRequest(request);
                if (response.getStatusLine().getStatusCode() >= 400) {
                    emitter.fail(new RuntimeException("Bulk index failed: " +
                            EntityUtils.toString(response.getEntity())));
                } else {
                    emitter.complete(entities);
                }
            } catch (Exception e) {
                emitter.fail(e);
            }
        });
    }

    @Operation(
            summary = "Get document by ID",
            description = "Retrieves a single document from Elasticsearch by its ID"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Document found and returned",
                    content = @Content(schema = @Schema(implementation = Object.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "Document not found"
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or retrieval failed"
            )
    })
    @RequestBody(
            description = "Elasticsearch query in JSON format",
            content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = Object.class)
            )
    )
    public Uni<T> getById(String id) {
        return Uni.createFrom().emitter(emitter -> {
            try {
                Request request = new Request(
                        "GET",
                        "/" + getIndexName() + "/_doc/" + id);

                Response response = restClient.performRequest(request);
                if (response.getStatusLine().getStatusCode() == 404) {
                    emitter.fail(new RuntimeException("Document not found"));
                } else if (response.getStatusLine().getStatusCode() >= 400) {
                    emitter.fail(new RuntimeException("Get failed: " +
                            EntityUtils.toString(response.getEntity())));
                } else {
                    JsonObject json = new JsonObject(
                            EntityUtils.toString(response.getEntity()));
                    emitter.complete(json.getJsonObject("_source").mapTo(getEntityType()));
                }
            } catch (Exception e) {
                emitter.fail(e);
            }
        });
    }

    @Operation(
            summary = "Search documents",
            description = "Performs a search query against Elasticsearch and returns matching documents"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = List.class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    @RequestBody(
            description = "Elasticsearch query in JSON format",
            content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = Object.class)
            )
    )
    public Uni<List<T>> search(JsonObject query) {
        return Uni.createFrom().emitter(emitter -> {
            try {
                Request request = new Request("GET", "/" + getIndexName() + "/_search");
                request.setEntity(new StringEntity(
                        query.encode(),
                        ContentType.APPLICATION_JSON));

                Response response = restClient.performRequest(request);
                if (response.getStatusLine().getStatusCode() >= 400) {
                    emitter.fail(new RuntimeException("Search failed: " +
                            EntityUtils.toString(response.getEntity())));
                } else {
                    JsonObject json = new JsonObject(
                            EntityUtils.toString(response.getEntity()));
                    List<T> results = json.getJsonObject("hits")
                            .getJsonArray("hits").stream()
                            .map(o -> ((JsonObject)o).getJsonObject("_source").mapTo(getEntityType()))
                            .collect(Collectors.toList());
                    emitter.complete(results);
                }
            } catch (Exception e) {
                emitter.fail(e);
            }
        });
    }

    @Operation(
            summary = "Delete document by ID",
            description = "Removes a document from Elasticsearch by its ID"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Document successfully deleted",
                    content = @Content(schema = @Schema(implementation = Boolean.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "Document not found"
            )
    })
    @RequestBody(
            description = "Elasticsearch query in JSON format",
            content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = Object.class)
            )
    )
    public Uni<Boolean> deleteById(String id) {
        return Uni.createFrom().emitter(emitter -> {
            try {
                Request request = new Request(
                        "DELETE",
                        "/" + getIndexName() + "/_doc/" + id);

                Response response = restClient.performRequest(request);
                JsonObject responseBody = new JsonObject(EntityUtils.toString(response.getEntity()));

                if (response.getStatusLine().getStatusCode() == 404) {
                    emitter.complete(false);
                } else if (response.getStatusLine().getStatusCode() >= 400) {
                    emitter.fail(new RuntimeException("Delete failed: " +
                            responseBody.getString("reason", "Unknown error")));
                } else {
                    boolean deleted = "deleted".equals(responseBody.getString("result"));
                    emitter.complete(deleted);
                }
            } catch (Exception e) {
                emitter.fail(e);
            }
        });
    }
}