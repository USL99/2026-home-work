package company.vk.edu.distrib.compute.usl;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

final class EntityHttpHandler implements HttpHandler {
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String ID_PARAMETER = "id";
    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";
    private static final String QUERY_PARAMETER_SEPARATOR = "&";
    private static final byte[] EMPTY_BODY = new byte[0];

    private final Dao<byte[]> dao;

    EntityHttpHandler(Dao<byte[]> dao) {
        this.dao = dao;
    }

    @Override
    @SuppressWarnings("PMD.UseTryWithResources")
    public void handle(HttpExchange exchange) throws IOException {
        try {
            if (!ENTITY_PATH.equals(exchange.getRequestURI().getPath())) {
                ExchangeResponses.sendEmpty(exchange, 404);
                return;
            }

            String key = extractId(exchange.getRequestURI());
            byte[] requestBody = readBody(exchange);
            handleLocal(exchange, key, requestBody);
        } catch (IllegalArgumentException e) {
            ExchangeResponses.sendEmpty(exchange, 400);
        } catch (NoSuchElementException e) {
            ExchangeResponses.sendEmpty(exchange, 404);
        } catch (IOException e) {
            ExchangeResponses.sendEmpty(exchange, 503);
        } catch (Exception e) {
            ExchangeResponses.sendEmpty(exchange, 500);
        } finally {
            exchange.close();
        }
    }

    private void handleLocal(HttpExchange exchange, String key, byte[] requestBody) throws IOException {
        switch (exchange.getRequestMethod()) {
            case GET_METHOD -> ExchangeResponses.sendBody(exchange, 200, dao.get(key));
            case PUT_METHOD -> handlePut(exchange, key, requestBody);
            case DELETE_METHOD -> handleDelete(exchange, key);
            default -> ExchangeResponses.sendEmpty(exchange, 405);
        }
    }

    private void handlePut(HttpExchange exchange, String key, byte[] requestBody) throws IOException {
        dao.upsert(key, requestBody);
        ExchangeResponses.sendEmpty(exchange, 201);
    }

    private void handleDelete(HttpExchange exchange, String key) throws IOException {
        dao.delete(key);
        ExchangeResponses.sendEmpty(exchange, 202);
    }

    private static byte[] readBody(HttpExchange exchange) throws IOException {
        return switch (exchange.getRequestMethod()) {
            case PUT_METHOD -> exchange.getRequestBody().readAllBytes();
            case GET_METHOD, DELETE_METHOD -> EMPTY_BODY;
            default -> EMPTY_BODY;
        };
    }

    private static String extractId(URI requestUri) {
        String query = requestUri.getRawQuery();
        if (query == null || query.isEmpty()) {
            throw new IllegalArgumentException("Missing id query parameter");
        }

        String id = null;
        for (String parameter : query.split(QUERY_PARAMETER_SEPARATOR)) {
            id = extractIdValue(parameter, id);
        }

        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Empty id query parameter");
        }

        return id;
    }

    private static String extractIdValue(String parameter, String currentId) {
        int delimiterIndex = parameter.indexOf('=');
        String rawName = delimiterIndex < 0 ? parameter : parameter.substring(0, delimiterIndex);
        if (!ID_PARAMETER.equals(URLDecoder.decode(rawName, StandardCharsets.UTF_8))) {
            return currentId;
        }

        if (currentId != null) {
            throw new IllegalArgumentException("Duplicate id query parameter");
        }

        String rawValue = delimiterIndex < 0 ? "" : parameter.substring(delimiterIndex + 1);
        return URLDecoder.decode(rawValue, StandardCharsets.UTF_8);
    }
}
