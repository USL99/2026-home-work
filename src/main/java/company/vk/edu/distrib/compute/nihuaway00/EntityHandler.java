package company.vk.edu.distrib.compute.nihuaway00;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.NoSuchElementException;

public class EntityHandler implements HttpHandler {

    private final EntityDao dao;

    EntityHandler(EntityDao dao) {
        this.dao = dao;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        URI uri = exchange.getRequestURI();
        Map<String, String> params = QueryUtils.parse(uri.getQuery());

        switch (method) {
            case "GET" -> {
                handleGetEntity(exchange, params);
            }
            case "PUT" -> {
                handlePutEntity(exchange, params);
            }
            case "DELETE" -> {
                handleDeleteEntity(exchange, params);
            }
            default -> {
                exchange.close();
            }
        }

    }

    public void handleGetEntity(HttpExchange exchange, Map<String, String> params) throws IOException {
        String id = params.get("id");

        try (exchange) {
            byte[] data = dao.get(id);
            exchange.sendResponseHeaders(200, data.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(data);
            }
        } catch (NoSuchElementException err) {
            exchange.sendResponseHeaders(404, 0);
        } catch (IllegalArgumentException err) {
            exchange.sendResponseHeaders(400, 0);
        }
    }

    public void handlePutEntity(HttpExchange exchange, Map<String, String> params) throws IOException {
        String id = params.get("id");

        try (exchange; InputStream is = exchange.getRequestBody()) {
            var data = is.readAllBytes();
            try {
                dao.upsert(id, data);
                exchange.sendResponseHeaders(201, 0);
            } catch (IllegalArgumentException err) {
                exchange.sendResponseHeaders(400, 0);
            }
        }
    }

    public void handleDeleteEntity(HttpExchange exchange, Map<String, String> params) throws IOException {
        String id = params.get("id");

        try (exchange) {
            dao.delete(id);
            exchange.sendResponseHeaders(202, 0);
        } catch (IllegalArgumentException err) {
            exchange.sendResponseHeaders(400, 0);
        }
    }
}
