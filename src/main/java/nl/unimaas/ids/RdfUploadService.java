package nl.unimaas.ids;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.exec.environment.EnvironmentUtils;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.repository.util.RDFInserter;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.impl.VertxFactoryImpl;

public class RdfUploadService {

	static final String ENDPOINT_KEY = "ENDPOINT";
	static final String UPDATE_ENDPOINT_KEY = "UPDATE_ENDPOINT";
	static final String USERNAME_KEY = "USERNAME";
	static final String PASSWORD_KEY = "PASSWORD";

	public static void main(String[] args) {
		try {
			new RdfUploadService().run();
		} catch (Exception e) {
			e.printStackTrace();
			usage();
		}
	}

	private void run() throws IOException {
		Map<String, String> env = EnvironmentUtils.getProcEnvironment();
		final String endpoint = env.get(ENDPOINT_KEY);
		final String updateEndpoint = env.get(UPDATE_ENDPOINT_KEY);
		final String username = env.get(USERNAME_KEY);
		final String password = env.get(PASSWORD_KEY);

		if(endpoint == null || endpoint.isEmpty())
			throw new IllegalArgumentException();

		Vertx vertx = new VertxFactoryImpl().vertx();
		HttpServer httpServer = vertx.createHttpServer();

		System.out.println("Server listening on port 80");

		httpServer.requestHandler(req -> {
			try {
				req.setExpectMultipart(true);
				final StringBuilder payload = new StringBuilder();
				req.handler(data -> {
					payload.append(data.toString());
				});

				req.endHandler(handler -> {
					String contentType = req.headers().get(HttpHeaders.CONTENT_TYPE);
					Optional<RDFFormat> rdfFormat = Rio.getParserFormatForMIMEType(contentType);
					if(!rdfFormat.isPresent())
						throw new UnsupportedOperationException("Unable to handle Accept-Type: \"" + contentType + "\"");

					RDFParser parser = Rio.createParser(rdfFormat.orElse(RDFFormat.TRIG));
					StatementCollector collector = new StatementCollector();
					parser.setRDFHandler(collector);
					try {
						parser.parse(new StringReader(payload.toString()), "");
					} catch (RDFParseException | RDFHandlerException | IOException e) {
						e.printStackTrace();
						usage();
						System.exit(1);
					}

					SPARQLRepository repo = updateEndpoint == null
						? new SPARQLRepository(endpoint)
						: new SPARQLRepository(endpoint, updateEndpoint);
					if(username != null && password!=null && !username.isEmpty() && !password.isEmpty())
						repo.setUsernameAndPassword(username, password);
					repo.initialize();

					try(RepositoryConnection conn = repo.getConnection()) {
						RDFInserter inserter = new RDFInserter(conn);
						inserter.startRDF();
						collector.getStatements().forEach(statement -> {
							inserter.handleStatement(statement);
						});
						inserter.endRDF();
					}
				});

			} catch (Exception e2) {
				req.response().setStatusCode(400)
					.setStatusMessage(Arrays.toString(e2.getStackTrace()))
					.end();
			}

		req.response()
			.setStatusCode(200)
			.end();

		}).listen(80);
	}

	private static void usage() {
		System.out.println("\n Usage of RdfUploadService"
			+ "\n   Please set following environment variables"
			+ "\n     Mandatory:"
			+ "\n       " + ENDPOINT_KEY
			+ "\n     Optional:"
			+ "\n       " + UPDATE_ENDPOINT_KEY
			+ "\n       " + USERNAME_KEY
			+ "\n       " + PASSWORD_KEY);
	}

}
