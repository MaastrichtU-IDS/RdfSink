package nl.unimaas.ids;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.http.HttpStatus;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.repository.util.RDFInserter;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;

import com.eclipsesource.json.JsonArray;
import com.squareup.tape.QueueFile;

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

		File queueFile = new File("/data/nanopubs.queue");

		queueFile.getParentFile().mkdirs();

		final QueueFile queue = new QueueFile(queueFile);
		SparqlEndpointWriterThread rdfwriter = new SparqlEndpointWriterThread(queue, endpoint, updateEndpoint, username, password);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				rdfwriter.terminate();
				try {
					rdfwriter.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					queue.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		rdfwriter.start();

		System.out.println("Server listening on port 80");

		httpServer.requestHandler(req -> {
			try {
				String contentType = req.headers().get(HttpHeaders.CONTENT_TYPE);

				req.setExpectMultipart(true);
				final StringBuilder payload = new StringBuilder();
				req.handler(data -> {
					payload.append(data.toString());
				});

				req.endHandler(handler -> {
					if(!Rio.getParserFormatForMIMEType(contentType).isPresent())
						throw new UnsupportedOperationException("Unable to handle Accept-Type: \"" + contentType + "\"");
					try {
						queue.add(new JsonArray().add(contentType).add(payload.toString()).toString().getBytes());
//						System.out.println("ADDED");
					} catch (IOException e1) {
						throw new UnsupportedOperationException("Unable to add new entry to queue", e1);
					}
				});

			} catch (Exception e2) {
				req.response().setStatusCode(HttpStatus.SC_BAD_REQUEST)
					.setStatusMessage(Arrays.toString(e2.getStackTrace()))
					.end();
			}

		req.response()
			.setStatusCode(HttpStatus.SC_OK)
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
