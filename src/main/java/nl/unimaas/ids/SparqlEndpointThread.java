package nl.unimaas.ids;

import java.io.IOException;
import java.io.StringReader;

import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.repository.util.RDFInserter;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;


class SparqlEndpointThread extends Thread {
	private Queue queue = null;
	private String endpoint = null;
	private String updateEndpoint = null;
	private String username = null;
	private String password = null;
	private SPARQLRepository repository;

	volatile boolean terminated = false;

	public void terminate() {
		this.terminated = true;
	}



	public SparqlEndpointThread(Queue queue, String endpoint, String updateEndpoint, String username, String password) {
		this.queue = queue;
		this.endpoint = endpoint;
		this.updateEndpoint = updateEndpoint;
		this.username = username;
		this.password = password;
	}

	@Override
	public void run() {
		SPARQLRepository repo = null;
		while(!terminated) {
			try {
				if (!queue.isEmpty()) {
					String queueEntry = queue.poll();

					JsonArray arr = Json.parse(queueEntry).asArray();
					String contentType = arr.get(0).asString();
					String payload = arr.get(1).asString();

//					System.err.println("\n\nContent-Type: " + contentType + "\n" + payload + "\n");

					repo = getRepository();

					RDFFormat rdfFormat = Rio.getParserFormatForMIMEType(contentType).get();
					RDFParser parser = Rio.createParser(rdfFormat);
					StatementCollector collector = new StatementCollector();
					parser.setRDFHandler(collector);
					try {
						parser.parse(new StringReader(payload.toString()), "");

						try (RepositoryConnection conn = repo.getConnection()) {
							RDFInserter inserter = new RDFInserter(conn);
							inserter.startRDF();
							collector.getStatements().forEach(statement -> {
								inserter.handleStatement(statement);
							});
							inserter.endRDF();
						}
					} catch (RDFParseException | RDFHandlerException | IOException e) {
						e.printStackTrace();
					}

				} else
					sleep(100);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
		if(repo != null && repo.isInitialized())
			repo.shutDown();
	}

	private SPARQLRepository getRepository() {
		if(repository == null || !repository.isInitialized()) {
			repository = updateEndpoint == null
					? new SPARQLRepository(endpoint)
					: new SPARQLRepository(endpoint, updateEndpoint);
			if (username != null && password != null && !username.isEmpty() && !password.isEmpty())
				repository.setUsernameAndPassword(username, password);
			repository.initialize();
		}
		return repository;
	}
}