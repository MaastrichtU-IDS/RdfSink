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
import com.squareup.tape.QueueFile;

class SparqlEndpointWriterThread extends Thread {
	QueueFile queue = null;
	String endpoint = null;
	String updateEndpoint = null;
	String username = null;
	String password = null;

	volatile boolean terminated = false;

	public void terminate() {
		this.terminated = true;
	}



	public SparqlEndpointWriterThread(QueueFile queue, String endpoint, String updateEndpoint, String username,
			String password) {
		this.queue = queue;
		this.endpoint = endpoint;
		this.updateEndpoint = updateEndpoint;
		this.username = username;
		this.password = password;
	}

	@Override
	public void run() {
		while(!terminated) {
			try {
				if (!queue.isEmpty()) {
					String queueEntry = new String(queue.peek());

					JsonArray arr = Json.parse(queueEntry).asArray();
					String contentType = arr.get(0).asString();
					String payload = arr.get(1).asString();

//					System.out.println("\n\nContent-Type: " + contentType + "\n" + payload + "\n");

					RDFFormat rdfFormat = Rio.getParserFormatForMIMEType(contentType).get();

					SPARQLRepository repo = updateEndpoint == null
							? new SPARQLRepository(endpoint)
							: new SPARQLRepository(endpoint, updateEndpoint);
					if (username != null && password != null && !username.isEmpty() && !password.isEmpty())
						repo.setUsernameAndPassword(username, password);
					repo.initialize();

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

					repo.shutDown();
					queue.remove();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}