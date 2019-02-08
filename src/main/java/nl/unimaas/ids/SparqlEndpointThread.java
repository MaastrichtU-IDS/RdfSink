package nl.unimaas.ids;

import java.io.IOException;
import java.io.StringReader;

import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;


class SparqlEndpointThread extends Thread {
	private Queue queue = null;
	private String endpoint = null;
	private String updateEndpoint = null;
	private String username = null;
	private String password = null;
	private String module = null;
	private SPARQLRepository repository;

	volatile boolean terminated = false;

	public void terminate() {
		this.terminated = true;
	}



	public SparqlEndpointThread(Queue queue, String endpoint, String updateEndpoint, String username, String password, String module) {
		this.queue = queue;
		this.endpoint = endpoint;
		this.updateEndpoint = updateEndpoint;
		this.username = username;
		this.password = password;
		if (module == null) {
			this.module = "";
		} else {
			this.module = module.toLowerCase();
		}
	}

	@Override
	public void run() {
		while(!terminated) {
			try {
				if (!queue.isEmpty()) {
					String queueEntry = queue.poll();

					JsonArray arr = Json.parse(queueEntry).asArray();
					String contentType = arr.get(0).asString();
					String payload = arr.get(1).asString();

					RDFFormat rdfFormat = Rio.getParserFormatForMIMEType(contentType).get();
					try {
						process(payload, rdfFormat);
					} catch (RDF4JException | IOException e ) {
						// add item to end of queue if something went wrong and wait 5s
						queue.push(queueEntry);
						e.printStackTrace();
						sleep(5000);
					}

				} else
					// nothing to do, let's wait a second
					sleep(1000);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
		SPARQLRepository repo = getRepository();
		if(repo != null && repo.isInitialized()) {
			repo.shutDown();
		}
	}

	private void process(String payload, RDFFormat format) throws RDF4JException, IOException {
		RepositoryConnection conn = repository.getConnection();
		try {
			if (module.equals("nanopub")) {
				NanopubModule.process(conn, payload, format);
			} else {
				conn.add(new StringReader(payload), "http://null/", format);
			}
		} finally {
			conn.close();
		}
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
