package nl.unimaas.ids;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.exec.environment.EnvironmentUtils;
import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;

import virtuoso.rdf4j.driver.VirtuosoRepository;


class SparqlEndpointThread extends Thread {
	private Queue queue = null;
	private String endpoint = null;
	private String updateEndpoint = null;
	private String username = null;
	private String password = null;
	private String module = null;
	private String repoType = null;
	private SPARQLRepository sparqlRepository;
	private VirtuosoRepository virtuosoRepository;

	volatile boolean terminated = false;

	public void terminate() {
		this.terminated = true;
	}



	public SparqlEndpointThread(Queue queue) throws IOException {
		this.queue = queue;
		Map<String, String> env = EnvironmentUtils.getProcEnvironment();
		endpoint = env.get(RdfSink.ENDPOINT_KEY);
		if (endpoint.matches("https?://[^/]*/?")) {
			endpoint = endpoint.replaceFirst("^https?://([^/]*)/?$", "$1");
		}
		updateEndpoint = env.get(RdfSink.UPDATE_ENDPOINT_KEY);
		username = env.get(RdfSink.USERNAME_KEY);
		password = env.get(RdfSink.PASSWORD_KEY);
		module = env.get(RdfSink.MODULE_KEY);
		repoType = env.get(RdfSink.REPOTYPE_KEY);

		if(endpoint == null || endpoint.isEmpty())
			throw new IllegalArgumentException();

		if (module == null) {
			this.module = "";
		} else {
			this.module = module.toLowerCase();
		}
	}

	@Override
	public void run() {
		try {
			init();
		} catch (RDF4JException | IOException ex) {
			ex.printStackTrace();
		}
		while(!terminated) {
			try {
				if (!queue.isEmpty()) {
					String queueEntry = queue.poll();
					// Not sure how this can happen, but at least once it was experienced that queueEntry was null and then
					// this thread crashed, and that's why we have this additional check here:
					if (queueEntry == null) continue;

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

				} else {
					// nothing to do, let's wait a second
					sleep(1000);
				}
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
		shutdownRepository();
	}

	private void init() throws RDF4JException, IOException {
		// Wait a bit to make sure endpoint is ready:
		try {
			TimeUnit.SECONDS.sleep(60);
		} catch (InterruptedException ex) {}
		RepositoryConnection conn = getRepoConnection();
		try {
			if (module.equals("nanopub") || module.equals("nanopub-signed")) {
				NanopubModule.init(conn);
			}
		} finally {
			conn.close();
		}
	}

	private void process(String payload, RDFFormat format) throws RDF4JException, IOException {
		RepositoryConnection conn = getRepoConnection();
		try {
			if (module.equals("nanopub")) {
				NanopubModule.process(conn, payload, format, false);
			} else if (module.equals("nanopub-signed")) {
				NanopubModule.process(conn, payload, format, true);
			} else {
				conn.add(new StringReader(payload), "http://null/", format);
			}
		} finally {
			conn.close();
		}
	}

	private RepositoryConnection getRepoConnection() {
		if ("virtuoso".equals(repoType)) {
			return getVirtuosoRepository().getConnection();
		} else {
			return getSparqlRepository().getConnection();
		}
	}

	private SPARQLRepository getSparqlRepository() {
		if(sparqlRepository == null || !sparqlRepository.isInitialized()) {
			sparqlRepository = updateEndpoint == null
					? new SPARQLRepository(endpoint)
					: new SPARQLRepository(endpoint, updateEndpoint);
			if (username != null && password != null && !username.isEmpty() && !password.isEmpty())
				sparqlRepository.setUsernameAndPassword(username, password);
			sparqlRepository.initialize();
		}
		return sparqlRepository;
	}

	private VirtuosoRepository getVirtuosoRepository() {
		if (virtuosoRepository == null || !virtuosoRepository.isInitialized()) {
			virtuosoRepository = new VirtuosoRepository(endpoint, username, password);
			virtuosoRepository.initialize();
		}
		return virtuosoRepository;
	}

	private void shutdownRepository() {
		if ("virtuoso".equals(repoType)) {
			VirtuosoRepository repo = getVirtuosoRepository();
			if(repo != null && repo.isInitialized()) {
				repo.shutDown();
			}
		} else {
			SPARQLRepository repo = getSparqlRepository();
			if(repo != null && repo.isInitialized()) {
				repo.shutDown();
			}
		}
	}

}
