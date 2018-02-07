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

import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxFactoryImpl;

public class NanopubsUploader {

	public static void main(String[] args) {
		new NanopubsUploader().run();
	}
	
	void run() {
		Vertx vertx = new VertxFactoryImpl().vertx();
//		HttpClient client = vertx.createHttpClient(new HttpClientOptions());
		vertx.createHttpServer().requestHandler(req -> {
			System.out.println(req.uri());
			System.out.println(req.method());
			req.headers().forEach(h -> {
				System.out.println(h.getKey() + "=" + h.getValue());
			});
			
			req.setExpectMultipart(true); // TODO: check with large files
			final StringBuilder payload = new StringBuilder();
			req.handler(data -> {
				payload.append(data.toString());
			});
			req.endHandler(handler -> {
				//System.out.println("done");
				System.out.println(payload.toString());
				RDFParser parser = Rio.createParser(RDFFormat.TRIG);
				StatementCollector collector = new StatementCollector();
				parser.setRDFHandler(collector);
				try {
					parser.parse(new StringReader(payload.toString()), "");
				} catch (RDFParseException | RDFHandlerException | IOException e) {
					e.printStackTrace();
				}
				
				SPARQLRepository repo = new SPARQLRepository("");
				repo.initialize();
				try(RepositoryConnection conn = repo.getConnection()) {
					RDFInserter inserter = new RDFInserter(conn);
					inserter.startRDF();
					collector.getStatements().forEach(statement -> {
						inserter.handleStatement(statement);
					});
					inserter.endRDF();
				}
				
				req.response().setStatusCode(200).end();
			});
		}).listen(8090);
	}

}
