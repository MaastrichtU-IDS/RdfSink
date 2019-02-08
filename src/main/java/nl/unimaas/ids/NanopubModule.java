package nl.unimaas.ids;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.nanopub.MalformedNanopubException;
import org.nanopub.Nanopub;
import org.nanopub.NanopubImpl;
import org.nanopub.NanopubUtils;

public class NanopubModule {

	// no instances allowed
	private NanopubModule() {}

	public static void process(RepositoryConnection conn, String payload, RDFFormat format) throws RDF4JException {
		try {
			Nanopub np = new NanopubImpl(payload, format);
			List<Statement> st = new ArrayList<>();
			st.addAll(NanopubUtils.getStatements(np));
			// add admin graph statements...
			conn.add(st);
		} catch (MalformedNanopubException e) {
			e.printStackTrace();
		}
	}

}
