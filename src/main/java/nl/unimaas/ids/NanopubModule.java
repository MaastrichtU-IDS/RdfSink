package nl.unimaas.ids;


import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.nanopub.MalformedNanopubException;
import org.nanopub.Nanopub;
import org.nanopub.NanopubImpl;
import org.nanopub.NanopubUtils;
import org.nanopub.SimpleTimestampPattern;
import org.nanopub.extra.security.MalformedCryptoElementException;
import org.nanopub.extra.security.NanopubSignatureElement;
import org.nanopub.extra.security.SignatureUtils;

public class NanopubModule {

	// no instances allowed
	private NanopubModule() {}

	public static void process(RepositoryConnection conn, String payload, RDFFormat format) throws RDF4JException {
		try {
			Nanopub np = new NanopubImpl(payload, format);
			// TODO: check that nanopub doesn't use admin namespace
			List<Statement> st = new ArrayList<>();
			st.addAll(NanopubUtils.getStatements(np));
			st.add(vf.createStatement(np.getUri(), HAS_HEAD_GRAPH, np.getHeadUri(), ADMIN_GRAPH));
			try {
				NanopubSignatureElement el = SignatureUtils.getSignatureElement(np);
				if (el != null && SignatureUtils.hasValidSignature(el) && el.getPublicKeyString() != null) {
					st.add(vf.createStatement(np.getUri(), HAS_VALID_SIGNATURE_FOR_PUBLIC_KEY, vf.createLiteral(el.getPublicKeyString()), ADMIN_GRAPH));
				}
			} catch (GeneralSecurityException | MalformedCryptoElementException ex) {}
			Calendar timestamp = SimpleTimestampPattern.getCreationTime(np);
			if (timestamp != null) {
				st.add(vf.createStatement(np.getUri(), CREATION_DAY, vf.createLiteral(getDayString(timestamp)), ADMIN_GRAPH));
				st.add(vf.createStatement(np.getUri(), CREATION_MONTH, vf.createLiteral(getMonthString(timestamp)), ADMIN_GRAPH));
				st.add(vf.createStatement(np.getUri(), CREATION_YEAR, vf.createLiteral(getYearString(timestamp)), ADMIN_GRAPH));
			}
			conn.add(st);
		} catch (MalformedNanopubException e) {
			e.printStackTrace();
		}
	}

	private static String getDayString(Calendar c) {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		df.setTimeZone(timeZone);
		return df.format(c.getTime());
	}

	private static String getMonthString(Calendar c) {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMM");
		df.setTimeZone(timeZone);
		return df.format(c.getTime());
	}

	private static String getYearString(Calendar c) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy");
		df.setTimeZone(timeZone);
		return df.format(c.getTime());
	}

	private static TimeZone timeZone = TimeZone.getTimeZone("UTC");
	private static ValueFactory vf = SimpleValueFactory.getInstance();

	public static final IRI ADMIN_GRAPH = vf.createIRI("http://purl.org/nanopub/admin/graph");
	public static final IRI HAS_HEAD_GRAPH = vf.createIRI("http://purl.org/nanopub/admin/hasHeadGraph");
	public static final IRI CREATION_DAY = vf.createIRI("http://purl.org/nanopub/admin/creationDay");
	public static final IRI CREATION_MONTH = vf.createIRI("http://purl.org/nanopub/admin/creationMonth");
	public static final IRI CREATION_YEAR = vf.createIRI("http://purl.org/nanopub/admin/creationYear");
	public static final IRI HAS_VALID_SIGNATURE_FOR_PUBLIC_KEY = vf.createIRI("http://purl.org/nanopub/admin/hasValidSignatureForPublicKey");

}
