package nl.unimaas.ids;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Comparator;

@SuppressWarnings("serial")
class KeyComparator implements Comparator<byte[]>, Serializable {

    public int compare(byte[] key1, byte[] key2) {
        return new BigInteger(key1).compareTo(new BigInteger(key2));
    }

}