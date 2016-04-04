package gash.router.server.election;

/**
 * Created by pranav on 4/2/16.
 */
public class ElectionIDGenerator {
    private static int masterID = 0;

    public static synchronized int nextID() {
        if (masterID == Integer.MAX_VALUE) {
            masterID = 0;
        }
        return ++masterID;
    }

    public static synchronized void setMasterID(int id) {
        masterID = id;
    }
}
