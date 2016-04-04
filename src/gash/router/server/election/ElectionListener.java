package gash.router.server.election;

/**
 * Created by pranav on 4/2/16.
 */
public interface ElectionListener {
    void concludeWith(boolean success, Integer LeaderID);
}
