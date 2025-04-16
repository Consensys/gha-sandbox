package net.consensys.peershistory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@CommandLine.Command(name = "Peers History", version = "0.1", mixinStandardHelpOptions = true)
public class Main implements Runnable {
  private static final int DEFAULT_SCRAPE_INTERVAL_SECONDS = 15;
  private static final int DEFAULT_KEEP_ALIVE_LOG_INTERVAL_SECONDS = 60 * 5;
  private static final int DEFAULT_TTL_DAYS = 365;
  private static final URI DEFAULT_RPC_URI = URI.create("http://localhost:8545");
  private static final String LOG_FILENAME = "peers-history.log";
  private static final String SQLITE_CONNECTION_URL_TPL = "jdbc:sqlite:%s";
  private static final String SQLITE_DB_FILENAME = "peers-history.sqlite";
  private static final AtomicLong REQ_COUNTER = new AtomicLong();
  private final Logger APP_LOG = Logger.getLogger("App");
  private final Logger HISTORY_LOG = Logger.getLogger("App.History");
  private final ScheduledExecutorService cleanupScheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().name("CleanupExpiredPeerData").factory());
  private final ScheduledExecutorService keepAliveLogScheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().name("KeepAliveLog").factory());
  private final Map<String, Integer> slotByPeerId = new HashMap<>();
  private final NavigableSet<Integer> freeSlots = new TreeSet<>();
  private final Set<String> reinsertSlotForPeerId = new HashSet<>();
  private final Set<PeerData> prevPeerData = new HashSet<>();
  private final HttpClient httpClient;
  @Option(
      names = {"-v", "--verbose"},
      description = "Enable verbose output"
  )
  boolean verbose = false;
  @Option(
      names = {"-i", "--scrape-interval"},
      description = "Scrape interval in seconds. (default: ${DEFAULT-VALUE})"
  )
  int scrapeIntervalSecs = DEFAULT_SCRAPE_INTERVAL_SECONDS;
  @Option(
      names = {"-l", "--keep-alive-log-interval"},
      description = "Max interval between 2 logs. (default: ${DEFAULT-VALUE})"
  )
  int keepAliveLogIntervalSecs = DEFAULT_KEEP_ALIVE_LOG_INTERVAL_SECONDS;
  @Option(
      names = {"-t", "--ttl"},
      description = "How many days of history to keep. (default: ${DEFAULT-VALUE})"
  )
  int ttlDays = DEFAULT_TTL_DAYS;
  @Option(
      names = {"-d", "--log-dir"},
      description = "Where to put the logs. (default: ${DEFAULT-VALUE})"
  )
  Path logDir = Path.of(".");
  @Option(
      names = {"-s", "--db-dir"},
      description = "Where to put the SQLite db. (default: ${DEFAULT-VALUE})"
  )
  Path dbDir = Path.of(".");
  @Option(
      names = {"-u", "--rpc-url"},
      description = "Execution engine RPC url to scrape. (default: ${DEFAULT-VALUE})"
  )
  URI rpcUri = DEFAULT_RPC_URI;
  private Connection dbConn;
  private volatile Instant lastLogTime = Instant.ofEpochSecond(0);

  public Main() {
    httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(1))
        .build();
  }

  public static void main(final String[] args) {
    final var app = new Main();
    final int exitCode = new CommandLine(app)
        .setExecutionStrategy(app::init)
        .execute(args);
    System.exit(exitCode);
  }

  private static boolean containsPeerDataById(final Set<PeerData> peerData, final String peerId) {
    return peerData.stream().anyMatch(pd -> pd.id.equals(peerId));
  }

  private int init(final CommandLine.ParseResult parseResult) {
    try {
      initLog();
      registerShutdownHook();
      initDb();
      cleanupScheduler.scheduleAtFixedRate(this::deleteExpiredPeerData, 0, 1, TimeUnit.DAYS);
      keepAliveLogScheduler.schedule(this::keepAliveLog, keepAliveLogIntervalSecs, TimeUnit.SECONDS);
    } catch (final IOException | SQLException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    return new CommandLine.RunLast().execute(parseResult);
  }

  @Override
  public void run() {

    while (true) {
      try {
        final var now = Instant.now();
        final var peersData = fetchPeerData();
        savePeerData(now, peersData);
        logIfChanged(now, peersData);
      } catch (IOException e) {
        APP_LOG.log(Level.WARNING, "Error fetching peers", e);
      } catch (InterruptedException e) {
        APP_LOG.log(Level.WARNING, "Interrupted while fetching peers", e);
      }
      try {
        Thread.sleep(Duration.ofSeconds(scrapeIntervalSecs));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private Set<PeerData> fetchPeerData() throws IOException, InterruptedException {
    final var req = HttpRequest.newBuilder()
        .uri(rpcUri)
        .timeout(Duration.ofSeconds(1))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("""
            {"jsonrpc":"2.0","method":"admin_peers","params":[],"id":%d}
            """.formatted(REQ_COUNTER.getAndIncrement()))).build();
    final var resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
    final var jsonParser = JsonParser.parseString(resp.body());
    final var results = jsonParser.getAsJsonObject().getAsJsonArray("result");
    final var peersData = parseResults(results);
    APP_LOG.fine("Scraping admin peer returned " + peersData.size() + " peers");
    return peersData;
  }

  private void initLog() throws IOException {
    final var appLogHandler = new ConsoleHandler();
    appLogHandler.setLevel(verbose ? Level.ALL : Level.INFO);
    APP_LOG.setLevel(verbose ? Level.ALL : Level.INFO);
    APP_LOG.addHandler(appLogHandler);
    final var historyLogHandler = new FileHandler(logDir.resolve(LOG_FILENAME).toString(), 1_000_000, 3, true);
    historyLogHandler.setEncoding("UTF-8");
    historyLogHandler.setFormatter(new NoFormatter());
    HISTORY_LOG.setUseParentHandlers(false);
    HISTORY_LOG.addHandler(historyLogHandler);
  }

  private void initDb() throws SQLException, IOException, InterruptedException {
    dbConn = DriverManager.getConnection(SQLITE_CONNECTION_URL_TPL.formatted(dbDir.resolve(SQLITE_DB_FILENAME).toString()));


    try (final var statement = dbConn.createStatement()) {
      final long now = Instant.now().getEpochSecond();
      try (final var createStateTableStatement = dbConn.prepareStatement("""
          CREATE TABLE IF NOT EXISTS 
              state
          AS
              SELECT
                  ? AS last_fetch_time
          ;""")) {
        createStateTableStatement.setLong(1, now);
        createStateTableStatement.execute();
      }

      statement.executeUpdate("""
          CREATE TABLE IF NOT EXISTS 
              history(
                starting INTEGER,
                ending INTEGER,
                id CHARACTER(128),
                el_name VARCHAR(32),
                el_version VARCHAR(32),
                el_arch VARCHAR(32),
                el_runtime VARCHAR(32),
                protocol_version INTEGER,
                inbound BOOLEAN,
                ip VARCHAR(15),
                port INTEGER,
                slot INTEGER,
                PRIMARY KEY (
                    starting, 
                    id
                )                
              );
          """);
      APP_LOG.fine("DB initialized");

      // see if there is any unclosed slot
      final var unclosedSlots = statement.executeQuery("""
          SELECT 
              *
          FROM 
              history
          WHERE
              ending IS NULL;
          """);

      if (unclosedSlots.next()) {
        // close them now
        statement.executeUpdate("""
            UPDATE 
                history
            SET 
                ending = last_fetch_time
            FROM
                state
            WHERE
                ending IS NULL
            """);
      }

      final var rs = statement.executeQuery("""
          SELECT
              id, slot
          FROM
              history
          GROUP BY
              slot
          HAVING
              ending = max(ending)
          ORDER BY
              slot ASC            
          """);

      final var currentPeerData = fetchPeerData();

      prevPeerData.addAll(currentPeerData);

      IntStream.range(0, currentPeerData.size()).forEach(freeSlots::add);

      while (rs.next()) {
        final var peerId = rs.getString("id");
        final var slot = rs.getInt("slot");

        if (containsPeerDataById(currentPeerData, peerId)) {
          slotByPeerId.put(peerId, slot);
          freeSlots.remove(slot);
          reinsertSlotForPeerId.add(peerId);
        }
      }
    }
  }

  private void registerShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      cleanupScheduler.close();
      // attempt to set the ending on all ongoing connections
      try (final var update = dbConn.prepareStatement("""
          UPDATE
              history
          SET
              ending = ?
          WHERE
              ending IS NULL
          ;""")) {
        final var now = Instant.now().getEpochSecond();
        update.setLong(1, now);
        update.execute();
        APP_LOG.fine("Set ending time for ongoing connection on shutdown");
      } catch (final SQLException e) {
        e.printStackTrace();
      } finally {
        try {
          dbConn.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
    ));
  }

  private void logIfChanged(final Instant now, final Set<PeerData> peersData) {
    // log only if there are changes
    if (!peersData.equals(prevPeerData)) {
      APP_LOG.fine("Logging since peer data changed");
      performLog(now, peersData);
      prevPeerData.clear();
      prevPeerData.addAll(peersData);
    } else {
      APP_LOG.fine("Skipping log since peer data unchanged");
    }

  }

  private void keepAliveLog() {
    final var now = Instant.now();
    final var duration = Duration.between(lastLogTime, now);
    final var secsToNextKeepAlive = keepAliveLogIntervalSecs - duration.getSeconds();
    APP_LOG.fine("Keep alive check: now: %s, lastLogTime %s, duration %s, secsToNextKeepAlive %d".formatted(now, lastLogTime, duration, secsToNextKeepAlive));

    // log if enough time as passed
    if (secsToNextKeepAlive <= 0) {
      APP_LOG.fine("Keep alive log");
      performLog(now, prevPeerData);
      keepAliveLogScheduler.schedule(this::keepAliveLog, keepAliveLogIntervalSecs, TimeUnit.SECONDS);
    } else {
      APP_LOG.fine("Scheduling next keep alive check in %d secs".formatted(secsToNextKeepAlive));
      keepAliveLogScheduler.schedule(this::keepAliveLog, secsToNextKeepAlive, TimeUnit.SECONDS);
    }
  }

  private void performLog(final Instant now, final Set<PeerData> peersData) {
    final String sb = "timestamp=" + now.getEpochSecond() + ',' +
        peersData.stream()
            .map(pd -> slotByPeerId.get(pd.id) + "=" + pd.toMetricString())
            .collect(Collectors.joining(","));

    HISTORY_LOG.info(sb);
    lastLogTime = now;
  }

  private void savePeerData(final Instant now, final Set<PeerData> peerData) {
    try (final var update = dbConn.prepareStatement("""
        UPDATE
            state
        SET
            last_fetch_time = ?
        ;""")) {
      update.setLong(1, now.getEpochSecond());
      update.executeUpdate();
    } catch (SQLException e) {
      APP_LOG.log(Level.WARNING, "Error saving last fetch time", e);
    }

    // free slots of disconnected peers
    final var it = slotByPeerId.entrySet().iterator();
    while (it.hasNext()) {
      final var entry = it.next();
      if (!containsPeerDataById(peerData, entry.getKey())) {
        APP_LOG.fine("Disconnected slot " + entry.getValue() + ":" + entry.getKey());
        try (final var update = dbConn.prepareStatement("""
            UPDATE
                history
            SET
                ending = ?
            WHERE
                    id = ?
                AND ending IS NULL
            ;""")) {
          update.setLong(1, now.getEpochSecond());
          update.setString(2, entry.getKey());
          update.executeUpdate();
        } catch (SQLException e) {
          APP_LOG.log(Level.WARNING, "Error saving disconnection", e);
        }
        freeSlots.add(entry.getValue());
        it.remove();
      }
    }

    // insert newly connected peers
    peerData.stream()
        .filter(pd -> !slotByPeerId.containsKey(pd.id))
        .forEach(pd -> saveNewPeerDataRecord(now.getEpochSecond(), pd, getSlot(pd)));

    // special case on startup when the same peer was present also before we keep the same slot,
    // but we need to reinsert it with a new starting time
    if (!reinsertSlotForPeerId.isEmpty()) {
      peerData.stream()
          .filter(pd -> reinsertSlotForPeerId.contains(pd.id))
          .forEach(pd -> {
            saveNewPeerDataRecord(now.getEpochSecond(), pd, slotByPeerId.get(pd.id));
            reinsertSlotForPeerId.remove(pd.id);
          });
    }
  }

  private void saveNewPeerDataRecord(final long now, final PeerData peerData, final int slot) {

    try (final var insert = dbConn.prepareStatement("""
        INSERT INTO
            history
        VALUES(
            ?, -- starting
            NULL,
            ?, -- id
            ?, -- el_name
            ?, -- el_version
            ?, -- el_arch
            ?, -- el_runtime
            ?, -- protocol_version
            ?, -- inbound
            ?, -- ip
            ?, -- port
            ?  --slot
        );""")) {
      insert.setLong(1, now);
      insert.setString(2, peerData.id);
      insert.setString(3, peerData.elName);
      insert.setString(4, peerData.elVersion);
      insert.setString(5, peerData.elArch);
      insert.setString(6, peerData.elRuntime);
      insert.setInt(7, peerData.protocolVersion);
      insert.setBoolean(8, peerData.inbound);
      insert.setString(9, peerData.ip);
      insert.setInt(10, peerData.port);
      insert.setInt(11, slot);
      insert.executeUpdate();
      APP_LOG.fine("Connected slot " + slot + ":" + peerData);

    } catch (SQLException e) {
      APP_LOG.log(Level.WARNING, "Error saving new connections", e);
    }
  }

  private void deleteExpiredPeerData() {
    try (final var update = dbConn.prepareStatement("""
        DELETE FROM
            history
        WHERE
                ending < ?
        ;""")) {
      update.setLong(1, Instant.now().minus(Duration.ofDays(ttlDays)).getEpochSecond());
      update.executeUpdate();
    } catch (SQLException e) {
      APP_LOG.log(Level.WARNING, "Error deleting expired peer data", e);
    }
  }

  private int getSlot(final PeerData peerData) {
    return slotByPeerId.computeIfAbsent(peerData.id, id -> {
      if (freeSlots.isEmpty()) {
        return slotByPeerId.size();
      }
      return freeSlots.pollFirst();
    });
  }

  private Set<PeerData> parseResults(final JsonArray results) {
    return results.asList().stream().map(JsonElement::getAsJsonObject)
        .map(
            jsonObject -> {
              final var name = jsonObject.get("name").getAsString();
              final var nameParts = name.split("/");
              final var elName = nameParts.length > 0 ? nameParts[0] : name;
              final var elVersion = nameParts.length > 1 ? nameParts[1] : "";
              final var elArch = nameParts.length > 2 ? nameParts[2] : "";
              final var elRuntime = nameParts.length > 3 ? nameParts[3] : "";
              final var addressParts = jsonObject.getAsJsonObject("network").get("remoteAddress").getAsString().split(":");
              final var ip = addressParts.length > 0 ? addressParts[0] : "";

              int port;
              if (addressParts.length > 1) {
                try {
                  port = Integer.parseInt(addressParts[1]);
                } catch (NumberFormatException e) {
                  port = -2;
                }
              } else {
                port = -1;
              }

              return new PeerData(
                  jsonObject.get("id").getAsString(),
                  elName,
                  elVersion,
                  elArch,
                  elRuntime,
                  jsonObject.getAsJsonObject("protocols").getAsJsonObject("eth").get("version").getAsInt(),
                  jsonObject.getAsJsonObject("network").get("inbound").getAsBoolean(),
                  ip, port
              );
            }
        ).collect(Collectors.toUnmodifiableSet());
  }

  private record PeerData(String id, String elName, String elVersion, String elArch, String elRuntime,
                          int protocolVersion,
                          boolean inbound, String ip, int port) {
    @Override
    public boolean equals(final Object obj) {
      return obj instanceof PeerData && ((PeerData) obj).id.equals(id);
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }

    public String toMetricString() {
      return "%s⸱%s⸱%s⸱%s⸱%d⸱%s⸱%s⸱%d⸱%s".formatted(elName, elVersion, elArch, elRuntime, protocolVersion, inbound ? "↓" : "↑", ip, port, id);
    }
  }

  private class NoFormatter extends Formatter {
    @Override
    public String format(final LogRecord record) {
      return record.getMessage() + "\n";
    }
  }
}