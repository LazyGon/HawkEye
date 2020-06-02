package uk.co.oliwali.HawkEye.database;

import java.lang.reflect.InvocationTargetException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;

import uk.co.oliwali.HawkEye.DataType;
import uk.co.oliwali.HawkEye.HawkEye;
import uk.co.oliwali.HawkEye.entry.DataEntry;
import uk.co.oliwali.HawkEye.util.BlockUtil;
import uk.co.oliwali.HawkEye.util.Config;
import uk.co.oliwali.HawkEye.util.Util;

/**
 * Handler for everything to do with the database. All queries except searching
 * goes through this class.
 * 
 * @author oliverw92
 */
public class DataManager extends TimerTask {

	private static final LinkedBlockingQueue<DataEntry> queue = new LinkedBlockingQueue<>();
	private static ConnectionManager connections;
	public static Timer loggingTimer = null;
	public static Timer cleanseTimer = null;
	public static final HashMap<OfflinePlayer, Integer> dbPlayers = new HashMap<>();
	public static final HashMap<String, Integer> dbWorlds = new HashMap<>();

	/**
	 * Initiates database connection pool, checks tables, starts cleansing utility
	 * Throws an exception if it is unable to complete setup
	 * 
	 * @param instance
	 * @throws Exception
	 */
	public DataManager(HawkEye instance) throws Exception {

		connections = new ConnectionManager(Config.DbUrl, Config.DbUser, Config.DbPassword);
		getConnection().close();

		// Check tables and update player/world lists
		if (!checkTables() || !updateDbLists()) {
			throw new Exception();
		} 

		// Start cleansing utility
		try {
			new CleanseUtil();
		} catch (Exception e) {
			Util.severe(e.getMessage());
			Util.severe("Unable to start cleansing utility - check your cleanse age");
		}

		// Start logging timer
		loggingTimer = new Timer();
		loggingTimer.scheduleAtFixedRate(this, 2000, 2000);
	}

	/**
	 * Closes down all connections
	 */
	public static void close() {
		connections.close();
		if (cleanseTimer != null) {
			cleanseTimer.cancel();
		}
		if (loggingTimer != null) {
			loggingTimer.cancel();
		}
	}

	/**
	 * Adds a {@link DataEntry} to the database queue. {Rule}s are checked at this
	 * point
	 * 
	 * @param entry {@link DataEntry} to be added
	 * @return
	 */
	public static void addEntry(DataEntry entry) {

		if (!Config.isLogged(entry.getType())) {
			return;
		}

		// Check block filter
		if (entry.getType() == DataType.BLOCK_BREAK) {
			if (Config.BlockFilter.contains(BlockUtil.getBlockStringName(entry.getSqlData()))) {
				return;
			}
		} else if (entry.getType() == DataType.BLOCK_PLACE) {
			String txt = entry.getSqlData().indexOf("-") == -1
					? BlockUtil.getBlockStringName(entry.getSqlData())
					: BlockUtil.getBlockStringName(entry.getSqlData().substring(entry.getSqlData().indexOf("-") + 1));
			
			if (Config.BlockFilter.contains(txt)) {
				return;
			}
		}

		// Check world ignore list
		if (Config.IgnoreWorlds.contains(entry.getWorld()))
			return;

		queue.add(entry);
	}

	/**
	 * Retrieves an entry from the database
	 * 
	 * @param id id of entry to return
	 * @return
	 */
	public static DataEntry getEntry(int id) {
		try (
			JDCConnection conn = getConnection();
			ResultSet res = conn.createStatement().executeQuery(
					"SELECT * FROM `" + Config.DbHawkEyeTable + "` WHERE `data_id` = " + id);	
		) {
			if (res.next()) {
				return createEntryFromRes(res);
			}
		} catch (Exception ex) {
			Util.severe("Unable to retrieve data entry from MySQL Server: " + ex);
		}
		return null;
	}

	/**
	 * Deletes an entry from the database
	 * 
	 * @param dataid id to delete
	 */
	public static void deleteEntry(int dataid) {
		Thread thread = new Thread(new DeleteEntry(dataid));
		thread.start();
	}

	public static void deleteEntries(List<?> entries) {
		Thread thread = new Thread(new DeleteEntry(entries));
		thread.start();
	}

	/**
	 * Get a players name from the database player list
	 * 
	 * @param id
	 * @return offlineplayer
	 */
	public static OfflinePlayer getPlayer(int id) {
		for (Entry<OfflinePlayer, Integer> entry : dbPlayers.entrySet()) {
			if (entry.getValue() == id) {
				return entry.getKey();
			}
		}
		return null;
	}

	/**
	 * Get a world name from the database world list
	 * 
	 * @param id
	 * @return world name
	 */
	public static String getWorld(int id) {
		for (Entry<String, Integer> entry : dbWorlds.entrySet()) {
			if (entry.getValue() == id) {
				return entry.getKey();
			}
		}
		return null;
	}

	/**
	 * Returns a database connection from the pool
	 * 
	 * @return {JDCConnection}
	 */
	public static JDCConnection getConnection() {
		try {
			return connections.getConnection();
		} catch (final SQLException ex) {
			Util.severe("Error whilst attempting to get connection: " + ex);
			return null;
		}
	}

	/**
	 * Creates a {@link DataEntry} from the inputted {ResultSet}
	 * 
	 * @param res
	 * @return returns a {@link DataEntry}
	 * @throws SQLException
	 */
	public static DataEntry createEntryFromRes(ResultSet res) throws SQLException {
		DataType type = DataType.fromId(res.getInt("action"));
		if (type == null) {
			throw new SQLException("Invalid action type id is stored in database.");
		}

		DataEntry entry;
		try {
			entry = (DataEntry) type.getEntryClass().getConstructor().newInstance();
		} catch (InvocationTargetException | NoSuchMethodException | InstantiationException | IllegalAccessException e) {
			throw new SQLException("Cannot construct data entry: " + type.getConfigName());
		}
		entry.setPlayer(DataManager.getPlayer(res.getInt("player_id")));
		entry.setDate(res.getString("date"));
		entry.setDataId(res.getInt("data_id"));
		entry.setType(DataType.fromId(res.getInt("action")));
		entry.interpretSqlData(res.getString("data"));
		entry.setPlugin(res.getString("plugin"));
		entry.setWorld(DataManager.getWorld(res.getInt("world_id")));
		entry.setX(res.getInt("x"));
		entry.setY(res.getInt("y"));
		entry.setZ(res.getInt("z"));
		return entry;
	}

	/**
	 * Adds a player to the database
	 */
	private boolean addPlayer(OfflinePlayer player) {
		String uuid = player.getUniqueId().toString();
		String name = player.getName();
		Util.debug("Attempting to add player '" + name + "' to database");
		try (JDCConnection conn = getConnection()) {
			conn.createStatement()
					.execute("INSERT IGNORE INTO `" + Config.DbPlayerTable + "` (player_uuid, player_name) VALUES ('" + uuid + "', '" + name + "');");
		} catch (SQLException ex) {
			Util.severe("Unable to add player to database: " + ex);
			return false;
		}

		return updateDbLists();
	}

	/**
	 * Adds a world to the database
	 */
	private boolean addWorld(String name) {
		Util.debug("Attempting to add world '" + name + "' to database");
		try (JDCConnection conn = getConnection()) {
			conn.createStatement()
					.execute("INSERT IGNORE INTO `" + Config.DbWorldTable + "` (world) VALUES ('" + name + "');");
		} catch (SQLException ex) {
			Util.severe("Unable to add world to database: " + ex);
			return false;
		}
		
		return updateDbLists();
	}

	/**
	 * Updates world and player local lists
	 * 
	 * @return true on success, false on failure
	 */
	private boolean updateDbLists() {
		try (
			JDCConnection conn = getConnection();
			Statement stmnt = conn.createStatement();
			ResultSet resPlayer = stmnt.executeQuery("SELECT * FROM `" + Config.DbPlayerTable + "`;");
			ResultSet resWorld = stmnt.executeQuery("SELECT * FROM `" + Config.DbWorldTable + "`;");
		) {
			while (resPlayer.next()) {
				dbPlayers.put(Bukkit.getOfflinePlayer(UUID.fromString(resPlayer.getString("player_uuid"))) , resPlayer.getInt("player_id"));
			}
			while (resWorld.next()) {
				dbWorlds.put(resWorld.getString("world"), resWorld.getInt("world_id"));
			}
			return true;
		} catch (SQLException ex) {
			Util.severe("Unable to update local data lists from database: " + ex);
			return false;
		}
	}

	/**
	 * Checks that all tables are up to date and exist
	 * 
	 * @return true on success, false on failure
	 */
	private boolean checkTables() {

		try (
			JDCConnection conn = getConnection();
			Statement stmnt = conn.createStatement();
		) {
			DatabaseMetaData dbm = conn.getMetaData();
				
			// Check if tables exist
			if (!JDBCUtil.tableExists(dbm, Config.DbPlayerTable)) {
				Util.info("Table `" + Config.DbPlayerTable + "` not found, creating...");
				stmnt.execute("CREATE TABLE IF NOT EXISTS `" + Config.DbPlayerTable
						+ "` (`player_id` int(11) NOT NULL AUTO_INCREMENT, `player_uuid` char(36) NOT NULL, `player_name` varchar(32) NOT NULL, PRIMARY KEY (`player_id`), UNIQUE KEY `player` (`player_uuid`) );");
			}
			if (!JDBCUtil.tableExists(dbm, Config.DbWorldTable)) {
				Util.info("Table `" + Config.DbWorldTable + "` not found, creating...");
				stmnt.execute("CREATE TABLE IF NOT EXISTS `" + Config.DbWorldTable
						+ "` (`world_id` int(11) NOT NULL AUTO_INCREMENT, `world` varchar(255) NOT NULL, PRIMARY KEY (`world_id`), UNIQUE KEY `world` (`world`) );");
			}
			if (!JDBCUtil.tableExists(dbm, Config.DbHawkEyeTable)) {
				Util.info("Table `" + Config.DbHawkEyeTable + "` not found, creating...");
				stmnt.execute("CREATE TABLE IF NOT EXISTS `" + Config.DbHawkEyeTable
						+ "` (`data_id` int(11) NOT NULL AUTO_INCREMENT, `date` varchar(255) NOT NULL, `player_id` int(11) NOT NULL, `action` int(11) NOT NULL, `world_id` varchar(255) NOT NULL, `x` double NOT NULL, `y` double NOT NULL, `z` double NOT NULL, `data` varchar(500) DEFAULT NULL, `plugin` varchar(255) DEFAULT 'HawkEye', PRIMARY KEY (`data_id`), KEY `player_action_world` (`player_id`,`action`,`world_id`), KEY `x_y_z` (`x`,`y`,`z` ));");
			}

			return true;
		} catch (SQLException ex) {
			Util.severe("Error checking HawkEye tables: " + ex);
			return false;
		}
	}

	/**
	 * Empty the {@link DataEntry} queue into the database
	 */
	@Override
	public void run() {
		if (queue.isEmpty()) {
			return;
		}

		try (JDCConnection conn = getConnection()) {

			while (!queue.isEmpty()) {
				DataEntry entry = queue.poll();
				// Sort out player IDs
				if (!dbPlayers.containsKey(entry.getPlayer()) && !addPlayer(entry.getPlayer())) {
					Util.debug("Player '" + entry.getPlayer() + "' not found, skipping entry");
					continue;
				}
				if (!dbWorlds.containsKey(entry.getWorld()) && !addWorld(entry.getWorld())) {
					Util.debug("World '" + entry.getWorld() + "' not found, skipping entry");
					continue;
				}

				// If player ID is unable to be found, continue
				if (entry.getPlayer() == null || dbPlayers.get(entry.getPlayer()) == null) {
					Util.debug("No player found, skipping entry");
					continue;
				}

				PreparedStatement stmnt;
				// If we are re-inserting we need to also insert the data ID
				if (entry.getDataId() > 0) {
					stmnt = conn.prepareStatement("INSERT into `" + Config.DbHawkEyeTable
							+ "` (date, player_id, action, world_id, x, y, z, data, plugin, data_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
					stmnt.setInt(10, entry.getDataId());
				} else {
					stmnt = conn.prepareStatement("INSERT into `" + Config.DbHawkEyeTable
							+ "` (date, player_id, action, world_id, x, y, z, data, plugin) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);");
				}
				stmnt.setString(1, entry.getDate());
				stmnt.setInt(2, dbPlayers.get(entry.getPlayer()));
				stmnt.setInt(3, entry.getType().getId());
				stmnt.setInt(4, dbWorlds.get(entry.getWorld()));
				stmnt.setDouble(5, entry.getX());
				stmnt.setDouble(6, entry.getY());
				stmnt.setDouble(7, entry.getZ());
				stmnt.setString(8, entry.getSqlData());
				stmnt.setString(9, entry.getPlugin());
				stmnt.executeUpdate();
				stmnt.close();
			}
		} catch (Exception ex) {
			Util.severe("Exception: " + ex);
		}
	}
}
