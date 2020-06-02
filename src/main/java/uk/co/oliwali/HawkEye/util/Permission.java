package uk.co.oliwali.HawkEye.util;

import org.bukkit.command.CommandSender;

/**
 * Permissions handler for HawkEye Supports multiple permissions systems
 * 
 * @author oliverw92
 */
public final class Permission {

	private Permission() {
	}

	/**
	 * Permission to view different pages
	 * 
	 * @param player
	 * @return
	 */
	public static boolean page(CommandSender player) {
		return player.hasPermission("hawkeye.page");
	}

	/**
	 * Permission to search the logs
	 * 
	 * @param player
	 * @return
	 */
	public static boolean search(CommandSender player) {
		return player.hasPermission("hawkeye.search");
	}

	/**
	 * Permission to search a specific data type
	 * 
	 * @param player
	 * @return
	 */
	public static boolean searchType(CommandSender player, String type) {
		return player.hasPermission("hawkeye.search." + type.toLowerCase());
	}

	/**
	 * Permission to teleport to the location of a result
	 * 
	 * @param player
	 * @return
	 */
	public static boolean tpTo(CommandSender player) {
		return player.hasPermission("hawkeye.tpto");
	}

	/**
	 * Permission to use the rollback command
	 * 
	 * @param player
	 * @return
	 */
	public static boolean rollback(CommandSender player) {
		return player.hasPermission("hawkeye.rollback");
	}

	/**
	 * Permission to the hawkeye tool
	 * 
	 * @param player
	 * @return
	 */
	public static boolean tool(CommandSender player) {
		return player.hasPermission("hawkeye.tool");
	}

	/**
	 * Permission to be notified of rule breaks
	 * 
	 * @param player
	 * @return
	 */
	public static boolean notify(CommandSender player) {
		return player.hasPermission("hawkeye.notify");
	}

	/**
	 * Permission to preview rollbacks
	 * 
	 * @param player
	 * @return
	 */
	public static boolean preview(CommandSender player) {
		return player.hasPermission("hawkeye.preview");
	}

	/**
	 * Permission to bind a tool
	 * 
	 * @param player
	 * @return
	 */
	public static boolean toolBind(CommandSender player) {
		return player.hasPermission("hawkeye.tool.bind");
	}

	/**
	 * Permission to rebuild
	 * 
	 * @param player
	 * @return
	 */
	public static boolean rebuild(CommandSender player) {
		return player.hasPermission("hawkeye.rebuild");
	}

	/**
	 * Permission to delete entires
	 * 
	 * @param player
	 * @return
	 */
	public static boolean delete(CommandSender player) {
		return player.hasPermission("hawkeye.delete");
	}
}
