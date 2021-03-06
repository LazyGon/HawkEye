package uk.co.oliwali.HawkEye.entry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;

import org.bukkit.Material;
import org.bukkit.OfflinePlayer;
import org.bukkit.block.Block;
import org.bukkit.block.BlockFace;
import org.bukkit.block.Sign;
import org.bukkit.entity.Player;

import uk.co.oliwali.HawkEye.DataType;
import uk.co.oliwali.HawkEye.util.Util;

/**
 * Represents a sign entry in the database Contains system for encoding sign
 * text and storing sign orientation etc
 * 
 * @author oliverw92
 */
public class SignEntry extends DataEntry {

	private BlockFace facing = BlockFace.NORTH;
	private boolean wallSign = true;
	private String[] lines = new String[4];

	public SignEntry() {
	}

	public SignEntry(OfflinePlayer player, DataType type, Block block) {
		interpretSignBlock(block);
		setInfo(player, type, block.getLocation());
	}

	public SignEntry(OfflinePlayer player, DataType type, Block block, String[] lines) {
		interpretSignBlock(block);
		this.lines = lines;
		setInfo(player, type, block.getLocation());
	}

	/**
	 * Extracts the sign data from a block
	 * 
	 * @param block
	 */
	private void interpretSignBlock(Block block) {
		if (!(block.getState() instanceof Sign)) {
			return;
		}
		Sign sign = (Sign) block.getState();
		org.bukkit.material.Sign signData = (org.bukkit.material.Sign) sign.getData();
		this.facing = signData.isWallSign() ? signData.getAttachedFace() : signData.getFacing();
		this.wallSign = signData.isWallSign();
		this.lines = sign.getLines();
	}

	@Override
	public String getStringData() {
		if (data == null) {
			return Util.join(Arrays.asList(lines), " | ");
		}
		return data;
	}

	@Override
	public String getSqlData() {
		if (data != null) {
			return data;
		}

		Encoder encoder = Base64.getEncoder();
		List<String> encoded = new ArrayList<>();
		for (int i = 0; i < 4; i++) {
			if (lines[i] != null && lines[i].length() > 0) {
				encoded.add(encoder.encodeToString(lines[i].getBytes()));
			}
		}
		return wallSign + "@" + facing + "@" + Util.join(encoded, ",");
	}

	@Override
	public boolean rollback(Block block) {

		// If it is a sign place
		if (type == DataType.SIGN_PLACE) {
			block.setType(Material.AIR);
			return true;
		}

		// if it is a sign break
		if (wallSign) {
			block.setType(Material.WALL_SIGN);
		} else {
			block.setType(Material.SIGN_POST);
		}
		Sign sign = (Sign) (block.getState());
		for (int i = 0; i < lines.length; i++) {
			if (lines[i] != null) {
				sign.setLine(i, lines[i]);
			}
		}
		if (wallSign) {
			((org.bukkit.material.Sign) sign.getData()).setFacingDirection(facing.getOppositeFace());
		} else {
			((org.bukkit.material.Sign) sign.getData()).setFacingDirection(facing);
		}
		sign.update();

		return true;
	}

	@Override
	public boolean rollbackPlayer(Block block, Player player) {
		// If it is a sign place
		if (type == DataType.SIGN_PLACE) {
			player.sendBlockChange(block.getLocation(), 0, (byte) 0);
		} else {
			player.sendBlockChange(block.getLocation(), wallSign ? Material.WALL_SIGN : Material.SIGN_POST, (byte) 0);
		}

		return true;
	}

	@Override
	public boolean rebuild(Block block) {

		if (type == DataType.SIGN_BREAK) {
			block.setType(Material.AIR);
			return true;
		}

		if (wallSign) {
			block.setType(Material.WALL_SIGN);
		} else {
			block.setType(Material.SIGN_POST);
		}
		Sign sign = (Sign) (block.getState());
		for (int i = 0; i < lines.length; i++) {
			if (lines[i] != null) {
				sign.setLine(i, lines[i]);
			}
		}
		if (wallSign) {
			((org.bukkit.material.Sign) sign.getData()).setFacingDirection(facing.getOppositeFace());
		} else {
			((org.bukkit.material.Sign) sign.getData()).setFacingDirection(facing);
		}
		sign.update();
		return true;
	}

	@Override
	public void interpretSqlData(String data) {

		if (data.indexOf("@") == -1) {
			return;
		}

		String[] arr = data.split("@");
		// Parse wall sign or not
		wallSign = arr[0].equals("true");

		// Parse sign direction
		for (BlockFace face : BlockFace.values()) {
			if (face.toString().equalsIgnoreCase(arr[1])) {
				facing = face;
			}
		}

		// Parse lines
		if (arr.length != 3) {
			return;
		}
		Decoder decoder = Base64.getDecoder();
		List<String> decoded = new ArrayList<>();
		String[] encLines = arr[2].split(",");
		for (int i = 0; i < encLines.length; i++) {
			try {
				decoded.add(new String(decoder.decode(encLines[i])));
			} catch (IllegalArgumentException e) {
				Util.severe("Unable to decode sign data from database");
			}
		}
		lines = decoded.toArray(new String[0]);

	}

}
