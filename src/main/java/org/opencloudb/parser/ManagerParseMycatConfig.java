package org.opencloudb.parser;

/**
 * 
 * @author CrazyPig
 * @since 2017-02-21
 *
 */
public final class ManagerParseMycatConfig {
	
	public static final int OTHER = -1;
	public static final int LIST = 1;
	public static final int CREATE = 2;
	public static final int DROP = 3;
	public static final int ALTER = 4;
	public static final int SET = 5;
	public static final int SHOW = 6;
	public static final int ROLLBACK = 7;
	public static final int BACKUP = 8;
	
	public static int parse(String stmt, int offset) {
		for(int i = offset + 1, len = stmt.length(); i < len; i++) {
			switch(stmt.charAt(i)) {
				case ' ':
				case '\t':
				case '\r':
				case '\n':
					continue;
				case 'A':
				case 'a':
					return alterCheck(stmt, i);
				case 'L':
				case 'l':
					return listCheck(stmt, i);
				case 'C':
				case 'c':
					return createCheck(stmt, i);
				case 'D':
				case 'd':
					return dropCheck(stmt, i);
				case 'S':
				case 's':
					return sCheck(stmt, i);
				case 'R':
				case 'r':
					return rollbackCheck(stmt, i);
				case 'B':
				case 'b':
					return backupCheck(stmt, i);
				default:
					break;
			}
		}
		return OTHER;
	}
	
	private static int alterCheck(String stmt, int offset) {
		if(stmt.length() > "LTER ".length() + offset) {
			char c1 = stmt.charAt(++offset); // L
			char c2 = stmt.charAt(++offset); // T
			char c3 = stmt.charAt(++offset); // E
			char c4 = stmt.charAt(++offset); // R
			char c5 = stmt.charAt(++offset);
			if((c1 == 'L' || c1 == 'l') && (c2 == 'T' || c2 == 't')
					&& (c3 == 'E' || c3 == 'e') && (c4 == 'R' || c4 == 'r')
					&& (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
				return ALTER;
			}
		}
		return OTHER;
	}
	
	private static int listCheck(String stmt, int offset) {
		if(stmt.length() > "IST ".length() + offset) {
			char c1 = stmt.charAt(++offset); // I
			char c2 = stmt.charAt(++offset); // S
			char c3 = stmt.charAt(++offset); // T
			char c4 = stmt.charAt(++offset);
			if((c1 == 'I' || c1 == 'i')
					&& (c2 == 'S' || c2 == 's') && (c3 == 'T' || c3 == 't')
					&& (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
				return LIST;
			}
		}
		return OTHER;
	}
	
	private static int createCheck(String stmt, int offset) {
		if(stmt.length() > offset + "REATE ".length()) {
			char c1 = stmt.charAt(++offset); // R
			char c2 = stmt.charAt(++offset); // E
			char c3 = stmt.charAt(++offset); // A
			char c4 = stmt.charAt(++offset); // T
			char c5 = stmt.charAt(++offset); // E
			char c6 = stmt.charAt(++offset);
			if((c1 == 'R' || c1 == 'r') && (c2 == 'E' || c2 == 'e')
					&& (c3 == 'A' || c3 == 'a') && (c4 == 'T' || c4 == 't')
					&& (c5 == 'E' || c5 == 'e')
					&& (c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {
				return CREATE;
			}
		}
		return OTHER;
	}
	
	private static int dropCheck(String stmt, int offset) {
		if(stmt.length() > offset + "ROP ".length()) {
			char c1 = stmt.charAt(++offset); // R
			char c2 = stmt.charAt(++offset); // O
			char c3 = stmt.charAt(++offset); // P
			char c4 = stmt.charAt(++offset);
			if((c1 == 'R' || c1 == 'r') && (c2 == 'O' || c2 == 'o')
					&& (c3 == 'P' || c3 == 'p')
					&& (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
				return DROP;
			}
		}
		return OTHER;
	}
	
	private static int rollbackCheck(String stmt, int offset) {
		if(stmt.length() > offset + "OLLBACK ".length()) {
			char c1 = stmt.charAt(++offset); // O
			char c2 = stmt.charAt(++offset); // L
			char c3 = stmt.charAt(++offset); // L
			char c4 = stmt.charAt(++offset); // B
			char c5 = stmt.charAt(++offset); // A
			char c6 = stmt.charAt(++offset); // C
			char c7 = stmt.charAt(++offset); // K
			char c8 = stmt.charAt(++offset);
			if((c1 == 'O' || c1 == 'o') && (c2 == 'L' || c2 == 'l')
					&& (c3 == 'L' || c3 == 'l') && (c4 == 'B' || c4 == 'b')
					&& (c5 == 'A' || c5 == 'a') && (c6 == 'C' || c6 == 'c')
					&& (c7 == 'K' || c7 == 'k')
					&& (c8 == ' ' || c8 == '\t' || c8 == '\r' || c8 == '\n')) {
				return ROLLBACK;
			}
		}
		return OTHER;
	}
	
	private static int backupCheck(String stmt, int offset) {
		if(stmt.length() > offset + "ACKUP ".length()) {
			char c1 = stmt.charAt(++offset); // A
			char c2 = stmt.charAt(++offset); // C
			char c3 = stmt.charAt(++offset); // K
			char c4 = stmt.charAt(++offset); // U
			char c5 = stmt.charAt(++offset); // P
			char c6 = stmt.charAt(++offset);
			if((c1 == 'A' || c1 == 'a') && (c2 == 'C' || c2 == 'c')
					&& (c3 == 'K' || c3 == 'k') && (c4 == 'U' || c4 == 'u')
					&& (c5 == 'P' || c5 == 'p') 
					&& (c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {
				return BACKUP;
			}
		} 
		
		else if (stmt.length() == offset + "ACKUP ".length()) {
			char c1 = stmt.charAt(++offset); // A
			char c2 = stmt.charAt(++offset); // C
			char c3 = stmt.charAt(++offset); // K
			char c4 = stmt.charAt(++offset); // U
			char c5 = stmt.charAt(++offset); // P
			if((c1 == 'A' || c1 == 'a') && (c2 == 'C' || c2 == 'c')
					&& (c3 == 'K' || c3 == 'k') && (c4 == 'U' || c4 == 'u')
					&& (c5 == 'P' || c5 == 'p')) {
				return BACKUP;
			}
		}
		return OTHER;
	}
	
	/**
	 * SET或SHOW
	 * @param stmt
	 * @param offset
	 * @return
	 */
	private static int sCheck(String stmt, int offset) {
		if(stmt.length() > offset + 1) {
			switch (stmt.charAt(++offset)) {
			case 'E':
			case 'e':
				return seCheck(stmt, offset);
			case 'H':
			case 'h':
				return shCheck(stmt, offset);
			default:
				break;
			}
		}
		
		return OTHER;
	}
	
	private static int seCheck(String stmt, int offset) {
		if(stmt.length() > offset + "T ".length()) {
			char c1 = stmt.charAt(++offset); // T
			char c2 = stmt.charAt(++offset);
			
			if((c1 == 'T' || c1 == 't')
					&& (c2 == ' ' || c2 == '\t' || c2 == '\r' || c2 == '\n')) {
				return SET;
			}
		}
		return OTHER;
	}
	
	private static int shCheck(String stmt, int offset) {
		if(stmt.length() > offset + "OW ".length()) {
			char c1 = stmt.charAt(++offset); // O
			char c2 = stmt.charAt(++offset); // W
			char c3 = stmt.charAt(++offset);
			
			if((c1 == 'O' || c1 == 'o') && (c2 == 'W' || c2 == 'w')
					&& (c3 == ' ' || c3 == '\t' || c3 == '\r' || c3 == '\n')) {
				return SHOW;
			}
		}
		return OTHER;
	}

}
