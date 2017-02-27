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
	public static final int SET = 4;
	
	public static int parse(String stmt, int offset) {
		for(int i = offset + 1, len = stmt.length(); i < len; i++) {
			switch(stmt.charAt(i)) {
				case ' ':
				case '\t':
				case '\r':
				case '\n':
					continue;
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
					return setCheck(stmt, i);
				default:
					break;
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
	
	private static int setCheck(String stmt, int offset) {
		if(stmt.length() > offset + "ET ".length()) {
			char c1 = stmt.charAt(++offset); // E
			char c2 = stmt.charAt(++offset); // T
			char c3 = stmt.charAt(++offset); 
			
			if((c1 == 'E' || c1 == 'e') && (c2 == 'T' || c2 == 't')
					&& (c3 == ' ' || c3 == '\t' || c3 == '\r' || c3 == '\n')) {
				return SET;
			}
		}
		return OTHER;
	}

}
