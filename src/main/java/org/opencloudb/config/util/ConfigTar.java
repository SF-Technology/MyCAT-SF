package org.opencloudb.config.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.IOUtils;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarInputStream;
import org.kamranzafar.jtar.TarOutputStream;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.model.SystemConfig;

/**
 * 将配置文件打包成tar包
 * 
 * @author 01140003
 * @version 2017年5月11日 下午9:30:09
 */
public class ConfigTar {
	private static final int BUFFER = 2048;
	private static final int BACKUP_SIZE = 10; // 保留的备份数量

	private static File confBak; // 备份的配置文件压缩包会保存到这个目录下
	private static File classPath; // classpath
	private static File[] confFiles; // 需要备份的配置文件或文件夹列表
	private static BackupFileMap backupFileMap; // 备份文件
	
	private static final ReentrantLock lock = new ReentrantLock();

	static {
		MycatConfig mycatConf = MycatServer.getInstance().getConfig();
		int backupSize = mycatConf.getSystem().getConfBackupSize();
		
		confBak = new File(SystemConfig.getHomePath(), SystemConfig.getConfBak());

		classPath = new File(SystemConfig.class.getClassLoader().getResource("").getPath());

		confFiles = new File[] { 
				new File(classPath, "server.xml"), 
				new File(classPath, "schema.xml"),
				new File(classPath, "database.xml"), 
				new File(classPath, "rule.xml"), 
				new File(classPath, "user.xml"),
				new File(classPath, "dnindex.properties"), 
				new File(classPath, SystemConfig.getMapFileFolder()) 
				};

		backupFileMap = new BackupFileMap(backupSize);
	}
	
	/**
	 * @param info
	 *            备份信息，对应备份时发生的操作
	 * @throws Exception
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void tarConfig(String info) throws Exception {
		lock.lock();
		try {
			// Output file stream
			File tarFile = new File(confBak, tarFileName(info));
			FileOutputStream dest = new FileOutputStream(tarFile);

			// Create a TarOutputStream
			TarOutputStream out = new TarOutputStream(new BufferedOutputStream(dest));
			try {
				tar(classPath, out); // 备份配置文件
			} catch (Exception e) {
				throw e;
			} finally {
				if (out != null) {
					out.close();
				}
			}

			backupFileMap.addFile(tarFile);
		} catch (Exception e) {
			throw e;
		} finally {
			lock.unlock();
		}
		
	}

    /**
     * 根据指定id找到相应的备份文件，然后覆盖到classpath下
     *
     * @param index 备份文件的下标
     * @return
     * @throws IOException
     */
    public static boolean untarConfig(int index) throws IOException {
        lock.lock();
        TarInputStream tis = null;
        try {
            if (!backupFileMap.containsIndex(index)) {
                return false;
            }

            tis = new TarInputStream(
                    new BufferedInputStream(new FileInputStream(backupFileMap.acquireFile(index).getFile()))); // 取出最近一次备份

            deleteMapfiles(); // 删除mapfile
            untar(tis, classPath); // 将备份解压到classpath下
            return true;
        } catch (Exception e) {
            throw e;
        } finally {
            if (tis != null) {
                tis.close();
            }
            lock.unlock();
        }
    }
	
	

	/**
	 * 将指定的配置文件打包成tar包
	 * 
	 * @param folder
	 * @param out
	 * @throws IOException
	 */
	private static void tar(File folder, TarOutputStream out) throws IOException {
		for (File file : confFiles) {
			if (file.exists()) {
				tar(null, file, out);
			}
		}
	}

    /**
     * 递归地将指定文件或文件夹输出到tar文件的输出流中
     *
     * @param parent 上一级的目录
     * @param file   被压缩的文件或文件夹
     * @param out    tar文件的输出流
     * @throws IOException
     */
    private static void tar(String parent, File file, TarOutputStream out) throws IOException {
        String files[] = file.list();

        // is file
        if (files == null) {
            files = new String[1];
            files[0] = file.getName();
        }

        parent = ((parent == null) ? (file.isFile()) ? "" : file.getName() + "/" : parent + file.getName() + "/");

        if (files == null || files.length == 0) {
            TarEntry entry = new TarEntry(file, parent);
            out.putNextEntry(entry);
        }

        for (int i = 0; i < files.length; i++) {
            File fe = file;
            if (file.isDirectory()) {
                fe = new File(file, files[i]);
            }

            if (fe.isDirectory()) {
                String[] fl = fe.list();
                if (fl != null && fl.length != 0) {
                    tar(parent, fe, out);
                } else {
                    TarEntry entry = new TarEntry(fe, parent + files[i] + "/");
                    out.putNextEntry(entry);
                }
                continue;
            }

            FileInputStream fi = new FileInputStream(fe);
            BufferedInputStream origin = new BufferedInputStream(fi);
            TarEntry entry = new TarEntry(fe, parent + files[i]);
            out.putNextEntry(entry);
            try {
                IOUtils.writeChunked(IOUtils.toByteArray(origin), out);
            } finally {
                out.flush();
                origin.close();
            }

        }
    }

    /**
     * 从指定tar文件的输入流输入到指定的文件夹下
     *
     * @param tis        tar文件的输入流
     * @param destFolder 目标文件夹（最终解压到此文件夹）
     * @throws IOException
     */
    private static void untar(TarInputStream tis, File destFolder) throws IOException {

        TarEntry entry;
        while ((entry = tis.getNextEntry()) != null) {
            if (entry.isDirectory()) {
                new File(destFolder.getAbsolutePath(), entry.getName()).mkdirs();
                continue;
            } else {
                int di = entry.getName().lastIndexOf('/');
                if (di != -1) {
                    new File(destFolder.getAbsolutePath(), entry.getName().substring(0, di)).mkdirs();
                }
            }
            FileOutputStream fos = new FileOutputStream(new File(destFolder.getAbsolutePath(), entry.getName()));
            final BufferedOutputStream dest = new BufferedOutputStream(fos);
            try {
                IOUtils.writeChunked(IOUtils.toByteArray(tis), dest);
            } finally {
                dest.flush();
                dest.close();
            }
        }
    }

	/**
	 * 获得tar文件的文件名
	 * 
	 * @param info
	 * @return
	 * @throws InterruptedException
	 */
	private static String tarFileName(String info) throws InterruptedException {
		long timeStamp = System.currentTimeMillis();

		// 如果当前获得的时间戳已经被用来做某个备份文件的id，则sleep 1ms后再尝试
		while (backupFileMap.containsTimeStamp(timeStamp)) {
			Thread.sleep(1L);
			timeStamp = System.currentTimeMillis();
		}
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddhhmmssSSS");
		String timeStr = simpleDateFormat.format(new Date(timeStamp));

		return info.replace(" ", "_") + '_' + timeStr + ".tar";
	}

	/**
	 * 删除所有的mapfile
	 */
	private static void deleteMapfiles() {
		File mapfileFolder = new File(classPath, SystemConfig.getMapFileFolder());
		deleteFile(mapfileFolder);
	}

	/**
	 * 删除文件或文件夹
	 * 
	 * @param file
	 */
	private static void deleteFile(File file) {
		if (file.isFile()) {
			file.delete();
			return;
		} else {
			File files[] = file.listFiles();
			for (File f : files) {
				deleteFile(f);
			}
			file.delete();
		}
	}

	/**
	 * 用来维护和管理tar备份文件的类
	 * 
	 * @author 01140003
	 * @version 2017年5月17日 下午4:34:05
	 */
	public static class BackupFileMap {
		int size; // 维护的备份数
		private TreeMap<Long, BackupFile> tarFileMap = new TreeMap<Long, BackupFile>();

		public BackupFileMap(int size) {
			this.size = size > 0 ? size : 0;
			reloadFiles();
			deleteExceededFiles();
		}

		/**
		 * 重新读取目录下的备份文件
		 */
		public void reloadFiles() {
			File[] tarFiles = confBak.listFiles();

			for (File file : tarFiles) {
				tarFileMap.put(tarFileTimestamp(file.getName()), new BackupFile(file));
			}
		}

		/**
		 * 删除多余的备份文件
		 */
		public void deleteExceededFiles() {
			while (tarFileMap.size() > size) {
				tarFileMap.pollFirstEntry().getValue().getFile().delete(); // 删除id最小的备份文件
			}
		}

		/**
		 * 根据备份文件的下标获得文件
		 * 
		 * @param index
		 * @return
		 */
		public BackupFile acquireFile(int index) {
			ArrayList<Long> timestampList = new ArrayList<Long>(tarFileMap.descendingKeySet()); // keyset降序排列后转化为list
			long timestamp = timestampList.get(index);
			return tarFileMap.get(timestamp);
		}
		
		/**
		 * 根据下标获得备份文件的时间戳
		 * 
		 * @param index
		 * @return
		 */
		public Long acquireTimestamp (int index) {
			ArrayList<Long> timestampList = new ArrayList<Long>(tarFileMap.descendingKeySet()); // keyset降序排列后转化为list
			return timestampList.get(index);
		}

		/**
		 * 增加一个备份文件
		 * 
		 * @param file
		 * @return
		 */
		public boolean addFile(File file) {
			long timestamp = tarFileTimestamp(file.getName());

			if (tarFileMap.containsKey(timestamp)) { // id重复
				return false;
			}
			
			tarFileMap.put(timestamp, new BackupFile(file));

			deleteExceededFiles(); // 删除多余的备份

			return true;
		}

		/**
		 * 根据tar备份文件名获得文件timestamp
		 * 
		 * @param fileName
		 * @return
		 */
		private long tarFileTimestamp(String fileName) {
			int startInd = fileName.lastIndexOf('_') + 1;
			int endInd = fileName.lastIndexOf(".tar");

			if (startInd < 0 || endInd < 0 || startInd >= endInd) { // 如果tar备份文件名不合法
				throw new RuntimeException("Illegal backup file name.");
			} else {
				return Long.valueOf(fileName.substring(startInd, endInd));
			}
		}

		/**
		 * 判断下标有没有超出范围
		 * 
		 * @param index
		 * @return
		 */
		public boolean containsIndex (int index) {
			return index < tarFileMap.size();
		}
		
		/**
		 * 判断某个时间戳是否已经被使用
		 * 
		 * @param timestamp
		 * @return
		 */
		public boolean containsTimeStamp(Long timestamp) {
			return tarFileMap.containsKey(timestamp);
		}
		
		public TreeMap<Long, BackupFile> getTarFileMap() {
			return tarFileMap;
		}
		
	}
	
	public static class BackupFile{
		private File file; // 备份文件
		private String operation; // 备份时发生的操作s
		
		public BackupFile(File file) {
			this.file = file;
			this.operation = tarFileOperation(file.getName());
		}
		
		/**
		 * 根据tar备份文件名获得备份时发生的操作名称
		 * @param fileName
		 * @return
		 */
		private String tarFileOperation (String fileName) {
			int beginIndex = 0;
			int endIndex = fileName.lastIndexOf('_');
			
			if (beginIndex < 0 || endIndex < 0 || beginIndex >= endIndex) { // 如果tar备份文件名不合法
				throw new RuntimeException("Illegal backup file name.");
			} else {
				String operation = fileName.substring(beginIndex, endIndex).replaceFirst("_", " ").replaceFirst("_", " "); // 将最前面的两个下划线替换成空格
				return operation;
			}
		}
		
		public File getFile() {
			return file;
		}

		public void setFile(File file) {
			this.file = file;
		}

		public String getOperation() {
			return operation;
		}

		public void setOperation(String operation) {
			this.operation = operation;
		}
		
	}

	public static BackupFileMap getBackupFileMap() {
		return backupFileMap;
	}
	

}