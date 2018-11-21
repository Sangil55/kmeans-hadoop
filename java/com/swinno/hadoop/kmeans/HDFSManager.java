package com.swinno.hadoop.kmeans;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class HDFSManager extends FileSystem {
	private Configuration mconf;
	private FileSystem fs;
	public HDFSManager() {
		// TODO Auto-generated constructor stub
	}
	
	/*usage
	Configuration conf = new Configuration();
	HDFSManager hdfsmanager = new HDFSManager(conf ,HDFSManager.get(URI.create(confpath),conf) );
	*/
	public HDFSManager(Configuration conf , FileSystem fs) {
		// TODO Auto-generated constructor stub
		mconf = conf;
		this.fs = fs;
	}
	public FileSystem getFileSystem()
	{
		return fs;
	}
	public Configuration getConfiguration()
	{
		return mconf;
	}

	@Override
	public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FSDataOutputStream create(Path arg0, FsPermission arg1, boolean arg2, int arg3, short arg4, long arg5,
			Progressable arg6) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean delete(Path arg0, boolean arg1) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public FileStatus getFileStatus(Path arg0) throws IOException {
		// TODO Auto-generated method stub
		//return super.getFileSt
		return null;
	}

	@Override
	public URI getUri() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path getWorkingDirectory() {
		// TODO Auto-generated method stub
		return super.getInitialWorkingDirectory();
		//return null;
	}

	@Override
	public FileStatus[] listStatus(Path arg0) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public FSDataInputStream open(Path arg0, int arg1) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
	public FSDataInputStream open(String strpath) throws IOException {
		// TODO Auto-generated method stub
		Path hdfsreadpath = new Path(strpath);
		return fs.open(hdfsreadpath);
		//return null;
	}

	@Override
	public boolean rename(Path arg0, Path arg1) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setWorkingDirectory(Path arg0) {
		// TODO Auto-generated method stub

	}
	public boolean exists(String path)
	{
		Path hdfsreadpath = new Path(path);
		try {
			return fs.exists(hdfsreadpath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
}
