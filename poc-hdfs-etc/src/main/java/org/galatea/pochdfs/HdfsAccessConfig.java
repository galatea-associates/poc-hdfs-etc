package org.galatea.pochdfs;

import org.apache.hadoop.fs.FileSystem;
import org.galatea.pochdfs.utils.hdfs.FileSystemFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "org.galatea.pochdfs")
public class HdfsAccessConfig {

	// private FileSystemFactory factory;

//	@Bean
//	public IHdfsWriter IhdfsWriter() {
//		return new HdfsWriter(FileSystemFactory.newDefaultFileSystem());
//	}

	@Bean
	public FileSystem fileSystem() {
		return FileSystemFactory.newDefaultFileSystem();
	}

}
