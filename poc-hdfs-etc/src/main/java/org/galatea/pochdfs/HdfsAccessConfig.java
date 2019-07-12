package org.galatea.pochdfs;

import org.galatea.pochdfs.utils.hdfs.FileSystemFactory;
import org.galatea.pochdfs.utils.hdfs.HdfsWriter;
import org.galatea.pochdfs.utils.hdfs.IHdfsWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HdfsAccessConfig {

	@Bean
	public IHdfsWriter IhdfsWriter() {
		return new HdfsWriter(FileSystemFactory.newDefaultFileSystem());
	}

}
