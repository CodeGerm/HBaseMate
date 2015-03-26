package org.cg.hbase.kiji.common;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cg.hbase.kiji.exception.KijiException;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

/**
 * @author yingkaihu, WZ
 * 
 */
public class KijiEntitiyManager {
	
	private static volatile KijiEntitiyManager instance;
	
	private volatile Kiji kiji;
	
	protected static final Log log = LogFactory.getLog(KijiEntitiyManager.class);

	protected KijiEntitiyManager () {
		
	}
	
	private KijiEntitiyManager( String kijiInstance, String kijiCluster ) {
		log.info("[KijiEntitiyManager]starting");
		ClassLoader ctxCl = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(Kiji.class.getClassLoader());
		try {
			kiji = Kiji.Factory.open(KijiURI.newBuilder(kijiCluster).withInstanceName(kijiInstance).build()); 
			log.info("[KijiEntitiyManager]connection open: " + kiji.getURI());
		} catch (IOException e) {
			log.error("[KijiEntitiyManager]Fail to open Kiji instance", e);
			throw new KijiException(e);
		} finally {
			Thread.currentThread().setContextClassLoader(ctxCl);
		}
	}

	/**
	 * Get the kiji manager instance
	 * @param kijiInstance
	 * @param kijiCluster
	 * @throws KijiException
	 * @return
	 */
	public static KijiEntitiyManager getInstance( String kijiInstance, String kijiCluster ){
		if(instance!=null){
			return instance;
		}
		synchronized (KijiEntitiyManager.class) {
			if(instance!=null) return instance;
			try {
				instance = new KijiEntitiyManager( kijiInstance, kijiCluster );
			}catch(Exception e){
				throw new KijiException(e);
			}
		}
		return instance;	
	}

	public void cleanUp() throws Exception {
		ResourceUtils.releaseOrLog(kiji);
		log.info("[cleanUp]Kiji connection closed");	
	}

	public Kiji getKiji() {
		return kiji;
	}
}
