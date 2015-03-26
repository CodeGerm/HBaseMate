package org.cg.hbase.kiji.exception;

/**
 * 
 * @author WZ
 *
 */
public class KijiException extends RuntimeException {

	private static final long serialVersionUID = 5147356148461133922L;

	/**
	 * Constructor
	 */
	public KijiException() {
		super( new Throwable("Internal Error.") );
	}

	/**
	 * Constructor
	 * @param throwable The cause 
	 */
	public KijiException(Throwable throwable) {
		super( throwable );
	}
	
	/**
	 * Constructor
	 * @param msg The error message
	 */
	public KijiException( String msg ) {
		super( new Throwable(msg) );
	}
}
