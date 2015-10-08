package org.cg.hbase.kiji.dao;

import java.util.List;

/**
 * A generic interface that support CRUD for the entity
 * @author WZ
 *
 * @param <Entity> the entity type
 */
public interface GenericDao<Entity> {
	/**
	 * get all entities
	 * 
	 * @return a list of entities
	 * @throws KijiException
	 */
	List<Entity> getAll();
	
	/**
	 * get with a list of entity ids
	 * 
	 * @return a list of entity id string
	 * @throws KijiException
	 */
	List<Entity> bulkGet(List<String> entityIdStrs);

	/**
	 * get entity by entityIdComponents
	 * 
	 * @param entityIdComponents 
	 * the entity id components in the SAME ORDER as the schema
	 * @return the entity
	 * @throws KijiException
	 */
	Entity get(Object... entityIdComponents);

	/**
	 * scan the entities with idComponents
	 * 
	 * @param entityIdComponents the entity id components in the SAME ORDER as the schema
	 * <p> (Pass IdComponentType as a placeholder for the components to scan on)</p>
	 * <p> (Now also support IdComponentScanRange to scan on a specific id component. 
	 * 	   See AttemptSessionTimePreferredKijiDao.getAttemptsBySessionPeriod for instance)
	 * <p><b>Example:</b></p>  
	 * 		<p>getLearnerAbility by learnerId and categoryId:</p>
	 * 		<pre>scanWithIdComponent(learnerId, IdComponentType.Long, IdComponentType.Integer, categoryId);</pre>
	 * 
	 * @return a list of entities
	 * @throws KijiException
	 */
	List<Entity> scanWithIdComponent(Object... scanComponents);
	
	/**
	 * See scanWithIdComponent
	 * 
	 * @param limit the limit of the rows
	 * @param entityIdComponents
	 * 
	 * @return
	 * @throws KijiException
	 */
	List<Entity> scanWithIdComponentWithLimit(int limit, Object... scanComponents);
	
	/**
	 * save(create/update) the entity
	 * @param theEntity the entity to save
	 * @throws KijiException
	 */
	void save(Entity theEntity);
	
	/**
	 * initialize a buffered writer
	 * @param bufferSize the buffer size (-1 indicates manual flush)
	 * @throws KijiException
	 */
	void initBufferedWriter(long bufferSize);
	
	/**
	 * buffered save(create/update) the entity
	 * @param theEntity the entity to save
	 * @throws KijiException
	 */
	void bufferedSave(Entity entityObject);
	
	/**
	 * manually flush to ensure all writes are committed.
	 * @throws KijiException
	 */
	void flushBufferedWriter();

	/**
	 * delete the entity
	 * @param theEntity the entity to delete
	 * @throws KijiException
	 */
	void delete(Entity theEntity);
	
	/**
	 * true if is buffered writing
	 * @return
	 */
	boolean isBufferedWriting();
}
