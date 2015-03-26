package org.cg.hbase.kiji.dao;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cg.hbase.kiji.common.KijiEntitiyManager;
import org.cg.hbase.kiji.exception.KijiException;
import org.cg.hbase.kiji.schema.EntityIdComponentEqualsRowFilter;
import org.cg.hbase.kiji.schema.IdComponentScanRange;
import org.cg.hbase.kiji.schema.IdComponentType;
import org.cg.hbase.kiji.schema.annotation.Column;
import org.cg.hbase.kiji.schema.annotation.Family;
import org.cg.hbase.kiji.schema.annotation.IdComponent;
import org.cg.hbase.kiji.schema.annotation.KijiEntity;
import org.cg.hbase.kiji.schema.annotation.Translator;
import org.cg.hbase.kiji.translator.KijiDataTranslator;
import org.joda.time.DateTime;
import org.kiji.schema.EntityId;
import org.kiji.schema.HBaseEntityId;
import org.kiji.schema.KijiBufferedWriter;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.util.ResourceUtils;
import org.springframework.util.ReflectionUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * A generic kiji implementation for all kiji Data Access Object
 * @author WZ, Liang
 *
 * @param <Entity> the entity type
 */
public abstract class GenericKijiDao<Entity> implements GenericDao<Entity> {

	private static final Log LOG = LogFactory.getLog(GenericKijiDao.class);

	private final Class<Entity> entity;

	private final KijiEntity entityInfo;

	private KijiEntitiyManager kijiManager;

	private static final int UNLIMITED_ROWS = -1;
	
	private final Set<String> familySet;
	
	private KijiBufferedWriter bufferedWriter = null;
	
	private KijiTable bufferedWriterKijiTable = null;
	
	private int bufferedWriterCount = 0;

	/**
	 * Constructor
	 * @param sessionFactory
	 * @param entity
	 */
	public GenericKijiDao(Class<Entity> entity) {
		Preconditions.checkNotNull(entity, "The entity must be provided");
		Preconditions.checkArgument(entity.isAnnotationPresent(KijiEntity.class), 
				"The entity must be a Kiji Entity");

		this.entity = entity;
		this.entityInfo = entity.getAnnotation(KijiEntity.class);
		Preconditions.checkArgument(!Strings.isNullOrEmpty(this.entityInfo.name()), 
				"The entity table name must be specified");

		//Initialize family set
		this.familySet = new HashSet<String>();
		// Add family specified in KijiEntity
		if ( !Strings.isNullOrEmpty(entityInfo.family()) ) {
			familySet.add(entityInfo.family());
		}
		// Add family for each field if specified
		for (Field field : entity.getDeclaredFields()) {
			Column column = field.getAnnotation(Column.class);
			Family family = field.getAnnotation(Family.class);
			if ( column != null && !Strings.isNullOrEmpty(column.family()) ) {
				familySet.add(column.family());
			} else if ( family!=null && !Strings.isNullOrEmpty(family.name()) ) {
				familySet.add(family.name());
			}
		}
	}

	/**
	 * inti
	 * @param theKijiManager
	 */
	public abstract void init (KijiEntitiyManager theKijiManager);
	
	/**
	 * get kiji manager
	 * @return kijiManager
	 */
	public KijiEntitiyManager getKijiManager() {
		return kijiManager;
	}

	/**
	 * set kiji manager
	 * @param kijiManager
	 */
	public void setKijiManager(KijiEntitiyManager kijiManager) {
		Preconditions.checkState(kijiManager != null, "The kijiManager must be provided");
		this.kijiManager = kijiManager;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<Entity> getAll(){
		List<Entity> result = new ArrayList<Entity>();
		KijiTable table = null;
		KijiTableReader reader = null;
		KijiRowScanner rowScanner = null;
		try {
			table = this.getKijiManager().getKiji().openTable(entityInfo.name());
			reader = table.openTableReader();
			
			ColumnsDef columnsDef = this.getColumnsDefFromEntityClass();
			KijiDataRequest dataRequest = KijiDataRequest.builder().
					addColumns(columnsDef).build();
			rowScanner = reader.getScanner(dataRequest); 

			for (KijiRowData rowData: rowScanner) {
				result.add(this.parseRowData(rowData));
			}
		} catch (KijiTableNotFoundException e) {
			throw new KijiException(e);
		} catch (IOException e) {
			throw new KijiException(e);
		} catch (InstantiationException e) {
			throw new KijiException(e);
		} catch (IllegalAccessException e) {
			throw new KijiException(e);
		} finally {
			ResourceUtils.closeOrLog(rowScanner);
			ResourceUtils.closeOrLog(reader);
			ResourceUtils.releaseOrLog(table);
		}

		if (result.isEmpty()) {
			LOG.info(String.format("[getAll]no resource found for %s.", 
					entity.getName() ));	
		}
		return result;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<Entity> bulkGet(List<String> entityIdStrs) {
		Preconditions.checkArgument(entityIdStrs!=null && !entityIdStrs.isEmpty(), "The entityIds must be provided");
		
		List<Entity> result = new ArrayList<Entity>(entityIdStrs.size());
		
		// parse entity ids
		List<EntityId> entityIds = new ArrayList<EntityId>(entityIdStrs.size());
		for (String idStr : entityIdStrs) {
			byte[] rowKeyBytes = Base64.decodeBase64(idStr);
			entityIds.add(HBaseEntityId.fromHBaseRowKey(rowKeyBytes));
		}
		
		KijiTable table = null;
		KijiTableReader reader = null;
		try {
			// Open kiji table reader
			table=this.getKijiManager().getKiji().openTable(entityInfo.name());
			reader = table.openTableReader();
			ColumnsDef columnsDef = this.getColumnsDefFromEntityClass();
			
			// Read row
			KijiDataRequest dataRequest = KijiDataRequest.builder().
					addColumns(columnsDef).build();
			List<KijiRowData> rowData = reader.bulkGet(entityIds, dataRequest);
			for (KijiRowData row : rowData) {
				if ( !isRowDataEmpty(row) ) {
					result.add(this.parseRowData(row));
				} else {
					LOG.error(String.format("[bulkGet]found empty row data: %s. stop here.", row));
					break;
				}
			}

		} catch (KijiTableNotFoundException e) {
			throw new KijiException(e);
		} catch (IOException e) {
			throw new KijiException(e);
		} catch (InstantiationException e) {
			throw new KijiException(e);
		} catch (IllegalAccessException e) {
			throw new KijiException(e);
		} finally {
			ResourceUtils.closeOrLog(reader);
			ResourceUtils.releaseOrLog(table);
		}
		
		if (result.isEmpty()) {
			LOG.info(String.format("[bulkGet]no resource found for %s.", entity.getName() ));	
		}
		return result;
	}
	
	private boolean isRowDataEmpty(KijiRowData rowData) {
		//empty KijiRowData = containsColumn() false for all columns
		for ( Field field : entity.getDeclaredFields() ) {
			Column column = field.getAnnotation(Column.class);
			Family family = field.getAnnotation(Family.class);
			if (column != null) {
				if (rowData.containsColumn( 
					Strings.isNullOrEmpty(column.family()) ? entityInfo.family() : column.family(),
					Strings.isNullOrEmpty(column.name()) ? field.getName() : column.name()) ) {
					return false;
				}
					
			} else if ( family!=null && !Strings.isNullOrEmpty(family.name()) ) {
				if (rowData.containsColumn( family.name()) ) {
					return false;
				}
			}
		}
		return true;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Entity get(Object... entityIdComponents) {
		Preconditions.checkArgument(entityIdComponents!=null && entityIdComponents.length != 0,
				"The entityIdComponents must be provided");

		Entity theEntity = null;
		KijiTable table = null;
		KijiTableReader reader = null;
		try {
			// Open kiji table reader
			table=this.getKijiManager().getKiji().openTable(entityInfo.name());
			reader = table.openTableReader();

			// Create entity id
			EntityId entityId = table.getEntityId(entityIdComponents);
			ColumnsDef columnsDef = this.getColumnsDefFromEntityClass();

			// Read row
			KijiDataRequest dataRequest = KijiDataRequest.builder().
					addColumns(columnsDef).build();
			KijiRowData rowData = reader.get(entityId, dataRequest);
			if ( !isRowDataEmpty(rowData) ) {
				theEntity = this.parseRowData(rowData);
			} else {
				LOG.error(String.format("[get]found empty row data: %s", rowData));
			}

			String logMsg;
			if ( theEntity == null ) {
				logMsg = String.format("[get]no enity %s found with entity id : %s.", 
						entity.getName(), entityId.toShellString());
			} else {
				logMsg = String.format("[get]enity %s found with entity id : %s.", 
						entity.getName(), entityId.toShellString());
			}
			LOG.info(logMsg);

		} catch (KijiTableNotFoundException e) {
			throw new KijiException(e);
		} catch (IOException e) {
			throw new KijiException(e);
		} catch (InstantiationException e) {
			throw new KijiException(e);
		} catch (IllegalAccessException e) {
			throw new KijiException(e);
		} finally {
			ResourceUtils.closeOrLog(reader);
			ResourceUtils.releaseOrLog(table);
		}
		return theEntity;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<Entity> scanWithIdComponentWithLimit (int limit, Object... scanComponents) {
		Preconditions.checkArgument(scanComponents!=null && scanComponents.length != 0,
				"The entityIdComponents must be provided");

		List<Entity> result = new ArrayList<Entity>();
		KijiTable table = null;
		KijiTableReader reader = null;
		KijiRowScanner rowScanner = null;
		EntityIdComponentEqualsRowFilter entityIdRowFilter = null;
		try {
			table = this.getKijiManager().getKiji().openTable(entityInfo.name());
			reader = table.openTableReader();
			ColumnsDef columnsDef = this.getColumnsDefFromEntityClass();
			KijiDataRequest dataRequest = KijiDataRequest.builder().
					addColumns(columnsDef).build();

			List<Object> startRow = new ArrayList<Object>();
			List<Object> stopRow = new ArrayList<Object>();
			KijiRowFilter rowFilter = null;
			for ( Object scanComponent : scanComponents ) {
				if ( scanComponent.equals(IdComponentType.Long) ) {
					startRow.add(Long.MIN_VALUE);
					stopRow.add(Long.MAX_VALUE);
				} else if ( scanComponent.equals(IdComponentType.Integer) ) {
					startRow.add(Integer.MIN_VALUE);
					stopRow.add(Integer.MAX_VALUE);
				} else if (scanComponent.equals(IdComponentType.String) ) {
					startRow.add("");
					stopRow.add("~");
				} else if ( scanComponent instanceof IdComponentScanRange ) {
					startRow.add( ((IdComponentScanRange<?>)scanComponent).getStart() );
					stopRow.add( ((IdComponentScanRange<?>)scanComponent).getEnd() );
				} else if ( scanComponent instanceof KijiRowFilter )	{
					rowFilter = (KijiRowFilter)scanComponent;
				} else if ( scanComponent instanceof EntityIdComponentEqualsRowFilter ) {
					entityIdRowFilter = (EntityIdComponentEqualsRowFilter)scanComponent;
				} else {
					startRow.add(scanComponent);
					stopRow.add(scanComponent);
				}
			}

			EntityId startId= table.getEntityId(startRow.toArray());
			EntityId stopId = table.getEntityId(stopRow.toArray());

			KijiScannerOptions options = new KijiScannerOptions()
											.setStartRow(startId)
											.setStopRow(stopId);
			if (rowFilter!=null) {
				options.setKijiRowFilter(rowFilter);
			}
			rowScanner = reader.getScanner(dataRequest, options); 
			
			Iterator<KijiRowData> it = rowScanner.iterator();
			int numRows = 0;
			while ( it.hasNext() && (numRows < limit || limit == UNLIMITED_ROWS) ) {
				KijiRowData rowData = it.next();
				Entity e = this.parseRowDataWithRowFilter(rowData, entityIdRowFilter);
				if (e!=null) {
					result.add(e);
					numRows++;
				}
			}
		} catch (KijiTableNotFoundException e) {
			throw new KijiException(e);
		} catch (IOException e) {
			throw new KijiException(e);
		} catch (InstantiationException e) {
			throw new KijiException(e);
		} catch (IllegalAccessException e) {
			throw new KijiException(e);
		} finally {
			ResourceUtils.closeOrLog(rowScanner);
			ResourceUtils.closeOrLog(reader);
			ResourceUtils.releaseOrLog(table);
		}

		if (result.isEmpty()) {
			LOG.info(String.format("[scanWithIdComponent]no resource found for %s.", 
					entity.getName() ));		
		}
		return result;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<Entity> scanWithIdComponent ( Object... scanComponents) {
		return scanWithIdComponentWithLimit( UNLIMITED_ROWS, scanComponents );
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void delete(Entity entityObject) {
		Preconditions.checkNotNull(entityObject, "The entity must be provided");

		KijiTable table = null;
		KijiTableWriter writer = null;
		try {
			table = this.getKijiManager().getKiji().openTable(entityInfo.name());
			writer = table.openTableWriter();
			Object[] idComponents = this.getEntityIdComponentsFromEntityClass(entityObject);
			EntityId entityId = table.getEntityId(idComponents);
			writer.deleteRow(entityId);

			LOG.info(String.format("[delete]Entity %s with entity id %s deleted.", 
					entity.getName(), entityId.toShellString()));
		
		} catch (KijiTableNotFoundException e) {
			throw new KijiException(e);
		} catch (IOException e) {
			throw new KijiException(e);
		} catch (IllegalAccessException e) {
			throw new KijiException(e);
		} catch (IllegalArgumentException e) {
			throw new KijiException(e);
		} catch (InstantiationException e) {
			throw new KijiException(e);
		} finally {
			ResourceUtils.closeOrLog(writer);
			ResourceUtils.releaseOrLog(table);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void save(Entity entityObject) {
		Preconditions.checkNotNull(entityObject, "The entity must be provided");

		KijiTable table = null;
		KijiTableWriter writer = null;
		try {
			table = this.getKijiManager().getKiji().openTable(entityInfo.name());
			writer = table.openTableWriter();
			Object[] idComponents = this.getEntityIdComponentsFromEntityClass(entityObject);
			EntityId entityId = table.getEntityId(idComponents);

			for (Field field : entity.getDeclaredFields()) {
				Column column = field.getAnnotation(Column.class);
				Family family = field.getAnnotation(Family.class);
				if ( column != null ) {
					ReflectionUtils.makeAccessible(field);
					Object value = field.get(entityObject);
					Translator translatorAnnotaion = field.getAnnotation(Translator.class);
					if ( translatorAnnotaion!=null ) {
						Class<?> translatorClass = translatorAnnotaion.using();
						value = this.translateToKijiData(translatorClass, value);
					}
					writer.put(
							entityId, 
							Strings.isNullOrEmpty(column.family()) ? entityInfo.family() :column.family(),
							Strings.isNullOrEmpty(column.name()) ? field.getName() : column.name(), 
							value
					);
				} else if ( family != null && !Strings.isNullOrEmpty(family.name()) ) {
					ReflectionUtils.makeAccessible(field);
					Map<String, ?> valueMap = (Map<String, ?>) field.get(entityObject);
					for ( Entry<String, ?> e : valueMap.entrySet() ) {
						if (e.getValue()!= null && e.getKey()!= null ) {
							writer.put(
									entityId, 
									family.name(),
									e.getKey(), 
									e.getValue()
									);
						}
					}
				}
			}
			LOG.info(String.format("[save]Entity %s with entity id %s saved.", 
					entity.getName(), entityId.toShellString()));

		} catch (KijiTableNotFoundException e) {
			throw new KijiException(e);
		} catch (InstantiationException e) {
			throw new KijiException(e);
		} catch (IOException e) {
			throw new KijiException(e);
		} catch (IllegalAccessException e) {
			throw new KijiException(e);
		} finally {
			ResourceUtils.closeOrLog(writer);
			ResourceUtils.releaseOrLog(table);
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initBufferedWriter(long bufferSize) {
		Preconditions.checkArgument(bufferSize >= -1, "invalid bufferSize");
		Preconditions.checkArgument(bufferedWriterCount==0, "Found unflushed entities in buffer. Please flush first");
		try {
			bufferedWriterKijiTable = this.getKijiManager().getKiji().openTable(entityInfo.name());
			bufferedWriter = bufferedWriterKijiTable.getWriterFactory().openBufferedWriter();
			long size = (bufferSize == -1 ? Long.MAX_VALUE : bufferSize);
			bufferedWriter.setBufferSize(size);
			LOG.info(String.format("[initBufferedWriter]buffered writter initialized for entity %s with size %s", entity.getName(), size));
		} catch (KijiTableNotFoundException e) {
			throw new KijiException(e);
		} catch (IOException e) {
			throw new KijiException(e);
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void bufferedSave(Entity entityObject) {
		Preconditions.checkNotNull(bufferedWriter, "The bufferedWriter be initialized");
		try {
			Object[] idComponents = this.getEntityIdComponentsFromEntityClass(entityObject);
			EntityId entityId = bufferedWriterKijiTable.getEntityId(idComponents);

			for (Field field : entity.getDeclaredFields()) {
				Column column = field.getAnnotation(Column.class);
				Family family = field.getAnnotation(Family.class);
				if ( column != null ) {
					ReflectionUtils.makeAccessible(field);
					Object value = field.get(entityObject);
					Translator translatorAnnotaion = field.getAnnotation(Translator.class);
					if ( translatorAnnotaion!=null ) {
						Class<?> translatorClass = translatorAnnotaion.using();
						value = this.translateToKijiData(translatorClass, value);
					}
					bufferedWriter.put(
							entityId, 
							Strings.isNullOrEmpty(column.family()) ? entityInfo.family() :column.family(),
							Strings.isNullOrEmpty(column.name()) ? field.getName() : column.name(), 
							value
					);
				} else if ( family != null && !Strings.isNullOrEmpty(family.name()) ) {
					ReflectionUtils.makeAccessible(field);
					Map<String, ?> valueMap = (Map<String, ?>) field.get(entityObject);
					for ( Entry<String, ?> e : valueMap.entrySet() ) {
						if (e.getValue()!= null && e.getKey()!= null ) {
							bufferedWriter.put(
									entityId, 
									family.name(),
									e.getKey(), 
									e.getValue()
									);
						}
					}
				}
			}
			bufferedWriterCount++;
			LOG.info(String.format("[bufferedSave]Entity %s with entity id %s saved in buffer. current buffer size: %s", 
					entity.getName(), entityId.toShellString(), bufferedWriterCount));

		} catch (KijiTableNotFoundException e) {
			throw new KijiException(e);
		} catch (InstantiationException e) {
			throw new KijiException(e);
		} catch (IOException e) {
			throw new KijiException(e);
		} catch (IllegalAccessException e) {
			throw new KijiException(e);
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void flushBufferedWriter() {
		Preconditions.checkNotNull(bufferedWriter, "The bufferedWriter must be initialized first");
		try {
			bufferedWriter.flush();
			LOG.info(String.format("[flushBufferedWriter]flushing %s. size: %s", entity.getName(), bufferedWriterCount));
			bufferedWriterCount = 0;
		} catch (IOException e) {
			throw new KijiException(e);
		} finally {
			ResourceUtils.closeOrLog(bufferedWriter);
			ResourceUtils.releaseOrLog(bufferedWriterKijiTable);
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isBufferedWriting() {
		return bufferedWriter!=null;
	}
	
	/**
	 * Get the column definitions from entity class
	 * @return ColumnsDef
	 */
	private ColumnsDef getColumnsDefFromEntityClass ( ) {
		
		// Add column definition
		ColumnsDef columnsDef = ColumnsDef.create();
		for (String f : familySet ) {
			columnsDef.addFamily(f);
		}
		return columnsDef;
	}

	/**
	 * Get the id components for the entity
	 * @return the id components
	 * @throws IllegalAccessException 
	 * @throws IllegalArgumentException 
	 * @throws InstantiationException 
	 * @throws IOException
	 */
	private Object[] getEntityIdComponentsFromEntityClass(Entity entityObject) 
			throws IllegalAccessException, InstantiationException, IOException {
		
		List<Object> entityIdComponents = new ArrayList<Object>();
		for (Field field : entity.getDeclaredFields()) {
			IdComponent idComponent = field.getAnnotation(IdComponent.class);
			if (idComponent != null) {
				ReflectionUtils.makeAccessible(field);
				Object value = field.get(entityObject);
				Translator translatorAnnotaion = field.getAnnotation(Translator.class);
				if ( translatorAnnotaion!=null ) {
					Class<?> translatorClass = translatorAnnotaion.using();
					value = this.translateToKijiData(translatorClass, value);
				}
				entityIdComponents.add(value);
			}
		}
		return entityIdComponents.toArray();
	}

	/**
	 * See below
	 * @param rowData
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	private Entity parseRowData ( KijiRowData rowData ) 
			throws InstantiationException, IllegalAccessException, IOException {
		return this.parseRowDataWithRowFilter(rowData, null);
	}
	
	/**
	 * Parse the row data into entity class
	 * @param rowData
	 * @param entityIdRowFilter
	 * @return Entity
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws IOException 
	 */
	private Entity parseRowDataWithRowFilter ( KijiRowData rowData, EntityIdComponentEqualsRowFilter entityIdRowFilter ) 
			throws InstantiationException, IllegalAccessException, IOException {
		
		Entity theEntity = null;
		theEntity = entity.newInstance();
		int idIndex = 0;
		Object[] idFilterComponents = entityIdRowFilter == null ? null : entityIdRowFilter.getComponents();
		for ( Field field : entity.getDeclaredFields() ) {
			Column column = field.getAnnotation(Column.class);
			Family family = field.getAnnotation(Family.class);
			IdComponent idComponent = field.getAnnotation(IdComponent.class);
			
			Object value = null;
			// 1. parse id components
			if (idComponent != null && !(rowData.getEntityId() instanceof HBaseEntityId)) {
				// the Id Components has to be in the same order as the schema
				// will skip if id is HBaseEntityId
				try {
					value = rowData.getEntityId().getComponents().get(idIndex);
					//apply entityIdRowFilter
					//TODO 
					//this piece will be replaced by FormattedEntityIdRowFilter
					//once the CDH5 compatible issue is fixed
					//https://groups.google.com/a/kiji.org/forum/#!topic/user/nx7cvzLWsHQ
					if ( idFilterComponents != null && 
						 idIndex < idFilterComponents.length && 
						 idFilterComponents[idIndex] != null ) {
						if ( !idFilterComponents[idIndex].equals(value) ) {
							return null;
						}
					}
				} catch (IndexOutOfBoundsException e) {
					throw new KijiException(e); 
				}
				idIndex++;
			}
			// 2. parse column data
			if (column != null) {
				Object columnValue = rowData.getMostRecentValue(
						Strings.isNullOrEmpty(column.family()) ? entityInfo.family() : column.family(),
								Strings.isNullOrEmpty(column.name()) ? field.getName() : column.name()
						);
				// if the field is both IdComponent and Column, 
				// column value (if not null) will overwrite the field
				if (columnValue!=null) {
					value = columnValue;
				}
			// 3. parse family data	
			} else if ( family!=null && !Strings.isNullOrEmpty(family.name()) ) {
				value = rowData.getMostRecentValues(family.name());
			}

			if (value!=null) {
				this.setEntityField(theEntity, field, value);
			}
		}
		return theEntity;
	}

	/**
	 * Set the field with proper type
	 * @param theEntity
	 * @param field
	 * @param value
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	private void setEntityField (Entity theEntity, Field field, Object value) 
			throws InstantiationException, IllegalAccessException, IOException {
		
		if ( value == null ) return;
		Translator translatorAnnotaion = field.getAnnotation(Translator.class);
		if  ( field.getType().equals(org.joda.time.DateTime.class) ) {
			value = new DateTime(value);
		} else if ( field.getType().equals(java.util.Date.class) ) {
			value = new Date((Long)value);
		} else if (field.getType().equals(java.lang.String.class) && value instanceof Utf8) {
			value = ((Utf8)value).toString();
		} else if ( translatorAnnotaion!=null ) {
			Class<?> translatorClass = translatorAnnotaion.using();
			value = this.translateFromKijiData(translatorClass, value);
		}
		ReflectionUtils.makeAccessible(field);
		ReflectionUtils.setField(field, theEntity, value);	
	}
	
	/**
	 * Translate from Kiji row data using translator
	 * @param translatorClass
	 * @param value
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private Object translateFromKijiData (Class<?> translatorClass, Object value) 
			throws InstantiationException, IllegalAccessException, IOException {
		
		Object translator = translatorClass.newInstance();
		if ( translator instanceof KijiDataTranslator<?, ?> ) {
			value = ((KijiDataTranslator<?, Object>) translator).reverseConvert(value);
			return value;
		} else {
			throw new IOException("Unspported translator");
		}
	}
	
	/**
	 * Translate to Kiji row data using translator
	 * @param translatorClass
	 * @param value
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private Object translateToKijiData (Class<?> translatorClass, Object value) 
			throws InstantiationException, IllegalAccessException, IOException {
		
		Object translator = translatorClass.newInstance();
		if ( translator instanceof KijiDataTranslator<?, ?> ) {
			value = ((KijiDataTranslator<Object, ?>) translator).convert(value);
			return value;
		} else {
			throw new IOException("Unspported translator");
		}
	}
}
