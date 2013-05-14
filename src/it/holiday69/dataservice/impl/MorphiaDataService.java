/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.dataservice.impl;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Key;

import it.holiday69.dataservice.DataService;
import it.holiday69.dataservice.query.FieldFilter;
import it.holiday69.dataservice.query.OrderFilter;
import it.holiday69.dataservice.query.Query;
import java.util.LinkedList;
import java.util.List;

public class MorphiaDataService extends DataService {

  private static Datastore _ds;
  private Throwable _lastError;
  
  public MorphiaDataService() {
  }
  
  public static synchronized void setDataStore(Datastore ds) {  _ds = ds;  }
  
  
  @Override
  public <T> void put(T object) {
    if(_ds == null) throw new RuntimeException("The datastore is not set! Call 'MorphiaDataService.setDataStore' first");
    try { _ds.save(object); } catch(Throwable th) { _lastError = th; }
  }

  @Override
  public <T> void putAll(Iterable<T> entities) {
    if(_ds == null) throw new RuntimeException("The datastore is not set! Call 'MorphiaDataService.setDataStore' first");
    try { _ds.save(entities); } catch(Throwable th) { _lastError = th; }
  }

  @Override
  public <T, V> T get(V key, Class<T> classOfT) {
    if(_ds == null) throw new RuntimeException("The datastore is not set! Call 'MorphiaDataService.setDataStore' first");
    try { return _ds.get(classOfT, key); } catch(Throwable th) { _lastError = th; return null; }
  }

  @Override
  public <T> T get(String fieldName, Object fieldValue, Class<T> classOfT) {
    if(_ds == null) throw new RuntimeException("The datastore is not set! Call 'MorphiaDataService.setDataStore' first");
    try { return getMorphiaQuery(new Query().filter(fieldName, fieldValue), classOfT).get(); } catch(Throwable th) { _lastError = th; return null; }
  }

  @Override
  public <T> T get(Query query, Class<T> classOfT) {
    if(_ds == null) throw new RuntimeException("The datastore is not set! Call 'MorphiaDataService.setDataStore' first");
    try { return getMorphiaQuery(query, classOfT).get(); } catch(Throwable th) { _lastError = th; return null; }
  }

  @Override
  public <T> T get(Class<T> classOfT) {
    if(_ds == null) throw new RuntimeException("The datastore is not set! Call 'MorphiaDataService.setDataStore' first");
    try { return _ds.find(classOfT).get(); } catch(Throwable th) { _lastError = th; return null; }
  }

  @Override
  public <T> List<T> getList(String fieldName, Object fieldValue, Class<T> classOfT) {
    if(_ds == null) throw new RuntimeException("The datastore is not set! Call 'MorphiaDataService.setDataStore' first");
    try { return getMorphiaQuery(new Query().filter(fieldName, fieldValue), classOfT).asList(); } catch(Throwable th) { _lastError = th; return new LinkedList<T>(); }
  }

  @Override
  public <T> List<T> getList(Class<T> classOfT) {
    if(_ds == null) throw new RuntimeException("The datastore is not set! Call 'MorphiaDataService.setDataStore' first");
    try { return getMorphiaQuery(new Query(), classOfT).asList(); } catch(Throwable th) { _lastError = th; return new LinkedList<T>(); }
  }

  @Override
  public <T> List<T> getList(Query query, Class<T> classOfT) {
    if(_ds == null) throw new RuntimeException("The datastore is not set! Call 'MorphiaDataService.setDataStore' first");
    try { return getMorphiaQuery(query, classOfT).asList(); } catch(Throwable th) { _lastError = th; return new LinkedList<T>(); }
  }

  @Override
  public <T> void delete(T object) {
    if(_ds == null) throw new RuntimeException("The datastore is not set! Call 'MorphiaDataService.setDataStore' first");
    try { _ds.delete(object); } catch(Throwable th) { _lastError = th; }
  }

  @Override
  public <T> void deleteAll(Iterable<T> entities) {
    if(_ds == null) throw new RuntimeException("The datastore is not set! Call 'MorphiaDataService.setDataStore' first");
    try { 
      for(T entity : entities)
        _ds.delete(entity); 
    } catch(Throwable th) { _lastError = th; }
  }

  @Override
  public <T> void deleteAll(Query query, Class<T> classOfT) {
    if(_ds == null) throw new RuntimeException("The datastore is not set! Call 'MorphiaDataService.setDataStore' first");
    try { 
      
      List<T> dataList = null;
      do {
        dataList = getMorphiaQuery(query.limit(100), classOfT).asList();
        for(T item : dataList)
          _ds.delete(item);
        
      } while(dataList != null && !dataList.isEmpty());
      
    } catch(Throwable th) { _lastError = th; }
  }

  @Override
  public <T> void deleteAll(Class<T> classOfT) {
    if(_ds == null) throw new RuntimeException("The datastore is not set! Call 'MorphiaDataService.setDataStore' first");
    try { 
      
      List<T> dataList = null;
      do {
        dataList = getMorphiaQuery(new Query().limit(100), classOfT).asList();
        for(T item : dataList)
          _ds.delete(item);
        
      } while(dataList != null && !dataList.isEmpty());
      
    } catch(Throwable th) { _lastError = th; }
  }

  @Override
  public <T> long getResultSetSize(Class<T> classOfT) {
    if(_ds == null) throw new RuntimeException("The datastore is not set! Call 'MorphiaDataService.setDataStore' first");
    try { return _ds.find(classOfT).countAll(); } catch(Throwable th){ _lastError = th; return 0; }
  }

  @Override
  public <T> long getResultSetSize(String fieldName, Object fieldValue, Class<T> classOfT) {
    if(_ds == null) throw new RuntimeException("The datastore is not set! Call 'MorphiaDataService.setDataStore' first");
    try { return getMorphiaQuery(new Query().filter(fieldName, fieldValue), classOfT).countAll(); } catch(Throwable th){ _lastError = th; return 0; }
  }

  @Override
  public <T> long getResultSetSize(Query query, Class<T> classOfT) {
    if(_ds == null) throw new RuntimeException("The datastore is not set! Call 'MorphiaDataService.setDataStore' first");
    try { return getMorphiaQuery(query, classOfT).countAll(); } catch(Throwable th){ _lastError = th; return 0; }
  }
  
  @Override
  public Throwable getLastError() { Throwable th = _lastError; _lastError = null; return th; }
  
  public <T> long getMaxId(Class<T> classOfT) {
    if(_ds == null) throw new RuntimeException("The datastore is not set! Call 'MorphiaDataService.setDataStore' first");
    try {
      List<Key<T>> keyList = _ds.find(classOfT).asKeyList();

      long maxID = 0;
      for(Key<T> key : keyList) {
        if(key.getId() instanceof Integer) {
          int curID = (Integer)key.getId();
          if(maxID < curID)
              maxID = (long)curID;

        } else if(key.getId() instanceof Long) {
          long curID = (Long)key.getId();
          if(maxID < curID)
              maxID = curID;
        } else
          throw new RuntimeException("getMaxId: Only int or long types are supported, found type: " + key.getId().getClass() + " for class " + classOfT);

      }
      return maxID;
    } catch(Throwable th) { _lastError = th; return 0; }
  }
  
  
  private <T> com.google.code.morphia.query.Query<T> getMorphiaQuery(Query srcQuery, Class<T> classOfT) {
    
    com.google.code.morphia.query.Query<T> destQuery = _ds.find(classOfT);
		
		for(FieldFilter fieldFilter : srcQuery.getFieldFilterList()) {
			
			switch(fieldFilter.getFieldFilterType()) {
				case EQUAL: destQuery = destQuery.filter(fieldFilter.getFieldName(), fieldFilter.getFieldValue()); break;
				case GREATER_THAN: destQuery = destQuery.filter(fieldFilter.getFieldName() + " >", fieldFilter.getFieldValue()); break;
				case GREATER_THAN_INC: destQuery = destQuery.filter(fieldFilter.getFieldName() + " >=", fieldFilter.getFieldValue()); break;
				case LOWER_THAN: destQuery = destQuery.filter(fieldFilter.getFieldName() + " <", fieldFilter.getFieldValue()); break;
				case LOWER_THAN_INC: destQuery = destQuery.filter(fieldFilter.getFieldName() + " <=", fieldFilter.getFieldValue()); break;
			}
			
		}
		
		for(OrderFilter orderFilter : srcQuery.getOrderFilterList()) {
			
			switch(orderFilter.getOrderType()) {
				case ASCENDING: destQuery = destQuery.order(orderFilter.getFieldName()); break;
				case DESCENDING: destQuery = destQuery.order("-" + orderFilter.getFieldName()); break;
			}
		}
		
		return destQuery.limit(srcQuery.getLimit()).offset(srcQuery.getOffset());
  }
		
}

