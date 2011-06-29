package de.pc2.dedup.util.udf;

 
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class CountingMap<K> implements Map<K, Long>, Serializable {

	private static final long serialVersionUID = -7261515408175064302L;
	
	private final LinkedHashMap<K, Long> internalMap = new LinkedHashMap<K, Long>();
	
	public CountingMap() {
		
	}
	
	public void clear() {
		internalMap.clear();		
	}

	public boolean containsKey(Object key) {
		return internalMap.containsKey(key);
	}

	public boolean containsValue(Object value) {
		return containsValue(value);
	}

	public Set<java.util.Map.Entry<K, Long>> entrySet() {
		return internalMap.entrySet();
	}

	public boolean isEmpty() {
		return internalMap.isEmpty();
	}

	public Set<K> keySet() {
		return internalMap.keySet();
	}

	public void putAll(Map<? extends K, ? extends Long> t) {
		internalMap.putAll(t);
	}
	

	public int size() {
		return internalMap.size();
	}

	public Collection<Long> values() {
		return internalMap.values();
	}

	public Long get(Object key) {
		Long value = internalMap.get(key);
		if (value == null) {
			return 0L;
		}
		return value;
	}

	public Long put(K key, Long value) {
		Long counter = internalMap.get(key);
		if (counter == null) {
			counter = (value == null ? 1 : value);
		} else {
			counter += (value == null ? 1 : value);
		}
		internalMap.put(key, counter);
		return counter;
	}
	
	public Long increment(K key) {
		return put(key, 1L);
	}
	
	/**
	 * Returns the counter of key after removing the key.
	 * 
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	public Long remove(Object key) {
		Long value = internalMap.get(key);
		if (value == null || value == 0L) {
			return 0L;
		}
		
		value--;
		internalMap.put((K) key, value);
		return value;
	}
	
	public void resetCounter(K key) {
		internalMap.remove(key);
	}

	public void add(K key) {
		this.put(key, null);
		
	}

	public K getMax() {
		long max = 0;
		K t = null;
		for(K key : keySet()) {
			Long count = get(key);
			if(count != null && count > max) {
				max = count;
				t = key;
			}
		}
		return t;
	}
	
}