package br.puc_rio.ele.lvc.interimage.common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 *  Keeps a list of tuples in ascending order of membership value.
 * */

public class OrderedList {
	
	List<Tuple> _list;
	//Map<String,Tuple> _map = new HashMap<String,Tuple>();
	
	public OrderedList(long size) {
		_list = new ArrayList<Tuple>((int)size);
	}
	
	/** Adds an element in the proper ordered position. */
	
	public void add(Tuple value) {
	
		//_map.put(key, value);

		try {
		
			if (_list.isEmpty()) {
				_list.add(value);
			} else {
			
				Map<String,Object> p = DataType.toMap(value.get(2));
				
				Double m = DataType.toDouble(p.get("membership"));
				
				int pos = -1;
				
				for (int i=0; i<_list.size(); i++) {
				
					Map<String,Object> props = DataType.toMap(_list.get(i).get(2));
					
					Double membership = DataType.toDouble(props.get("membership"));
					
					if (m <= membership) {
						pos = i;
						break;
					}

				}
				
				if (pos!=-1)
					_list.add(pos, value);
				else
					_list.add(value);
			
			}
			
		} catch (Exception e) {
				System.err.println("Failed to add element; error - " + e.getMessage());					
		}
			
	}
	
	/** Returns the tuple with the highest membership value. */
	
	public Tuple poll() {
		
		Tuple result = null;
		
		try {
		
			/*boolean found = false;
			
			Tuple t = null;
			
			while ((!found) && (!_list.isEmpty())) {
				t = _list.remove(0);
				
				Map<String,Object> p = DataType.toMap(t.get(2));
								
				found = !p.containsKey("remove");
			}
			
			if (found)
				result = t;*/
			
			result = _list.remove(0);
			
		} catch (Exception e) {
			System.err.println("Failed to poll element; error - " + e.getMessage());	
		}
		
		return result;
		
	}
	
	/*public void remove(String key) {
		_map.remove(key);
	}*/

	/** Returns true if the list is empty. */
	
	public boolean isEmpty() {
		return _list.isEmpty();
	}
	
	/** Returns the element at the specified position. */
	
	public Tuple get(int index) {
		return _list.get(index);
	}
	
	/** Returns the size of the list. */
	
	public Iterator<Tuple> iterator() {
		return _list.iterator();
	}
	
	public int size() {
		return _list.size();
	}
	
}
