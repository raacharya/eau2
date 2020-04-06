#pragma once
#include "object.h"
#include "string.h"
#include "network.h"
#include <cstring>
#include <assert.h>
#include <stdlib.h>
#include <iostream>

/**
 * @file array.h
 * @brief Implementation of Array class.
 * @author Rohit Pathak and Rahul Acharya
 * @date January 26, 2020
 */

/**
 * Represents an array of Objects.
 */
class FixedArray : public Object {
	public:
		Object** array;
		size_t used;
		size_t capacity;


		/**
		 * Default constructor of this array.
		 */
		FixedArray() {
			used = 0;
			capacity = 1;
			array  = new Object*[1];
			for (int i = 0; i < capacity; i++) {
				array[i] = nullptr;
			}
		}


		/**
		 * Constructor of this array of Object.
		 *
		 * @param size the base size of this array
		 */
		FixedArray(size_t size) : Object() {
			used = 0;
			capacity = size;
			array  = new Object*[size];
			for (int i = 0; i < capacity; i++) {
				array[i] = nullptr;
			}
		}


		/**
		 * Returns an element at the given index.
		 *
		 * @param index the index of the element
		 * @return the element of the array at the given index
		 */
		virtual Object* get(size_t index) {
			assert(index < capacity);
			return array[index];
		}
		
		/**
		 * @brief Does this equal other?
		 * 
		 * @param other the other Object to compare
		 * @return true if other equals this
		 * @return false if other does not equal this
		 */
		bool equals(Object* other) {
			FixedArray* o = dynamic_cast<FixedArray*>(other);
			if (!o) return false;
			else if (o->used != used) return false;
			for (int i = 0; i < used; i++) {
				if (!array[i]->equals(o->get(i))) {
					return false;
				}
			}
			return true;
		}

		/**
		 * Pushes the given item to the end of this array.
		 *
		 * @param item the given item to be added to the end of this array
		 */
		virtual void pushBack(Object* item) {
			assert(used < capacity);
			array[used] = item;
			used += 1;
		}

		void set(size_t index, Object* item) {
			assert(index < capacity);
			array[index] = item;
		}

		int indexOf(Object* item) {
			assert(item != nullptr);
			for (int i = 0; i < capacity; i += 1) {
				if (item->equals(array[i])) {
					return i;
				}
			}
			return -1;
		}

        void removeAndSwitch(int index) {
            set(index, get(used - 1));
            set(used - 1, nullptr);
            used -= 1;
        }


		/**
		 * The destructor of this array.
		 */
		virtual ~FixedArray() {
			delete[] array;
		}
};

class DistFixedArray : public Object  {
    public:
        String* id;
        size_t used;
        size_t capacity;
        Distributable* kv_store;

        DistFixedArray(FixedArray& from, String* id_var, Distributable* kvStore_var) : Object() {
            id = id_var;
            kv_store = kvStore_var;
        }

        DistFixedArray(String* id_var, Distributable* kvStore_var) : Object() {
            id = id_var;
            kv_store = kvStore_var;
        }
};


/**
 * Represents an array of ints.
 */
class FixedIntArray : public Object {
	public:
		int* array;
		size_t used;
		size_t capacity;


		/**
		 * Default constructor of this array.
		 */
		FixedIntArray() : Object() {
			used = 0;
			capacity = 1;
			array = new int[1];
		}


		/**
		 * Constructor of this array of Object.
		 *
		 * @param size the base size of this array
		 */
		FixedIntArray(size_t size) : Object() {
			used = 0;
			capacity = size;
			array = new int[size];
		}

		FixedIntArray(FixedIntArray& from) : Object() {
			used = from.used;
			capacity = from.capacity;
			array = new int[capacity];
			for (int i = 0; i < used; i += 1) {
				array[i] = from.get(i);
			}
		}

		FixedIntArray* clone() {
		    return new FixedIntArray(*this);
		}


		/**
		 * Returns an element at the given index.
		 *
		 * @param index the index of the element
		 * @return the element of the array at the given index
		 */
		virtual int get(size_t index) {
			assert(index < capacity);
			return array[index];
		}
		
		/**
		 * @brief Does this equal other?
		 * 
		 * @param other the other Object to compare
		 * @return true if other equals this
		 * @return false if other does not equal this
		 */
		bool equals(Object* other) {
			FixedIntArray* o = dynamic_cast<FixedIntArray*>(other);
			if (!o) return false;
			else if (o->used != used) return false;
			for (int i = 0; i < used; i++) {
				if (array[i] != o->get(i)) {
					return false;
				}
			}
			return true;
		}

		/**
		 * Pushes the given item to the end of this array.
		 *
		 * @param item the given item to be added to the end of this array
		 */
		virtual void pushBack(int item) {
			assert(used < capacity);
			array[used] = item;
			used += 1;
		}

		/**
		 * @brief sets the element in this array to the item
		 * 
		 * @param index 
		 * @param item 
		 */
		void set(size_t index, int item) {
			assert(index < capacity);
			array[index] = item;
		}

		/**
		 * @brief Returns the index of the item. Returns -1 if not there.
		 * 
		 * @param item 
		 * @return int 
		 */
		int indexOf(int item) {
			for (int i = 0; i < used; i += 1) {
				if (item == array[i]) {
					return i;
				}
			}
			return -1;
		}

        void removeAndSwitch(int index) {
            set(index, get(used - 1));
            set(used - 1, 0);
            used -= 1;
        }


		/**
		 * The destructor of this array.
		 */
		virtual ~FixedIntArray() {
			delete[] array;
		}
};

/**
 * Represents an array of chars.
 */
class FixedCharArray : public Object {
	public:
		char* array;
		size_t used;
		size_t capacity;


		/**
		 * Default constructor of this array.
		 */
		FixedCharArray() : Object() {
			used = 0;
			capacity = 1;
			array = new char[1];
		}

		FixedCharArray(FixedCharArray& from) : Object() {
			used = from.used;
			capacity = from.capacity;
			array = new char[capacity];
			for (int i = 0; i < used; i += 1) {
				array[i] = from.get(i);
			}
		}


		/**
		 * Constructor of this array of Object.
		 *
		 * @param size the base size of this array
		 */
		FixedCharArray(size_t size) : Object() {
			used = 0;
			capacity = size;
			array = new char[size];
		}

        FixedCharArray* clone() {
            return new FixedCharArray(*this);
        }


		/**
		 * Returns an element at the given index.
		 *
		 * @param index the index of the element
		 * @return the element of the array at the given index
		 */
		virtual char get(size_t index) {
			assert(index < capacity);
			return array[index];
		}
		
		/**
		 * @brief Does this equal other?
		 * 
		 * @param other the other Object to compare
		 * @return true if other equals this
		 * @return false if other does not equal this
		 */
		bool equals(Object* other) {
			FixedCharArray* o = dynamic_cast<FixedCharArray*>(other);
			if (!o) return false;
			else if (o->used != used) return false;
			for (int i = 0; i < used; i++) {
				if (array[i] != o->get(i)) {
					return false;
				}
			}
			return true;
		}

		/**
		 * Pushes the given item to the end of this array.
		 *
		 * @param item the given item to be added to the end of this array
		 */
		virtual void pushBack(char item) {
			assert(used < capacity);
			array[used] = item;
			used += 1;
		}

		/**
		 * @brief sets the element in this array to the item
		 * 
		 * @param index 
		 * @param item 
		 */
		void set(size_t index, char item) {
			assert(index < capacity);
			array[index] = item;
		}

		/**
		 * @brief Returns the index of the item. Returns -1 if not there.
		 * 
		 * @param item 
		 * @return int 
		 */
		int indexOf(char item) {
			for (int i = 0; i < used; i += 1) {
				if (item == array[i]) {
					return i;
				}
			}
			return -1;
		}


		/**
		 * The destructor of this array.
		 */
		virtual ~FixedCharArray() {
			delete[] array;
		}
};

/**
 * Represents an array of bools.
 */
class FixedBoolArray : public Object {
	public:
		bool* array;
		size_t used;
		size_t capacity;


		/**
		 * Default constructor of this array.
		 */
		FixedBoolArray() : Object() {
			used = 0;
			capacity = 1;
			array = new bool[1];
		}


		/**
		 * Constructor of this array of Object.
		 *
		 * @param size the base size of this array
		 */
		FixedBoolArray(size_t size) : Object() {
			used = 0;
			capacity = size;
			array = new bool[size];
		}

		FixedBoolArray(FixedBoolArray& from) : Object() {
			used = from.used;
			capacity = from.capacity;
			array = new bool[capacity];
			for (int i = 0; i < used; i += 1) {
				array[i] = from.get(i);
			}
		}

        FixedBoolArray* clone() {
            return new FixedBoolArray(*this);
        }


		/**
		 * Returns an element at the given index.
		 *
		 * @param index the index of the element
		 * @return the element of the array at the given index
		 */
		virtual bool get(size_t index) {
			assert(index < capacity);
			return array[index];
		}
		
		/**
		 * @brief Does this equal other?
		 * 
		 * @param other the other Object to compare
		 * @return true if other equals this
		 * @return false if other does not equal this
		 */
		bool equals(Object* other) {
			FixedBoolArray* o = dynamic_cast<FixedBoolArray*>(other);
			if (!o) return false;
			else if (o->used != used) return false;
			for (int i = 0; i < used; i++) {
				if (array[i] != o->get(i)) {
					return false;
				}
			}
			return true;
		}

		/**
		 * Pushes the given item to the end of this array.
		 *
		 * @param item the given item to be added to the end of this array
		 */
		virtual void pushBack(bool item) {
			assert(used < capacity);
			array[used] = item;
			used += 1;
		}

		/**
		 * @brief sets the element in this array to the item
		 * 
		 * @param index 
		 * @param item 
		 */
		void set(size_t index, bool item) {
			assert(index < capacity);
			array[index] = item;
		}

		/**
		 * @brief Returns the index of the item. Returns -1 if not there.
		 * 
		 * @param item 
		 * @return int 
		 */
		int indexOf(bool item) {
			for (int i = 0; i < used; i += 1) {
				if (item == array[i]) {
					return i;
				}
			}
			return -1;
		}


		/**
		 * The destructor of this array.
		 */
		virtual ~FixedBoolArray() {
			delete[] array;
		}
};

/**
 * Represents an array of floats.
 */
class FixedFloatArray : public Object {
	public:
		float* array;
		size_t used;
		size_t capacity;


		/**
		 * Default constructor of this array.
		 */
		FixedFloatArray() : Object() {
			used = 0;
			capacity = 1;
			array = new float[1];
		}


		/**
		 * Constructor of this array of Object.
		 *
		 * @param size the base size of this array
		 */
		FixedFloatArray(size_t size) : Object() {
			used = 0;
			capacity = size;
			array = new float[size];
		}

		FixedFloatArray(FixedFloatArray& from) : Object() {
			used = from.used;
			capacity = from.capacity;
			array = new float[capacity];
			for (int i = 0; i < used; i += 1) {
				array[i] = from.get(i);
			}
		}

        FixedFloatArray* clone() {
            return new FixedFloatArray(*this);
        }


		/**
		 * Returns an element at the given index.
		 *
		 * @param index the index of the element
		 * @return the element of the array at the given index
		 */
		virtual float get(size_t index) {
			assert(index < capacity);
			return array[index];
		}
		
		/**
		 * @brief Does this equal other?
		 * 
		 * @param other the other Object to compare
		 * @return true if other equals this
		 * @return false if other does not equal this
		 */
		bool equals(Object* other) {
			FixedFloatArray* o = dynamic_cast<FixedFloatArray*>(other);
			if (!o) return false;
			else if (o->used != used) return false;
			for (int i = 0; i < used; i++) {
				if (array[i] != o->get(i)) {
					return false;
				}
			}
			return true;
		}

		/**
		 * Pushes the given item to the end of this array.
		 *
		 * @param item the given item to be added to the end of this array
		 */
		virtual void pushBack(float item) {
			assert(used < capacity);
			array[used] = item;
			used += 1;
		}

		/**
		 * @brief sets the element in this array to the item
		 * 
		 * @param index 
		 * @param item 
		 */
		void set(size_t index, float item) {
			assert(index < capacity);
			array[index] = item;
		}

		/**
		 * @brief Returns the index of the item. Returns -1 if not there.
		 * 
		 * @param item 
		 * @return int 
		 */
		int indexOf(float item) {
			for (int i = 0; i < used; i += 1) {
				if (item == array[i]) {
					return i;
				}
			}
			return -1;
		}


		/**
		 * The destructor of this array.
		 */
		virtual ~FixedFloatArray() {
			delete[] array;
		}
};

/**
 * Represents an array of Strings.
 */
class FixedStrArray : public Object {
	public:
		FixedArray* array;

		/**
		 * Default constructor of this array.
		 */
		FixedStrArray() : Object() {
			array = new FixedArray();
		}


		/**
		 * Constructor of this array of floats.
		 *
		* @param size the base size of this array
		*/
		FixedStrArray(size_t size) : Object() {
			array = new FixedArray(size);
		}

		FixedStrArray(FixedStrArray& from) : FixedStrArray(from.size()) {
			for (int i = 0; i < from.size(); i += 1) {
				String* str = from.get(i);
				set(i, new String(str->c_str()));
			}
		}

        FixedStrArray* clone() {
            return new FixedStrArray(*this);
        }


		/**
		 * Returns an element at the given index.
		 *
		 * @param index the index of the element
		 * @return the element of the array at the given index
		 */
		virtual String* get(size_t index) {
			String* element = dynamic_cast <String *> (array->get(index));
			return element;
		}

		/**
		 * @brief Does this equal other?
		 * 
		 * @param other the other Object to compare
		 * @return true if other equals this
		 * @return false if other does not equal this
		 */
		bool equals(Object* other) {
			return array->equals(other);
		}

		/**
		 * Pushes the given item to the end of this array.
		 *
		 * @param item the given item to be added to the end of this array
		 */
		virtual void pushBack(String* item) {
			array->pushBack(item);
		}

		/**
		 * @brief sets the element in this array to the item
		 * 
		 * @param index 
		 * @param item 
		 */
		void set(size_t index, String* item) {
			array->set(index, item);
		}

		/**
		 * @brief Returns the capacity of this array
		 * 
		 * @return size_t 
		 */
		size_t size() {
			return array->capacity;
		}

		/**
		 * @brief Returns te size of this array
		 * 
		 * @return size_t 
		 */
		size_t numElements() {
			return array->used;
		}

		/**
		 * @brief Returns the index of the item. Returns -1 if not there.
		 * 
		 * @param item 
		 * @return int 
		 */
		int indexOf(String* item) {
			return array->indexOf(item);
		}

        void removeAndSwitch(int index) {
            array->removeAndSwitch(index);
        }


		/**
		 * Destructor of this class.
		 */
		~FixedStrArray() {
			delete array;
		}
};

/**
 * @brief A union for each type of column our dataframe supports. Useful for rows.
 * 
 */
union Type {
  	int i;
  	float f;
  	bool b;
  	String* s;
};

/**
 * Represents an array of Type.
 */
class FixedTypeArray : public Object {
	public:
		Type** array;
		size_t used;
		size_t capacity;


		/**
		 * Default constructor of this array.
		 */
		FixedTypeArray() : Object() {
			used = 0;
			capacity = 1;
			array = new Type*[1];
		}


		/**
		 * Constructor of this array of Object.
		 *
		 * @param size the base size of this array
		 */
		FixedTypeArray(size_t size) : Object() {
			used = 0;
			capacity = size;
			array = new Type*[size];
		}

		FixedTypeArray(FixedTypeArray& from) : Object() {
			used = from.used;
			capacity = from.capacity;
			array = new Type*[capacity];
			for (int i = 0; i < used; i += 1) {
				array[i] = from.get(i);
			}
		}


		/**
		 * Returns an element at the given index.
		 *
		 * @param index the index of the element
		 * @return the element of the array at the given index
		 */
		virtual Type* get(size_t index) {
			assert(index < capacity);
			return array[index];
		}
		
		/**
		 * @brief Does this equal other?
		 * 
		 * @param other the other Object to compare
		 * @return true if other equals this
		 * @return false if other does not equal this
		 */
		bool equals(Object* other) {
			FixedTypeArray* o = dynamic_cast<FixedTypeArray*>(other);
			if (!o) return false;
			else if (o->used != used) return false;
			for (int i = 0; i < used; i++) {
				if (array[i] != o->get(i)) {
					return false;
				}
			}
			return true;
		}

		/**
		 * Pushes the given item to the end of this array.
		 *
		 * @param item the given item to be added to the end of this array
		 */
		virtual void pushBack(Type* item) {
			assert(used < capacity);
			array[used] = item;
			used += 1;
		}

		/**
		 * @brief sets the element in this array to the item
		 * 
		 * @param index 
		 * @param item 
		 */
		void set(size_t index, Type* item) {
			assert(index < capacity);
			array[index] = item;
		}

		/**
		 * @brief Returns the index of the item. Returns -1 if not there.
		 * 
		 * @param item 
		 * @return int 
		 */
		int indexOf(Type* item) {
			for (int i = 0; i < used; i += 1) {
				if (item == array[i]) {
					return i;
				}
			}
			return -1;
		}


		/**
		 * The destructor of this array.
		 */
		virtual ~FixedTypeArray() {
			for (int i = 0; i < used; i += 1) {
				delete array[i];
			}
			delete[] array;
		}
};