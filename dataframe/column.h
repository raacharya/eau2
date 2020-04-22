#pragma once
#include <stdlib.h>
#include "../util/string.h"
#include <stdarg.h>
#include "../array/efficientArray.h"


class IntColumn;
class BoolColumn;
class FloatColumn;
class StringColumn;
class DistIntColumn;
class DistBoolColumn;
class DistFloatColumn;
class DistStringColumn;


/**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality. */

class Column : public Object {
  public:

    char type;

    /** Type converters: Return same column under its actual type, or
     *  nullptr if of the wrong type.  */
    virtual IntColumn* as_int() {
        assert(false);
    }
    virtual BoolColumn*  as_bool() {
        assert(false);
    }
    virtual FloatColumn* as_float() {
        assert(false);
    }
    virtual StringColumn* as_string() {
        assert(false);
    }

    /** Type appropriate push_back methods. Calling the wrong method is
      * undefined behavior. **/
    virtual void push_back(int val) {

    }
    virtual void push_back(bool val) {

    }
    virtual void push_back(float val) {

    }
    virtual void push_back(String* val) {

    }

    /** Returns the number of elements in the column. */
    virtual size_t size() {
        assert(false);
    }

    /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'.*/
    char get_type() {
      return type;
    }

    virtual Column* clone(){
        assert(false);
    };

    virtual ~Column() {

    }

};

class DistColumn : public Object {
    public:

        char type;

        /** Type converters: Return same column under its actual type, or
         *  nullptr if of the wrong type.  */
        virtual DistIntColumn* as_int() {
            assert(false);
        }
        virtual DistBoolColumn*  as_bool() {
            assert(false);
        }
        virtual DistFloatColumn* as_float() {
            assert(false);
        }
        virtual DistStringColumn* as_string() {
            assert(false);
        }

        /** Returns the number of elements in the column. */
        virtual size_t size() {
            assert(false);
        }

        /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'.*/
        char get_type() {
            return type;
        }

        virtual ~DistColumn() {

        }

};

/*************************************************************************
 * IntColumn::
 * Holds int values.
 */
class IntColumn : public Column {
    public:
        EffIntArr* array;

        /**
         * @brief Construct a new Int Column object
         * 
         */
        IntColumn() {
            array = new EffIntArr();
            type = 'I';
        }

        /**
         * @brief Construct a new Int Column object
         * 
         * @param from 
         */
        IntColumn(IntColumn& from) {
            array = new EffIntArr(*from.array);
            type = 'I';
        }

        IntColumn* clone(){
            return new IntColumn(*this);
        };

        /**
         * @brief get the int at the given index
         * 
         * @param idx 
         * @return int 
         */
        int get(size_t idx) {
            return array->get(idx);
        }

        /**
         * @brief return this int column as an int column
         * 
         * @return IntColumn* 
         */
        IntColumn* as_int() {
            return this;
        }

        /**
         * @brief throw error since this is an int column
         * 
         * @return FloatColumn* 
         */
        FloatColumn* as_float() {
            assert(false);
        }

        /**
         * @brief throw error since this is an int column 
         * 
         * @return BoolColumn* 
         */
        BoolColumn* as_bool() {
            assert(false);
        }

        /**
         * @brief throw error since this is an int column
         * 
         * @return StringColumn* 
         */
        StringColumn* as_string() {
            assert(false);
        }

        /**
         * @brief set the element at the given index to val
         * 
         * @param idx 
         * @param val 
         */
        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, int val) {
            array->set(idx, val);
        }

        /**
         * @brief push back the val to this column
         * 
         * @param val 
         */
        void push_back(int val) {
          array->pushBack(val);
        }

        /**
         * @brief get the size of this int column
         * 
         * @return size_t 
         */
        size_t size() {
            return array->size();
        }

        /**
         * @brief Destroy the Int Column object
         * 
         */
        ~IntColumn() {
            delete array;
        }
};

/*************************************************************************
 * DistIntColumn::
 * Holds int values in a distributed network.
 */
class DistIntColumn : public DistColumn {
    public:
        DistEffIntArr* array;

        /**
         * @brief Construct a new Int Column object
         *
         */
        DistIntColumn(String* id, Distributable* kv_store, size_t metadata_node) {
            array = new DistEffIntArr(id, kv_store, metadata_node);
            type = 'I';
        }

        /**
         * @brief Construct a new Int Column object
         *
         * @param from
         */
        DistIntColumn(IntColumn& from, String* id, Distributable* kv_store, size_t metadata_node) {
            array = new DistEffIntArr(*from.array, id, kv_store, metadata_node);
            type = 'I';
        }

        /**
         * @brief get the int at the given index
         *
         * @param idx
         * @return int
         */
        int get(size_t idx) {
            return array->get(idx);
        }

        /**
         * @brief return this int column as an int column
         *
         * @return IntColumn*
         */
        DistIntColumn* as_int() {
            return this;
        }

        /**
         * @brief throw error since this is an int column
         *
         * @return FloatColumn*
         */
        DistFloatColumn* as_float() {
            assert(false);
        }

        /**
         * @brief throw error since this is an int column
         *
         * @return BoolColumn*
         */
        DistBoolColumn* as_bool() {
            assert(false);
        }

        /**
         * @brief throw error since this is an int column
         *
         * @return StringColumn*
         */
        DistStringColumn* as_string() {
            assert(false);
        }

        /**
         * @brief get the size of this int column
         *
         * @return size_t
         */
        size_t size() {
            return array->size();
        }

        /**
         * @brief Destroy the Int Column object
         *
         */
        ~DistIntColumn() {
            delete array;
        }
};

/*************************************************************************
 * BoolColumn::
 * Holds bool values.
 */
class BoolColumn : public Column {
    public:
        EffBoolArr* array;
        
        /**
         * @brief Construct a new Bool Column object
         * 
         */
        BoolColumn() {
            array = new EffBoolArr();
            type = 'B';
        }

        /**
         * @brief Construct a new Bool Column object
         * 
         * @param from 
         */
        BoolColumn(BoolColumn& from) {
            array = new EffBoolArr(*from.array);
            type = 'B';
        }

        BoolColumn* clone(){
            return new BoolColumn(*this);
        };

        /**
         * @brief Destroy the Bool Column object
         * 
         */
        ~BoolColumn() {
            delete array;
        }

        /**
         * @brief get the bool at the index in the bool column
         * 
         * @param idx 
         * @return true 
         * @return false 
         */
        bool get(size_t idx) {
            return array->get(idx);
        }

        /**
         * @brief return this bool column as a bool column
         * 
         * @return BoolColumn* 
         */
        BoolColumn* as_bool() {
            return this;
        }

        /**
         * @brief throw erorr since this is a bool column
         * 
         * @return IntColumn* 
         */
        IntColumn* as_int() {
            assert(false);
        }

        /**
         * @brief throw error since this is a bool column
         * 
         * @return FloatColumn* 
         */
        FloatColumn* as_float() {
            assert(false);
        }

        /**
         * @brief throw error since this is a bool column
         * 
         * @return StringColumn* 
         */
        StringColumn* as_string() {
            assert(false);
        }

        /**
         * @brief set the val at the given idx to the new val
         * 
         * @param idx 
         * @param val 
         */
        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, bool val) {
            array->set(idx, val);
        }

        /**
         * @brief push back this val to the bool column
         * 
         * @param val 
         */
        void push_back(bool val) {
          array->pushBack(val);
        }

        /**
         * @brief return the size of this bool column
         * 
         * @return size_t 
         */
        size_t size() {
            return array->size();
        }
};

/*************************************************************************
 * DistBoolColumn::
 * Holds bool values in a distributed network.
 */
class DistBoolColumn : public DistColumn {
    public:
        DistEffBoolArr* array;

        /**
         * @brief Construct a new Int Column object
         *
         */
        DistBoolColumn(String* id, Distributable* kv_store, size_t metadata_node) {
            array = new DistEffBoolArr(id, kv_store, metadata_node);
            type = 'B';
        }

        /**
         * @brief Construct a new Int Column object
         *
         * @param from
         */
        DistBoolColumn(BoolColumn& from, String* id, Distributable* kv_store, size_t metadata_node) {
            array = new DistEffBoolArr(*from.array, id, kv_store, metadata_node);
            type = 'B';
        }

        /**
         * @brief get the int at the given index
         *
         * @param idx
         * @return int
         */
        bool get(size_t idx) {
            return array->get(idx);
        }

        /**
         * @brief return this int column as an int column
         *
         * @return IntColumn*
         */
        DistIntColumn* as_int() {
            assert(false);
        }

        /**
         * @brief throw error since this is an int column
         *
         * @return FloatColumn*
         */
        DistFloatColumn* as_float() {
            assert(false);
        }

        /**
         * @brief throw error since this is an int column
         *
         * @return BoolColumn*
         */
        DistBoolColumn* as_bool() {
            return this;
        }

        /**
         * @brief throw error since this is an int column
         *
         * @return StringColumn*
         */
        DistStringColumn* as_string() {
            assert(false);
        }

        /**
         * @brief get the size of this int column
         *
         * @return size_t
         */
        size_t size() {
            return array->size();
        }

        /**
         * @brief Destroy the Int Column object
         *
         */
        ~DistBoolColumn() {
            delete array;
        }
};

/*************************************************************************
 * FloatColumn::
 * Holds float values.
 */
class FloatColumn : public Column {
    public:
        EffFloatArr* array;

        /**
         * @brief Construct a new Float Column object
         * 
         */
        FloatColumn() {
            array = new EffFloatArr();
            type = 'F';
        }

        /**
         * @brief Construct a new Float Column object
         * 
         * @param from 
         */
        FloatColumn(FloatColumn& from) {
            array = new EffFloatArr(*from.array);
            type = 'F';
        }

        /**
         * @brief Destroy the Float Column object
         * 
         */
        ~FloatColumn() {
            delete array;
        }

        FloatColumn* clone(){
            return new FloatColumn(*this);
        };

        /**
         * @brief get the float at the given index
         * 
         * @param idx the index
         * @return float 
         */
        float get(size_t idx) {
            return array->get(idx);
        }

        /**
         * @brief return this float column as a float column
         * 
         * @return FloatColumn* 
         */
        FloatColumn* as_float() {
            return this;
        }

          /**
         * @brief throw error since this is a float column
         * 
         * @return IntColumn* 
         */
        IntColumn* as_int() {
            assert(false);
        }

        /**
         * @brief throw error since this is a float column 
         * 
         * @return BoolColumn* 
         */
        BoolColumn* as_bool() {
            assert(false);
        }

        /**
         * @brief throw error since this is a float column
         * 
         * @return StringColumn* 
         */
        StringColumn* as_string() {
            assert(false);
        }

        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, float val) {
            array->set(idx, val);
        }

        /**
         * @brief push back the val to this column
         * 
         * @param val 
         */
        void push_back(float val) {
          array->pushBack(val);
        }

        /**
         * @brief return the size of this column
         * 
         * @return size_t 
         */
        size_t size() {
            return array->size();
        }
};

/*************************************************************************
 * DistFloatColumn::
 * Holds float values in a distributed network.
 */
class DistFloatColumn : public DistColumn {
    public:
        DistEffFloatArr* array;

        /**
         * @brief Construct a new Int Column object
         *
         */
        DistFloatColumn(String* id, Distributable* kv_store, size_t metadata_node) {
            array = new DistEffFloatArr(id, kv_store, metadata_node);
            type = 'F';
        }

        /**
         * @brief Construct a new Int Column object
         *
         * @param from
         */
        DistFloatColumn(FloatColumn& from, String* id, Distributable* kv_store, size_t metadata_node) {
            array = new DistEffFloatArr(*from.array, id, kv_store, metadata_node);
            type = 'F';
        }

        /**
         * @brief get the int at the given index
         *
         * @param idx
         * @return int
         */
        float get(size_t idx) {
            return array->get(idx);
        }

        /**
         * @brief return this int column as an int column
         *
         * @return IntColumn*
         */
        DistIntColumn* as_int() {
            assert(false);
        }

        /**
         * @brief throw error since this is an int column
         *
         * @return FloatColumn*
         */
        DistFloatColumn* as_float() {
            return this;
        }

        /**
         * @brief throw error since this is an int column
         *
         * @return BoolColumn*
         */
        DistBoolColumn* as_bool() {
            assert(false);
        }

        /**
         * @brief throw error since this is an int column
         *
         * @return StringColumn*
         */
        DistStringColumn* as_string() {
            assert(false);
        }

        /**
         * @brief get the size of this int column
         *
         * @return size_t
         */
        size_t size() {
            return array->size();
        }

        /**
         * @brief Destroy the Int Column object
         *
         */
        ~DistFloatColumn() {
            delete array;
        }
};

/*************************************************************************
 * StringColumn::
 * Holds String values.
 */
class StringColumn : public Column {
    public:
        EffStrArr* array;

        /**
         * @brief Construct a new String Column object
         * 
         */
        StringColumn() {
            array = new EffStrArr();
            type = 'S';
        }

        /**
         * @brief Construct a new String Column object
         * 
         * @param from 
         */
        StringColumn(StringColumn& from) {
            array = new EffStrArr(*from.array);
            type = 'S';
        }

        /**
         * @brief Destroy the String Column object
         * 
         */
        ~StringColumn() {
            delete array;
        }

        StringColumn* clone(){
            return new StringColumn(*this);
        };

        /**
         * @brief get the string at the given index
         * 
         * @param idx 
         * @return String* 
         */
        String* get(size_t idx) {
            return array->get(idx);
        }

        /**
         * @brief return this string column as a string column
         * 
         * @return StringColumn* 
         */
        StringColumn* as_string() {
            return this;
        }

          /**
         * @brief throw error since this is a string column
         * 
         * @return IntColumn* 
         */
        IntColumn* as_int() {
            assert(false);
        }

        /**
         * @brief throw error since this is a string column
         * 
         * @return FloatColumn* 
         */
        FloatColumn* as_float() {
            assert(false);
        }

        /**
         * @brief throw error since this is a string column 
         * 
         * @return BoolColumn* 
         */
        BoolColumn* as_bool() {
            assert(false);
        }

        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, String* val) {
            array->set(idx, val);
        }

        /**
         * @brief add the val to this string column
         * 
         * @param val 
         */
        void push_back(String* val) {
          array->pushBack(val);
        }

        /**
         * @brief get the size of this string column
         * 
         * @return size_t 
         */
        size_t size() {
            return array->size();
        }
};

/*************************************************************************
 * DistStringColumn::
 * Holds String values in a distributed network.
 */
class DistStringColumn : public DistColumn {
    public:
        DistEffStrArr* array;

        /**
         * @brief Construct a new Int Column object
         *
         */
        DistStringColumn(String* id, Distributable* kv_store, size_t metadata_node) {
            array = new DistEffStrArr(id, kv_store, metadata_node);
            type = 'S';
        }

        /**
         * @brief Construct a new Int Column object
         *
         * @param from
         */
        DistStringColumn(StringColumn& from, String* id, Distributable* kv_store, size_t metadata_node) {
            array = new DistEffStrArr(*from.array, id, kv_store, metadata_node);
            type = 'S';
        }

        /**
         * @brief get the int at the given index
         *
         * @param idx
         * @return int
         */
        String* get(size_t idx) {
            return array->get(idx);
        }

        /**
         * @brief return this int column as an int column
         *
         * @return IntColumn*
         */
        DistIntColumn* as_int() {
            assert(false);
        }

        /**
         * @brief throw error since this is an int column
         *
         * @return FloatColumn*
         */
        DistFloatColumn* as_float() {
            assert(false);
        }

        /**
         * @brief throw error since this is an int column
         *
         * @return BoolColumn*
         */
        DistBoolColumn* as_bool() {
            assert(false);
        }

        /**
         * @brief throw error since this is an int column
         *
         * @return StringColumn*
         */
        DistStringColumn* as_string() {
            return this;
        }

        /**
         * @brief get the size of this int column
         *
         * @return size_t
         */
        size_t size() {
            return array->size();
        }

        /**
         * @brief Destroy the Int Column object
         *
         */
        ~DistStringColumn() {
            delete array;
        }
};

/**
 * Represents an array of Columns on distributed nodes.
 */
class FixedColArray : public Object {
	public:
		FixedArray* array;

		/**
		 * Default constructor of this array.
		 */
		FixedColArray() : Object() {
			array = new FixedArray();
		}


		/**
		 * Constructor of this array of floats.
		 *
		* @param size the base size of this array
		*/
		FixedColArray(size_t size) : Object() {
			array = new FixedArray(size);
		}

        /**
         * @brief Construct a new Fixed Col Array object
         * 
         * @param from 
         */
        FixedColArray(FixedColArray& from) : FixedColArray(from.size()) {
            for (int i = 0; i < from.numElements(); i += 1) {
                Column* col = from.get(i);
                if (col->get_type() == 'I') {
                    IntColumn* copy = new IntColumn(*col->as_int());
                    set(i, copy);
                } else if (col->get_type() == 'F') {
                    FloatColumn* copy = new FloatColumn(*col->as_float());
                    set(i, copy);
                } else if (col->get_type() == 'B') {
                    BoolColumn* copy = new BoolColumn(*col->as_bool());
                    set(i, copy);
                } else {
                    StringColumn* copy = new StringColumn(*col->as_string());
                    set(i, copy);
                }
            }
        }

        FixedColArray* clone() {
            return new FixedColArray(*this);
        }


		/**
		 * Returns an element at the given index.
		 *
		 * @param index the index of the element
		 * @return the element of the array at the given index
		 */
		virtual Column* get(size_t index) {
			Column* element = dynamic_cast <Column *> (array->get(index));
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
		virtual void pushBack(Column* item) {
			array->pushBack(item);
		}

        /**
         * @brief sets the column at the index to item
         * 
         * @param index 
         * @param item 
         */
		void set(size_t index, Column* item) {
			array->set(index, item);
		}

        /**
         * @brief return the size of this array
         * 
         * @return size_t 
         */
		size_t size() {
			return array->capacity;
		}

        /**
         * @brief get the number of elements in this arrays
         * 
         * @return size_t 
         */
        size_t numElements() {
          return array->used;
        }


		/**
		 * Destructor of this class.
		 */
		~FixedColArray() {
		    for (int i = 0; i < numElements(); i += 1) {
		        if (array->get(i) != nullptr) {
                    delete array->get(i);
                }
		    }
			delete array;
		}
};

/**
 * Represents an array of Columns.
 */
class DistFixedColArray : public Object {
    public:
        String* id;
        size_t used;
        size_t capacity;
        DistColumn** array;

        /**
         * Default constructor of this array.
         */
        DistFixedColArray(FixedColArray &from, String* id_var, Distributable* kvStore_var) : Object() {
            id = id_var->clone();
            used = from.array->used;
            capacity = from.array->capacity;
            kvStore_var->put(0, id->clone()->concat("-used"), used);
            kvStore_var->put(0, id->clone()->concat("-capacity"), capacity);
            array = new DistColumn*[capacity];
            for (size_t i = 0; i < used; i += 1) {
                Column* col = from.get(i);
                String* col_id = id_var->clone();
                col_id->concat("-");
                col_id->concat(i);
                if (col->get_type() == 'I') {
                    array[i] = new DistIntColumn(*col->as_int(), col_id, kvStore_var, i % 5);
                } else if (col->get_type() == 'F') {
                    array[i] = new DistFloatColumn(*col->as_float(), col_id, kvStore_var, i % 5);
                } else if (col->get_type() == 'B') {
                    array[i] = new DistBoolColumn(*col->as_bool(), col_id, kvStore_var, i % 5);
                } else {
                    array[i] = new DistStringColumn(*col->as_string(), col_id, kvStore_var, i % 5);
                }
                delete col_id;
            }
        }


        /**
         * Default constructor of this array.
         */
        DistFixedColArray(DistEffCharArr* types, String* id_var, Distributable* kvStore_var) : Object() {
            id = id_var->clone();
            used = kvStore_var->get_size_t(0, id->clone()->concat("-used"));
            capacity = kvStore_var->get_size_t(0, id->clone()->concat("-capacity"));
            array = new DistColumn*[capacity];
            for (size_t i = 0; i < used; i += 1) {
                String* col_id = id_var->clone();
                col_id->concat("-");
                col_id->concat(i);
                if (types->get(i) == 'I') {
                    DistIntColumn* copy = new DistIntColumn(col_id, kvStore_var, i % 5);
                    array[i] = copy;
                } else if (types->get(i) == 'F') {
                    DistFloatColumn* copy = new DistFloatColumn(col_id, kvStore_var, i % 5);
                    array[i] = copy;
                } else if (types->get(i) == 'B') {
                    DistBoolColumn* copy = new DistBoolColumn(col_id, kvStore_var, i % 5);
                    array[i] = copy;
                } else {
                    DistStringColumn* copy = new DistStringColumn(col_id, kvStore_var, i % 5);
                    array[i] = copy;
                }
                delete col_id;
            }
        }


        /**
         * Returns an element at the given index.
         *
         * @param index the index of the element
         * @return the element of the array at the given index
         */
        virtual DistColumn* get(size_t index) {
            DistColumn* element = dynamic_cast <DistColumn*> (array[index]);
            return element;
        }

        /**
         * @brief return the size of this array
         *
         * @return size_t
         */
        size_t size() {
            return capacity;
        }

        /**
         * @brief get the number of elements in this arrays
         *
         * @return size_t
         */
        size_t numElements() {
            return used;
        }


        /**
         * Destructor of this class.
         */
        ~DistFixedColArray() {
            delete id;
            for (size_t i = 0; i < used; i += 1) {
                delete array[i];
            }
            delete[] array;
        }
};

/*************************************************************************
 * EffColArr::
 * Holds Columns
 */
class EffColArr : public Object {
    public:
        size_t chunkSize = 50;
        size_t capacity = 1;
        size_t currentChunkIdx = 0;
        size_t numberOfElements = 0;
        FixedColArray** chunks;

        /**
         * @brief Construct a new Eff Col Arr object
         * 
         */
        EffColArr() {
            chunks = new FixedColArray*[capacity];
            for (int i = 0; i < capacity; i += 1) {
              chunks[i] = new FixedColArray(chunkSize);
            }
        }

        /**
         * @brief Construct a new Eff Col Arr object from another eff col arr objects
         * 
         * @param from 
         */
        EffColArr(EffColArr& from) {
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;
            chunks = new FixedColArray*[capacity];
            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedColArray(*from.chunks[i]);
            }
        }
        
        /**
         * @brief Extend the size of this col array by copying pointers to chunks
         * 
         */
        void extendSize() {
            FixedColArray** newChunks = new FixedColArray*[capacity * 2];
            int i = 0;
            for (i; i < capacity; i += 1) {
                newChunks[i] = chunks[i];
            }
            capacity *= 2;
            for (i; i < capacity; i += 1) {
                newChunks[i] = new FixedColArray(chunkSize);
            }
            delete[] chunks;
            chunks = newChunks;
        }

        /**
         * @brief add a column to this efficient col array
         * 
         * @param value 
         */
        void pushBack(Column* value) {
            FixedColArray* currentChunk = chunks[currentChunkIdx];
            if (currentChunk->size() == currentChunk->numElements()) {
                currentChunkIdx += 1;
                if (currentChunkIdx == capacity) {
                    extendSize();
                }
                currentChunk = chunks[currentChunkIdx];
            }
            currentChunk->pushBack(value);
            numberOfElements += 1;
        }

        /**
         * @brief Get the column at the given index
         * 
         * @param idx 
         * @return Column* 
         */
        Column* get(size_t idx) {

            size_t chunkIdx = idx / chunkSize;

            return chunks[chunkIdx]->get(idx % chunkSize);
        }


        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, Column* val) {
            size_t chunkIdx = idx / chunkSize;

            chunks[chunkIdx]->set(idx % chunkSize, val);
        }

        /**
         * @brief get the size of this column
         * 
         * @return size_t 
         */
        size_t size() {
            return numberOfElements;
        }

        /**
         * @brief Destroy the Eff Col Arr object
         * 
         */
        ~EffColArr() {
          for (int i = 0; i < capacity; i += 1) {
            delete chunks[i];
          }
          delete[] chunks;
        }
};


/*************************************************************************
 * DistEffColArr:
 * Holds Columns in a distributed network
 */
class DistEffColArr : public Object {
    public:
        size_t chunkSize;
        size_t capacity;
        size_t currentChunkIdx;
        size_t numberOfElements;
        String* id;
        Distributable* kvStore;
        DistFixedColArray** array;

        /**
         * @brief Construct a new Eff Col Arr object
         *
         */
        DistEffColArr(DistEffCharArr* types, String* id_var, Distributable* kvStore_var) {
            id = id_var->clone();
            kvStore = kvStore_var;
            chunkSize = kvStore->get_size_t(0, id->clone()->concat("-chunkSize"));
            capacity = kvStore->get_size_t(0, id->clone()->concat("-capacity"));
            currentChunkIdx = kvStore->get_size_t(0, id->clone()->concat("-currentChunk"));
            numberOfElements = kvStore->get_size_t(0, id->clone()->concat("-numElements"));
            array = new DistFixedColArray*[capacity];
            for (size_t i = 0; i < capacity; i += 1) {
                String* col_id = id->clone()->concat("-")->concat(i);
                array[i] = new DistFixedColArray(types, col_id, kvStore);
                delete col_id;
            }
        }

        /**
         * @brief Construct a new Eff Col Arr object from another eff col arr objects
         *
         * @param from
         */
        DistEffColArr(EffColArr& from, String* id_var, Distributable* kvStore_var) {
            id = id_var->clone();
            kvStore = kvStore_var;
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;
            array = new DistFixedColArray*[capacity];
            kvStore->put(0, id->clone()->concat("-chunkSize"), chunkSize);
            kvStore->put(0, id->clone()->concat("-capacity"), capacity);
            kvStore->put(0, id->clone()->concat("-currentChunk"), currentChunkIdx);
            kvStore->put(0, id->clone()->concat("-numElements"), numberOfElements);
            for (size_t i = 0; i < capacity; i += 1) {
                String* col_id = id->clone()->concat("-")->concat(i);
                array[i] = new DistFixedColArray(*from.chunks[i], col_id, kvStore);
                delete col_id;
            }
        }

        /**
         * @brief Get the column at the given index
         *
         * @param idx
         * @return Column*
         */
        DistColumn* get(size_t idx) {
            size_t chunkIdx = idx / chunkSize;
            DistFixedColArray* curChunk = array[chunkIdx];
            return curChunk->get(idx % chunkSize);
        }

        /**
         * @brief get the size of this column
         *
         * @return size_t
         */
        size_t size() {
            return numberOfElements;
        }

        /**
         * @brief Destroy the Eff Col Arr object
         *
         */
        ~DistEffColArr() {
            delete id;
            for (size_t i = 0; i < capacity; i += 1) {
                delete array[i];
            }
            delete[] array;
        }
};