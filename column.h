#pragma once
#include <stdlib.h>
#include "string.h"
#include <stdarg.h>
#include "efficientArray.h"


class IntColumn;
class BoolColumn;
class FloatColumn;
class StringColumn;


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

    }
    virtual BoolColumn*  as_bool() {

    }
    virtual FloatColumn* as_float() {

    }
    virtual StringColumn* as_string() {

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

    }

    /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'.*/
    char get_type() {
      return type;
    }

    virtual ~Column() {

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
         * @brief Construct a new Int Column object from a variable # of args
         * 
         * @param n - the number of args
         * @param ... 
         */
        IntColumn(int n, ...) {
            array = new EffIntArr();
            va_list argp;
            va_start(argp, n);
            for(size_t i = 0; i < n; i++) {
                array->pushBack(va_arg(argp, int));
            }
            va_end(argp);
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
         * @brief Construct a new Bool Column object from a variable number of arguments
         * 
         * @param - number of args
         * @param ... 
         */
        BoolColumn(int n, ...) {
            array = new EffBoolArr();
            va_list argp;
            va_start(argp, n);
            for(size_t i = 0; i < n; i++) {
                array->pushBack(va_arg(argp, bool));
            }
            va_end(argp);
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
         * @brief Construct a new Float Column object from a variable number of floats
         * 
         * @param n - the  number of args
         * @param ... 
         */
        FloatColumn(int n, ...) {
            array = new EffFloatArr();
            va_list argp;
            va_start(argp, n);
            for(size_t i = 0; i < n; i++) {
                array->pushBack(va_arg(argp, float));
            }
            va_end(argp);
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
         * @brief Construct a new String Column object from a variable number of strings
         * 
         * @param n 
         * @param ... 
         */
        StringColumn(int n, ...) {
            array = new EffStrArr();
            va_list argp;
            va_start(argp, n);
            for(size_t i = 0; i < n; i++) {
              char* str = va_arg(argp, char*);
              size_t len = strlen(str);
              array->pushBack(new String(true, str, len));
            }
            va_end(argp);
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

/**
 * Represents an array of Columns.
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
            for (int i = 0; i < from.size(); i += 1) {
                Column* col = from.get(i);
                Column* copy = new Column(*col);
                set(i, copy);
            }
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
         * @brief get the index of item in this array
         * 
         * @param item 
         * @return int 
         */
		int indexOf(Column* item) {
			return array->indexOf(item);
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
         * @brief get the index of the item in this col array
         * 
         * @param item 
         * @return int 
         */
        int indexOf(Column* item) {
            for (int i = 0; i <= currentChunkIdx; i += 1) {
                int indexOf = chunks[i]->indexOf(item);
                if (indexOf != -1) {
                    return indexOf;
                }
            }
            return -1;
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
 * EffColArr::
 * Holds Columns
 */
class DistEffColArr : public Object {
    public:
        size_t chunkSize = 50;
        size_t capacity = 1;
        size_t currentChunkIdx = 0;
        size_t numberOfElements = 0;
        FixedColArray** chunks;

        int numNodes;

        /**
         * @brief Construct a new Eff Col Arr object
         *
         */
        DistEffColArr(int numberNodes) {
            chunks = new FixedColArray*[capacity];
            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedColArray(chunkSize);
            }
            numNodes = numberNodes;
        }

        /**
         * @brief Construct a new Eff Col Arr object from another eff col arr objects
         *
         * @param from
         */
        DistEffColArr(DistEffColArr& from) { // TODO fix this
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
        void extendSize() { // TODO fix this
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
        void pushBack(Column* value) { // TODO fix this
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
            int nodeOn = chunkIdx % numNodes;
            Column toReturn;
            // send request to node nodeOn to get Column at idx
            // toReturn = getColumnFromNode(nodeOn, idx % chunkSize)
            return &toReturn;
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
         * @brief get the index of the item in this col array
         *
         * @param item
         * @return int
         */
        int indexOf(Column* item) {
            for (int i = 0; i <= currentChunkIdx; i += 1) {
                int indexOf = chunks[i]->indexOf(item);
                if (indexOf != -1) {
                    return indexOf;
                }
            }
            return -1;
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