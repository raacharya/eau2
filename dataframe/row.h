#pragma once

#include "../array/efficientArray.h"
#include "schema.h"

/*************************************************************************
 * Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a dataframe hold data in columns.
 * Rows have pointer equality.
 */
class Row : public Object {
    public:
        EffTypeArr *values;
        int index;

        Row(size_t size) : Object() {
            index = -1;
            values = new EffTypeArr();
            for (int i = 0; i < size; i += 1) {
                Type *value = new Type;
                values->pushBack(value);
            }
        }

        /** Setters: set the given column with the given value. Setting a column with
          * a value of the wrong type throws an error. */
        void set(size_t col, int val) {
            values->get(col)->i = val;
        }

        void set(size_t col, float val) {
            values->get(col)->f = val;
        }

        void set(size_t col, bool val) {
            values->get(col)->b = val;
        }

        /** The String is external */
        void set(size_t col, String *val) {
            values->get(col)->s = val;
        }

        /** Set/get the index of this row (ie. its position in the dataframe. This is
         *  only used for informational purposes, unused otherwise */
        void set_idx(size_t idx) {
            index = idx;
        }

        size_t get_idx() {
            return index;
        }

        /** Getters: get the value at the given column. If the column is not
          * of the requested type, we throw an error. */
        int get_int(size_t col) {
            return values->get(col)->i;
        }

        bool get_bool(size_t col) {
            return values->get(col)->b;
        }

        float get_float(size_t col) {
            return values->get(col)->f;
        }

        String *get_string(size_t col) {
            return values->get(col)->s;
        }

        /** Number of fields in the row. */
        size_t width() {
            return values->numberOfElements;
        }

        ~Row() {
            delete values;
        }
};