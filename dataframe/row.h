#pragma once

#include "../array/efficientArray.h"
#include "schema.h"
#include "fielder.h"

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
        EffCharArr *types;
        int index;

        /** Build a row following a schema. */
        Row(Schema &scm) : Object() {
            index = -1;
            types = new EffCharArr(*scm.types);
            values = new EffTypeArr();
            for (int i = 0; i < types->size(); i += 1) {
                Type *value = new Type;
                values->pushBack(value);
            }
        }

        /** Setters: set the given column with the given value. Setting a column with
          * a value of the wrong type throws an error. */
        void set(size_t col, int val) {
            assert(col < types->size());
            assert(types->get(col) == 'I');
            values->get(col)->i = val;
        }

        void set(size_t col, float val) {
            assert(col < types->size());
            assert(types->get(col) == 'F');
            values->get(col)->f = val;
        }

        void set(size_t col, bool val) {
            assert(col < types->size());
            assert(types->get(col) == 'B');
            values->get(col)->b = val;
        }

        /** The String is external */
        void set(size_t col, String *val) {
            assert(col < types->size());
            assert(types->get(col) == 'S');
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
            assert(col < width());
            assert(types->get(col) == 'I');
            return values->get(col)->i;
        }

        bool get_bool(size_t col) {
            assert(col < width());
            assert(types->get(col) == 'B');
            return values->get(col)->b;
        }

        float get_float(size_t col) {
            assert(col < width());
            assert(types->get(col) == 'F');
            return values->get(col)->f;
        }

        String *get_string(size_t col) {
            assert(col < width());
            assert(types->get(col) == 'S');
            return values->get(col)->s;
        }

        /** Number of fields in the row. */
        size_t width() {
            return types->numberOfElements;
        }

        /** Type of the field at the given position. An idx >= width throws an error. */
        char col_type(size_t idx) {
            assert(idx < width());
            return types->get(idx);
        }

        /** Given a Fielder, visit every field of this row. The first argument is
          * index of the row in the dataframe.
          * Calling this method before the row's fields have been set is undefined. */
        void visit(size_t idx, Fielder &f) {
            f.start(idx);
            for (size_t i = 0; i < width(); i += 1) {
                char curType = types->get(i);
                if (curType == 'I') {
                    f.accept(values->get(i)->i);
                } else if (curType == 'F') {
                    f.accept(values->get(i)->f);
                } else if (curType == 'B') {
                    f.accept(values->get(i)->b);
                } else {
                    f.accept(values->get(i)->s);
                }
            }
            f.done();
        }

        ~Row() {
            delete values;
            delete types;
        }
};