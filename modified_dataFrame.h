#include "object.h"
#include "schema.h"
#include "array.h"
#include "column.h"
#include "row.h"
#include "rower.h"
#include <thread>

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 */
class ModifiedDataFrame : public Object {

    public:

        Schema *schema;
        EffColArr *columns;

        /** Create a data frame with the same columns as the given df but with no rows or rownmaes */
        ModifiedDataFrame(ModifiedDataFrame &df) : ModifiedDataFrame(*df.schema) {
        }

        /** Create a data frame from a schema and columns. All columns are created
          * empty. */
        ModifiedDataFrame(Schema &schemaRef) {
            schema = new Schema();
            columns = new EffColArr();
            for (int i = 0; i < schemaRef.types->size(); i += 1) {
                char curType = schemaRef.types->get(i);
                schema->add_column(curType, nullptr);
                if (curType == 'I') {
                    columns->pushBack(new IntColumn());
                } else if (curType == 'F') {
                    columns->pushBack(new FloatColumn());
                } else if (curType == 'B') {
                    columns->pushBack(new BoolColumn());
                } else {
                    columns->pushBack(new StringColumn());
                }
            }
        }

        /** Returns the dataframe's schema. Modifying the schema after a dataframe
          * has been created in undefined. */
        Schema &get_schema() {
            return *schema;
        }

        /** Adds a column this dataframe, updates the schema, the new column
          * is external, and appears as the last column of the dataframe, the
          * name is optional and external. A nullptr colum is undefined. */
        void add_column(Column *col, String *name) {
            assert(columns->size() == 0 || columns->get(0)->size() == 0);
            Column *copy;
            if (col->get_type() == 'I') {
                copy = new IntColumn(*(col->as_int()));
            } else if (col->get_type() == 'F') {
                copy = new FloatColumn(*(col->as_float()));
            } else if (col->get_type() == 'B') {
                copy = new BoolColumn(*(col->as_bool()));
            } else {
                copy = new StringColumn(*(col->as_string()));
            }
            columns->pushBack(copy);
            schema->add_column(copy->get_type(), name);
        }

        /** Return the value at the given column and row. Accessing rows or
         *  columns out of bounds, or request the wrong type is undefined.*/
        int get_int(size_t col, size_t row) {
            assert(0 <= col && col < schema->width());
            assert(0 <= row && row < schema->length());
            assert(columns->get(col)->get_type() == 'I');
            return columns->get(col)->as_int()->get(row);
        }

        bool get_bool(size_t col, size_t row) {
            assert(0 <= col && col < schema->width());
            assert(0 <= row && row < schema->length());
            assert(columns->get(col)->get_type() == 'B');
            return columns->get(col)->as_bool()->get(row);
        }

        float get_float(size_t col, size_t row) {
            assert(0 <= col && col < schema->width());
            assert(0 <= row && row < schema->length());
            assert(columns->get(col)->get_type() == 'F');
            return columns->get(col)->as_float()->get(row);
        }

        String *get_string(size_t col, size_t row) {
            assert(0 <= col && col < schema->width());
            assert(0 <= row && row < schema->length());
            assert(columns->get(col)->get_type() == 'S');
            return columns->get(col)->as_string()->get(row);
        }

        /** Return the offset of the given column name or -1 if no such col. */
        int get_col(String &col) {
            return schema->col_idx(col.cstr_);
        }

        /** Return the offset of the given row name or -1 if no such row. */
        int get_row(String &col) {
            return schema->row_idx(col.cstr_);
        }

        /** Set the value at the given column and row to the given value.
          * If the column is not  of the right type or the indices are out of
          * bound, the result is undefined. */
        void set(size_t col, size_t row, int val) {
            assert(0 <= col && col < schema->width());
            assert(0 <= row && row < schema->length());
            assert(columns->get(col)->get_type() == 'I');
            columns->get(col)->as_int()->set(row, val);
        }

        void set(size_t col, size_t row, bool val) {
            assert(0 <= col && col < schema->width());
            assert(0 <= row && row < schema->length());
            assert(columns->get(col)->get_type() == 'B');
            columns->get(col)->as_bool()->set(row, val);
        }

        void set(size_t col, size_t row, float val) {
            assert(0 <= col && col < schema->width());
            assert(0 <= row && row < schema->length());
            assert(columns->get(col)->get_type() == 'F');
            columns->get(col)->as_float()->set(row, val);
        }

        void set(size_t col, size_t row, String *val) {
            assert(0 <= col && col < schema->width());
            assert(0 <= row && row < schema->length());
            assert(columns->get(col)->get_type() == 'S');
            columns->get(col)->as_string()->set(row, val);
        }

        /** Set the fields of the given row object with values from the columns at
          * the given offset.  If the row is not form the same schema as the
          * dataframe, results are undefined.
          */
        void fill_row(size_t idx, Row &row) {
            for (int col = 0; col < columns->size(); col++) {
                Column *column = columns->get(col);
                char colType = column->get_type();
                if (colType == 'I') {
                    row.set(col, column->as_int()->get(idx));
                } else if (colType == 'B') {
                    row.set(col, column->as_bool()->get(idx));
                } else if (colType == 'F') {
                    row.set(col, column->as_float()->get(idx));
                } else {
                    row.set(col, column->as_string()->get(idx));
                }
            }
        }

        /** Add a row at the end of this dataframe. The row is expected to have
         *  the right schema and be filled with values, otherwise undedined.  */
        void add_row(Row &row) {
            for (size_t col = 0; col < columns->size(); col++) {
                Column *column = columns->get(col);
                char colType = column->get_type();
                if (colType == 'I') {
                    column->as_int()->push_back(row.get_int(col));
                } else if (colType == 'B') {
                    column->as_bool()->push_back(row.get_bool(col));
                } else if (colType == 'F') {
                    column->as_float()->push_back(row.get_float(col));
                } else {
                    column->as_string()->push_back(row.get_string(col));
                }
            }

            row.set_idx(schema->length());
            schema->add_row(nullptr);
        }

        /** The number of rows in the dataframe. */
        size_t nrows() {
            return schema->length();
        }

        /** The number of columns in the dataframe.*/
        size_t ncols() {
            return schema->width();
        }

        /** Visit rows in order */
        void map(Rower &r) {
            vmap(&r, 0, schema->length());
        }

        /** Visit rows from start (inclusive) to end (exclusive) */
        void vmap(Rower* r, size_t start, size_t end) {
            for(size_t i = start; i < end; i++) {
                Row* row = new Row(*schema);
                row->set_idx(i);
                fill_row(i, *row);
                r->accept(*row);
                for(size_t j = 0; j < row->width(); j++) {
                    char colType = columns->get(j)->get_type();
                    if (colType == 'I') {
                        set(j, i, row->get_int(j));
                    } else if (colType == 'B') {
                        set(j, i, row->get_bool(j));
                    } else if (colType == 'F') {
                        set(j, i, row->get_float(j));
                    } else {
                        set(j, i, row->get_string(j));
                    }
                }
                delete row;
            }
        }


        /**
         * @brief Use multithreading to improve the performance of map and use concurrency
         * 
         * @param r 
         */
        void pmap(Rower& r) {
            int numThreads = 10;
            std::thread pool[numThreads];
            Rower* rowerArr[numThreads];
            size_t len = schema->length();
            size_t interval = len / numThreads;
            for (int i = 0; i < numThreads; i += 1) {
                size_t start = interval * i;
                size_t end = std::min(start + interval, len);
                rowerArr[i] = r.clone();
                pool[i] = std::thread(&ModifiedDataFrame::vmap, this, rowerArr[i], start, end);
            }
            for (int i = 0; i < numThreads; i += 1) {
                pool[i].join();
            }
            for (int i = numThreads - 2; i >= 0; i -= 1) {
                rowerArr[i]->join(rowerArr[i + 1]);
            }
            r.join_delete(rowerArr[0]);
        }

        /** Create a new dataframe, constructed from rows for which the given Rower
          * returned true from its accept method. */
        ModifiedDataFrame *filter(Rower &r) {
            ModifiedDataFrame* newDf = new ModifiedDataFrame(*schema);
            for(size_t i = 0; i < schema->length(); i++) {
                Row* row = new Row(*schema);
                fill_row(i, *row);
                if(r.accept(*row)) {
                    newDf->add_row(*row);
                }
                else {
                    delete row;
                }
            }
            return newDf;
        }

        /** Print the dataframe in SoR format to standard output. */
        void print() {
            PrintRower* pr = new PrintRower();
            map(*pr);
            delete pr;
        }

        ~ModifiedDataFrame() {
            delete columns;
            delete schema;
        }
};