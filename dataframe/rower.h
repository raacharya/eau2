#pragma once

/*******************************************************************************
 *  Rower::
 *  An interface for iterating through each row of a data frame. The intent
 *  is that this class should subclassed and the accept() method be given
 *  a meaningful implementation. Rowers can be cloned for parallel execution.
 */
class Rower : public Object {
    public:

        /** This method is called once per row. The row object is on loan and
            should not be retained as it is likely going to be reused in the nextSizeT
            call. The return value is used in filters to indicate that a row
            should be kept. */
        virtual bool accept(Row &r) {

        }

        /** Once traversal of the data frame is complete the rowers that were
            split off will be joined.  There will be one join per split. The
            original object will be the last to be called join on. The join method
            is reponsible for cleaning up memory. */
        void join_delete(Rower *other) {
            join(other);
            delete other;
        }

        virtual void join(Rower* other) {

        }

        virtual Rower* clone() {

        }
};

class Writer: public Rower {

public:

    virtual void visit(Row& r) {
        assert(false);
    }

    virtual bool done() {
        assert(false);
    }
};

class Reader: public Rower  {
public:

    virtual bool visit(Row& r) {
        assert(false);
    }

    virtual bool done() {
        assert(false);
    }
};
