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
            should not be retained as it is likely going to be reused in the next
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

class PrintRower : public Rower {
    public:
        PrintFielder* pf;

        PrintRower() : Rower() {
            pf = new PrintFielder();
        }

        bool accept(Row &r) {
            r.visit(r.get_idx(), *pf);
            return true;
        }

        void join(Rower* other) {

        }

        PrintRower* clone() {
            return new PrintRower();
        }

        ~PrintRower() {
            delete pf;
        }
};

class AddIntsRower : public Rower {
    public:
        int count;

        AddIntsRower() : Rower() {
            count = 0;
        }

        bool accept(Row& r) {
            for (int i = 0; i < r.width(); i += 1) {
                char colType = r.types->get(i);
                if (colType == 'I') {
                    count += r.get_int(i);
                    r.set(i, r.get_int(i) * 2);
                }
            }
            return true;
        }

        void join(Rower* other) {
            AddIntsRower* o = dynamic_cast<AddIntsRower*>(other);
            count += o->count;
        }

        AddIntsRower* clone() {
            return new AddIntsRower();
        }
};

class ReverseStringRower : public Rower {
    public:

        ReverseStringRower() : Rower() {
        }

        bool accept(Row &r) {
            for(int i = 0; i < r.width(); i++) {
                char colType = r.types->get(i);
                if(colType == 'S') {
                    reverse(r.get_string(i));
                }
            }
            return true;
        }

        void reverse(String* s) {
            size_t l = 0;
            size_t r = s->size() - 1;

            while(l < r) {
                char temp = s->at(l);
                s->cstr_[l] = s->at(r);
                s->cstr_[r] = temp;
                l++;
                r--;
            }
        }

        void join(Rower* other) {

        }

        ReverseStringRower* clone() {
            return new ReverseStringRower();
        }
};

class EvenIntsOnlyRower : public Rower {
    public:
        bool accept(Row &r) {
            for(int i = 0; i < r.width(); i++) {
                char colType = r.types->get(i);
                if(colType != 'I' || r.get_int(i) % 2 != 0) {
                    return false;
                }
            }
            return true;
        }

        EvenIntsOnlyRower* clone() {
            return new EvenIntsOnlyRower();
        }
};

class AddOneRower : public Rower {
    public:
        bool accept(Row& r) {
            for (int i = 0; i < r.width(); i += 1) {
                char colType = r.types->get(i);
                if (colType == 'I') {
                    r.set(i, r.get_int(i) + 1);
                } else if (colType == 'F') {
                    r.set(i, r.get_float(i) + 1);
                }
            }
            return true;
        }

        AddOneRower* clone() {
            return new AddOneRower();
        }
};

class MultiRower : public Rower {
    public:
        bool accept(Row& r) {
            for(int i = 0; i < r.width(); i++) {
                char colType = r.types->get(i);
                if(colType == 'I') {
                    int curInt = r.get_int(i);
                    for (int j = 0; j < 100; j += 1) {
                        curInt *= curInt;
                        if (curInt != 0 ) curInt /= curInt;
                    }
                    r.set(i, curInt * curInt);
                } else if(colType == 'F') {
                    float curFloat = r.get_float(i);
                    for (int j = 0; j < 100; j += 1) {
                        curFloat *= curFloat;
                        curFloat /= curFloat;
                    }
                    r.set(i, curFloat * curFloat);
                } else if(colType == 'B') {
                    bool curBool = r.get_bool(i);
                    for (int j = 0; j < 100; j += 1) {
                        curBool = !curBool;
                        curBool = !curBool;
                    }
                    r.set(i, !curBool);
                } else if(colType == 'S') {
                    reverse(r.get_string(i));
                }
            }
            return true;
        }

        void reverse(String* s) {
            size_t l = 0;
            size_t r = s->size() - 1;

            while(l < r) {
                char temp = s->at(l);
                s->cstr_[l] = s->at(r);
                s->cstr_[r] = temp;
                l++;
                r--;
            }
        }

        AddOneRower* clone() {
            return new AddOneRower();
        }
};