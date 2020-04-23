#pragma once

#include "../dataframe/dataframe.h"
#include "../network/network.h"

class Application {
    public:
        KDStore* kd;
        size_t index;

        explicit Application(size_t idx, KDStore* kd_var) {
            index = idx;
            kd = kd_var;
        }

        virtual void run_() {
            assert(false);
        }
};

class Trivial : public Application {
    public:
        explicit Trivial(size_t idx, KDStore* kd_var) : Application(idx, kd_var) { }
        void run_() override {
            if (index == 2) {
                size_t SZ = 1000 * 1;
                auto *vals = new float[SZ];
                double sum = 0;
                for (size_t i = 0; i < SZ; ++i) {
                    vals[i] = i;
                    sum += i;
                }
                Key key("triv", 0);
                DistDataFrame *df = DistDataFrame::fromArray(&key, kd, SZ, vals);
                assert(df->get_float(0, 1) == 1);
                DistDataFrame *df2 = kd->get(key);
                for (size_t i = 0; i < SZ; ++i) {
                    float cur = df2->get_float(0, i);
                    sum -= cur;
                }
                assert(sum == 0);
                delete df;
                delete df2;
                delete[] vals;
            }
        }
};

class Writer {

    virtual void visit(Row& r);

    virtual bool done();
};

class Reader {

    virtual bool visit(Row& r);

    virtual bool done();
};

class FileReader : public Writer {

    public:

        /** Reads next word and stores it in the row. Actually read the word.
            While reading the word, we may have to re-fill the buffer  */
        void visit(Row & r) override {
            assert(i_ < end_);
            assert(! isspace(buf_[i_]));
            size_t wStart = i_;
            while (true) {
                if (i_ == end_) {
                    if (feof(file_)) { ++i_;  break; }
                    i_ = wStart;
                    wStart = 0;
                    fillBuffer_();
                }
                if (isspace(buf_[i_]))  break;
                ++i_;
            }
            buf_[i_] = 0;
            String word(buf_ + wStart, i_ - wStart);
            //change
            r.set(0, &word);
            ++i_;
            skipWhitespace_();
        }

        /** Returns true when there are no more words to read.  There is nothing
           more to read if we are at the end of the buffer and the file has
           all been read.     */
        bool done() override { return (i_ >= end_) && feof(file_);  }

        /** Creates the reader and opens the file for reading.  */
        FileReader( const char* filename) {
            file_ = fopen(filename, "r");
            if (file_ == nullptr) FATAL_ERROR("Cannot open file");
            buf_ = new char[BUFSIZE + 1]; //  null terminator
            fillBuffer_();
            skipWhitespace_();
        }

        static const size_t BUFSIZE = 1024;

        /** Reads more data from the file. */
        void fillBuffer_() {
            size_t start = 0;
            // compact unprocessed stream
            if (i_ != end_) {
                start = end_ - i_;
                memcpy(buf_, buf_ + i_, start);
            }
            // read more contents
            end_ = start + fread(buf_+start, sizeof(char), BUFSIZE - start, file_);
            i_ = start;
        }

        /** Skips spaces.  Note that this may need to fill the buffer if the
            last character of the buffer is space itself.  */
        void skipWhitespace_() {
            while (true) {
                if (i_ == end_) {
                    if (feof(file_)) return;
                    fillBuffer_();
                }
                // if the current character is not whitespace, we are done
                if (!isspace(buf_[i_]))
                    return;
                // otherwise skip it
                ++i_;
            }
        }

        char * buf_;
        size_t end_ = 0;
        size_t i_ = 0;
        FILE * file_;
};

class Num {
    public:
        int val;

        Num() {
            val = 0;
        }
};


/****************************************************************************/
class Adder : public Reader {
    public:

        std::map<std::string, int> map_;  // String to Num map;  Num holds an int

        Adder() {}

        bool visit(Row& r) override {
            String* word = r.get_string(0);
            assert(word != nullptr);
            int num = (map_.find(word->c_str()) != map_.end()) ? map_.at(word->c_str()) : -1;
            assert(num >= 0);
            num++;
            //unsure
            map_.at(word->c_str()) = num;
            return false;
        }
};

/***************************************************************************/
class Summer : public Writer {
    public:
        SIMap& map_;
        size_t i = 0;
        size_t j = 0;
        size_t seen = 0;

        Summer(SIMap& map) : map_(map) {
            if (!k()) {
                next();
            }
        }

        void next() {
            assert(!done());
            if (i == map_.capacity_ ) return;
            j++;
            ++seen;
            if ( j >= map_.items_[i].keys_.length() ) {
                ++i;
                j = 0;
                while( i < map_.capacity_ && map_.items_[i].keys_.length() == 0 ) {
                    i++;
                }
            }
        }

        String* k() {
            if (i==map_.capacity_ || j == map_.items_[i].keys_.length()) {
                return nullptr;
            }
            return (String*) (map_.items_[i].keys_.get(j));
        }

        size_t v() {
            if (i == map_.capacity_ || j == map_.items_[i].keys_.length()) {
                assert(false); return 0;
            }
            return ((Num*)(map_.items_[i].vals_.get(j)))->v;
        }

        void visit(Row& r) {
            String & key = *k();
            size_t value = v();
            r.set(0, &key);
            r.set(1, (int) value);
            next();
        }

        bool done() {
            return seen == map_.size();
        }
    };

};

/****************************************************************************
 * Calculate a word count for given file:
 *   1) read the data (single node)
 *   2) produce word counts per homed chunks, in parallel
 *   3) combine the results
 **********************************************************author: pmaj ****/
class WordCount: public Application {
public:
    static const size_t BUFSIZE = 1024;
    Key in;
    KeyBuff kbuf;
    SIMap all;

    WordCount(size_t idx, NetworkIfc & net):
            Application(idx, net), in("data"), kbuf(new Key("wc-map-",0)) { }

    /** The master nodes reads the input, then all of the nodes count. */
    void run_() override {
        if (index == 0) {
            FileReader fr;
            delete DataFrame::fromVisitor(&in, &kv, "S", fr);
        }
        local_count();
        reduce();
    }

    /** Returns a key for given node.  These keys are homed on master node
     *  which then joins them one by one. */
    Key* mk_key(size_t idx) {
        Key * k = kbuf.c(idx).get();
        LOG("Created key " << k->c_str());
        return k;
    }

    /** Compute word counts on the local node and build a data frame. */
    void local_count() {
        DataFrame* words = (kv.waitAndGet(in));
        p("Node ").p(index).pln(": starting local count...");
        SIMap map;
        Adder add(map);
        words->local_map(add);
        delete words;
        Summer cnt(map);
        delete DataFrame::fromVisitor(mk_key(index), &kv, "SI", cnt);
    }

    /** Merge the data frames of all nodes */
    void reduce() {
        if (index != 0) return;
        pln("Node 0: reducing counts...");
        SIMap map;
        Key* own = mk_key(0);
        merge(kv.get(*own), map);
        for (size_t i = 1; i < arg.num_nodes; ++i) { // merge other nodes
            Key* ok = mk_key(i);
            merge(kv.waitAndGet(*ok), map);
            delete ok;
        }
        p("Different words: ").pln(map.size());
        delete own;
    }

    void merge(DataFrame* df, SIMap& m) {
        Adder add(m);
        df->map(add);
        delete df;
    }
}; // WordcountDemo
