#pragma once

#include "../dataframe/dataframe.h"
#include "../network/network.h"
#include "../util/helper.h"

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
            if (index == 0) {
                size_t SZ = 1000 * 1000;
                auto** vals = new String*[SZ];
                double sum = 0;
                for (size_t i = 0; i < SZ; ++i) {
                    auto* str = new String("hello");
                    vals[i] = str;
                    sum += i;
                }
                Key key("triv", 0);
                DistDataFrame *df = DistDataFrame::fromArray(&key, kd, SZ, vals);
                auto* str = new String("hello");
                assert(df->get_string(0, 1)->equals(str));
                DistDataFrame *df2 = kd->get(key);
                for (size_t i = 0; i < SZ; ++i) {
                    assert(df->get_string(0, i)->equals(str));
                    sum -= i;
                }
                assert(sum == 0);
                delete str;
                delete df;
                delete df2;
                for (size_t i = 0; i < SZ; ++i) {
                    delete vals[i];
                }
                delete[] vals;
            }
        }
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
            auto* word = new String(buf_ + wStart, i_ - wStart);
            //change
            r.set(0, word);
            ++i_;
            skipWhitespace_();
        }

        /** Returns true when there are no more words to read.  There is nothing
           more to read if we are at the end of the buffer and the file has
           all been read.     */
        bool done() override { return (i_ >= end_) && feof(file_);  }

        /** Creates the reader and opens the file for reading.  */
        explicit FileReader( const char* filename) {
            file_ = fopen(filename, "r");
            if (file_ == nullptr) assert(false);
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

/****************************************************************************/
class Adder : public Reader {
    public:

        std::map<std::string, size_t> map_;  // String to Num map;  Num holds an int

        explicit Adder(std::map<std::string, size_t>* map) {
            map_ = *map;
        }

        bool visit(Row& r) {
            String* word = r.get_string(0);
            assert(word != nullptr);
            size_t num = (map_.find(std::string(word->c_str())) != map_.end()) ?
                    map_[std::string(word->c_str())] : 0;
            map_[std::string(word->c_str())] = num + 1;
            //changed this to true
            return true;
        }
};

/***************************************************************************/
class Summer : public Writer {
    public:
        std::map<std::string, size_t>::iterator itr;
        std::map<std::string, size_t>::iterator end;

        explicit Summer(std::map<std::string, size_t>* map) {
            itr = map->begin();
            end = map->end();
        }

        void next() {
            assert(!done());
            itr++;
        }

        const char* k() const {
            return itr->first.c_str();
        }

        int v() const {
            return itr->second;
        }

        void visit(Row& r) override {
            const char* key = k();
            auto* str = new String(key);
            size_t value = v();
            r.set(0, str);
            r.set(1, (int) value);
            next();
        }

        bool done() override {
            return itr == end;
        }
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
    Key* in;

    WordCount(size_t idx, KDStore* net):
            Application(idx, net) {
        in = new Key("data", 0);
    }

    /** The master nodes reads the input, then all of the nodes count. */
    void run_() override {
        if (index == 0) {
            FileReader fr{"../filename.txt"};
            delete DistDataFrame::fromVisitor(in, kd, "S", &fr);
        }
        local_count();
        reduce();
    }

    /** Compute word counts on the local node and build a data frame. */
    void local_count() {
        DistDataFrame* words = kd->waitAndGet(*in);
        std::map<std::string, size_t> map;
        Adder add(&map);
        words->local_map(&add);
        delete words;
        Summer cnt(&map);
        delete DistDataFrame::fromVisitor(in, kd, "SI", &cnt);
    }

    /** Merge the data frames of all nodes */
    void reduce() {
        if (index != 0) return;
        std::map<std::string, size_t> map;
        auto* key = new String("wc-map-");
        String* ownStr = key->clone()->concat((size_t)0);
        Key* own = new Key(ownStr->c_str(), 0);
        delete ownStr;
        merge(kd->get(*own), &map);
        for (size_t i = 1; i < 5; ++i) { // merge other nodes
            String* okStr = key->clone()->concat(i);
            Key* ok = new Key(okStr->c_str(), 0);
            delete okStr;
            merge(kd->waitAndGet(*ok), &map);
            delete ok;
        }
        delete key;
        delete own;
        std::cout << &map;
    }

    static void merge(DistDataFrame* df, std::map<std::string, size_t>* m) {
        Adder add(m);
        df->map(&add);
        delete df;
    }
}; // WordcountDemo
