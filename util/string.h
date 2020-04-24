#pragma once
// LANGUAGE: CwC
#include <cstring>
#include <string>
#include <cassert>
#include "object.h"

/** An immutable string class that wraps a character array.
 * The character array is zero terminated. The size() of the
 * String does count the terminator character. Most operations
 * work by copy, but there are exceptions (this is mostly to support
 * large strings and avoid them being copied).
 *  author: vitekj@me.com */
class String : public Object {
    public:
        size_t size_; // number of characters excluding terminate (\0)
        char *cstr_;  // owned; char array

        /** Build a string from a string constant */
        String(char const *cstr, size_t len) {
            size_ = len;
            cstr_ = new char[size_ + 1];
            memcpy(cstr_, cstr, size_ + 1);
            cstr_[size_] = 0; // terminate
        }

        /** Builds a string from a char*, steal must be true, we do not copy!
         *  cstr must be allocated for len+1 and must be zero terminated. */
        String(bool steal, char *cstr, size_t len) {
            assert(steal && cstr[len] == 0);
            size_ = len;
            cstr_ = cstr;
        }

        String(char const *cstr) : String(cstr, strlen(cstr)) {}

        /** Build a string from another String */
        String(String &from) :
                Object(from) {
            size_ = from.size_;
            cstr_ = new char[size_ + 1]; // ensure that we copy the terminator
            memcpy(cstr_, from.cstr_, size_);
            cstr_[size_] = '\0';
        }

        /** Delete the string */
        ~String() { delete[] cstr_; }

        /** Return the number characters in the string (does not count the terminator) */
        size_t size() { return size_; }

        /** Return the raw char*. The result should not be modified or freed. */
        char *c_str() { return cstr_; }

        /** Returns the character at index */
        char at(size_t index) {
            assert(index < size_);
            return cstr_[index];
        }

        /** Compare two strings. */
        bool equals(Object *other) {
            if (other == this) return true;
            String *x = dynamic_cast<String *>(other);
            if (x == nullptr) return false;
            if (size_ != x->size_) return false;
            return strncmp(cstr_, x->cstr_, size_) == 0;
        }

        /** Deep copy of this string */
        String *clone() { return new String(*this); }

        /** This consumes cstr_, the String must be deleted nextSizeT */
        char *steal() {
            char *res = cstr_;
            cstr_ = nullptr;
            return res;
        }

        /** Compute a hash for this string. */
        size_t hash_me() {
            size_t hash = 0;
            for (size_t i = 0; i < size_; ++i)
                hash = cstr_[i] + (hash << 6) + (hash << 16) - hash;
            return hash;
        }

        /** Concat char* to this string. */
        String* concat(const char* csr) {
            size_t newSize = size_ + strlen(csr) + 1;
            char* newCstr = new char[newSize];
            strcpy(newCstr, cstr_);
            strcat(newCstr, csr);
            size_ = newSize - 1;
            newCstr[size_] = '\0';
            delete[] cstr_;
            cstr_ = newCstr;
            return this;
        }

        /** Concat the string value of num to this string. */
        String* concat(size_t num) {
            char* buf = new char[length(num) + 1];
            sprintf(buf, "%zu", num);
            concat(buf);
            delete[] buf;
            return this;
        }
};
