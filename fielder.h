#include "object.h"

/*****************************************************************************
 * Fielder::
 * A field vistor invoked by Row.
 */
class Fielder : public Object {
    public:

        /** Called before visiting a row, the argument is the row offset in the
          dataframe. */
        virtual void start(size_t r) {
            std::cout << "Visiting row #" << r << "\n";
        }

        /** Called for fields of the argument's type with the value of the field. */
        virtual void accept(bool b) {

        }

        virtual void accept(float f) {

        }

        virtual void accept(int i) {

        }

        virtual void accept(String *s) {

        }

        /** Called when all fields have been seen. */
        virtual void done() {
            std::cout << "Finished visiting row" << "\n";
        }
};

class PrintFielder : public Fielder {
    public:
        void start(size_t r) {
            std::cout << "Row #" << r << " -----";
        }

        /** Called for fields of the argument's type with the value of the field. */
        void accept(bool b) {
            std::cout << " " << b;
        }

        void accept(float f) {
            std::cout << " " << f;
        }

        void accept(int i) {
            std::cout << " " << i;
        }

        void accept(String *s) {
            std::cout << " \"" << s->c_str() << "\"";
        }

        /** Called when all fields have been seen. */
        void done() {
            std::cout << "\n";
        }
};