#include "../network/serial.h"
#include "../dataframe/schema.h"
#include "../dataframe/dataframe.h"
#include <stdio.h>
#include <stdlib.h>

#include "../dataframe/parser.h"

/**
 * Enum representing different states of parsing command line arguments.
 */
enum class ParseState {
    DEFAULT,
    FLAG_F,
    FLAG_FROM,
    FLAG_LEN,
};

/**
 * Helper function to terminate with an error print if the given bool is not true.
 * @param test The bool
 */
void cli_assert(bool test) {
    if (!test) {
        printf("Unexpected command line arguments provided\n");
        exit(-1);
    }
}

/**
 * Asserts that the given ssize_t has not already been changed from -1, and then
 * parses the given c-style string as long to set it.
 * @param arg_loc The location of the ssize_t to work with
 * @param arg A string containing a long to parse
 */
void parse_size_t_arg(ssize_t* arg_loc, char* arg) {
    cli_assert(*arg_loc == -1);
    *arg_loc = atol(arg);
}

/**
 * Parses command line args given by argc and argv. Updates the given ssize_t pointers to
 * -1 for each arg that is not present on the command line (nullptr for file), or the value of that
 * command line argument.
 * @param argc, argv The command line arguments
 * @param file Pointer to result of parsing -f
 * @param start Pointer to result of parsing -from
 * @param len Pointer to result of parsing -len
 * @param col_type Pointer to result of parsing -print_col_type
 * @param col_idx_col, col_idx_off Pointer to result of parsing -print_col_idx
 * @param missing_idx_col, missing_idx_off Pointer to result of parsing -is_missing_idx
 */
void parse_args(int argc, char* argv[], char** file, ssize_t* start, ssize_t* len) {
    *file = nullptr;
    // -1 represents argument not provided
    *start = -1;
    *len = -1;

    ParseState state = ParseState::DEFAULT;

    for (int i = 1; i < argc; i++) {
        char* arg = argv[i];
        switch (state) {
            case ParseState::DEFAULT:
                if (strcmp(arg, "-f") == 0) {
                    state = ParseState::FLAG_F;
                } else if (strcmp(arg, "-from") == 0) {
                    state = ParseState::FLAG_FROM;
                } else if (strcmp(arg, "-len") == 0) {
                    state = ParseState::FLAG_LEN;
                } else {
                    cli_assert(false);
                }
                break;
            case ParseState::FLAG_F:
                cli_assert(*file == nullptr);
                *file = arg;
                state = ParseState::DEFAULT;
                break;
            case ParseState::FLAG_FROM:
                parse_size_t_arg(start, arg);
                state = ParseState::DEFAULT;
                break;
            case ParseState::FLAG_LEN:
                parse_size_t_arg(len, arg);
                state = ParseState::DEFAULT;
                break;
            default:
                cli_assert(false);
        }
    }
    cli_assert(state == ParseState::DEFAULT);
}


/**
 * function to build a dataframe from a sorer file
 */
DataFrame* buildFromFile(int argc, char* argv[]) {
    // Parse arguments
    char* filename = nullptr;
    // -1 represents argument not provided
    ssize_t start = -1;
    ssize_t len = -1;

    parse_args(argc, argv, &filename, &start, &len);

    // Check arguments
    if (filename == nullptr) {
        printf("No file provided\n");
        return nullptr;
    }

    // Open requested file
    FILE* file = fopen(filename, "r");
    if (file == NULL) {
        printf("Failed to open file\n");
        return nullptr;
    }
    fseek(file, 0, SEEK_END);
    size_t file_size = ftell(file);
    fseek(file, 0, SEEK_SET);

    // Set argument defaults
    if (start == -1) {
        start = 0;
    }

    if (len == -1) {
        len = file_size - start;
    }

    {
        // Run parsing
        SorParser parser{file, (size_t)start, (size_t)start + len, file_size};
        parser.guessSchema();
        parser.parseFile();
        ColumnSet* set = parser.getColumnSet();

        Schema s{};
        DataFrame df(s);

        for(size_t i = 0; i < set->getLength(); i++) {
            BaseColumn* column = set->getColumn(i);
            Column* newCol;
            ColumnType type = column->getType();
            if(type == ColumnType::STRING) {
                StringCol* to_add = dynamic_cast<StringCol*>(column);
                newCol = new StringColumn();
                for(size_t colIndex = 0; colIndex < column->getLength(); colIndex++) {
                    newCol->push_back(new String(to_add->getEntry(colIndex)));
                }
                df.add_column(newCol, nullptr);
            } else if(type == ColumnType::INTEGER) {
                IntegerCol* to_add = dynamic_cast<IntegerCol*>(column);
                newCol = new IntColumn();
                for(size_t colIndex = 0; colIndex < column->getLength(); colIndex++) {
                    newCol->push_back(to_add->getEntry(colIndex));
                }
                df.add_column(newCol, nullptr);
            } else if(type == ColumnType::FLOAT) {
                FloatCol* to_add = dynamic_cast<FloatCol*>(column);
                newCol = new FloatColumn();
                for(size_t colIndex = 0; colIndex < column->getLength(); colIndex++) {
                    newCol->push_back(to_add->getEntry(colIndex));
                }
                df.add_column(newCol, nullptr);
            } else {
                BoolCol* to_add = dynamic_cast<BoolCol*>(column);
                newCol = new BoolColumn();
                for(size_t colIndex = 0; colIndex < column->getLength(); colIndex++) {
                    newCol->push_back(to_add->getEntry(colIndex));
                }
                df.add_column(newCol, nullptr);
            }
        }
        delete set;

        df.print();
        fclose(file);
        return &df;
    }
}

/**
 * Main entry point to the program
 * @param argc
 * @param argv
 * @return
 */
int main(int argc, char* argv[]) {
    DataFrame* df = buildFromFile(argc, argv);
    return 0;
}
