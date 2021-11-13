/** 
 * This file is part of the ICdataUtils module for python.
 * It provides functions to handle big chunks of data more efficiently.
 */

#include <boost/python.hpp>
#define Py_PRINT_STR 0
//STD
#include <string>
#include <iostream>
#include <cctype>


namespace bp = boost::python;

/**
 * @brief Receives a list with dictionary representation of a json, count the appearence of each word and store them in a list of dictionaries,
 * after that, return this newly created list.
 * @param json The dictionary representation of a json.
 * @return Return the list issues.
 */
bp::list countWords(const bp::list json) {
    bp::list issues;
    std::cout << "Counting words" << std::endl;
    for (uint i = 0; i < bp::len(json); i++) {
        bp::dict element = bp::extract<bp::dict>(json[i]);
        bp::dict counter;
        for (uint j = 0; j < bp::len(element.keys()); j++) {
            std::string key = bp::extract<std::string>(element.keys()[j]);
            bp::dict word_count;
            std::string value;
            try {
                value = bp::extract<std::string>(element[key]);
            } catch (bp::error_already_set) {
                PyErr_Print();
                continue;
            }
            std::string word;
            for (auto c : value) {
                if (std::isalpha(c)) {
                    word.append(1, std::tolower(c));
                }
                else {
                    if (word_count.has_key(word)) {
                        word_count[word] = bp::extract<int>(word_count[word]) + 1;
                    }
                    else {
                        if (word != " " && word != "") {
                            word_count[word] = 1;
                        }
                    }
                    word = "";
                }
            }
            counter[key] = word_count;
            word_count = bp::dict();
        }
        PyObject_Print(bp::object(element["title"]).ptr(), stdout, Py_PRINT_STR);
        std::cout << std::endl;
        issues.append(counter);
    }
    std::cout << "Finished counting words" << std::endl;

    return issues;
}

BOOST_PYTHON_MODULE(ICdataUtils) {
    using namespace boost::python;
    Py_Initialize();
    def("countWords", countWords);
}
