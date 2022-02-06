#include <Python.h>
#include <boost/python.hpp>
#define Py_PRINT_STR 0
//STD
#include <cstring>
#include <string>
#include <iostream>
#include <cctype>

namespace bp = boost::python;

bool isStringConvertible(const bp::object &obj);
void countObjectWords(bp::object obj, bp::object &dst, std::string keyName = "");
void countDictWords(bp::dict src, bp::object &dst, std::string keyName = "");
void countListWords(bp::list src, bp::object &dst, std::string keyName = "");
void countStringWords(std::string src, bp::object &dst);
bp::list countWords(const bp::list json);