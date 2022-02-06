/** 
 * This file is part of the ICdataUtils module for python.
 * It provides functions to handle big chunks of data more efficiently.
 */
#include "ICdataUtils.hh"

namespace bp = boost::python;


/**
 * @brief Check if the object is a string convertible object.
 * @param obj The object to check.
 * @return True if the object is a string convertible object.
 */
bool isStringConvertible(const bp::object &obj)
{
    // Check if the object is a string.
    if (std::strcmp(obj.ptr()->ob_type->tp_name, "str") == 0) {
        return true;
    }
    // Check if the object is an integer.
    if (std::strcmp(obj.ptr()->ob_type->tp_name, "int") == 0) {
        return true;
    }
    // Check if the object is a float.
    if (std::strcmp(obj.ptr()->ob_type->tp_name, "float") == 0) {
        return true;
    }
    // Check if the object is a bool.
    if (std::strcmp(obj.ptr()->ob_type->tp_name, "bool") == 0) {
        return true;
    }
    
    return false;
}

/**
 * @brief Counts the words in a python object.
 * @param obj The python object to count the words in.
 * @param dict The dictionary to store the words in.
 */
void countObjectWords(bp::object obj, bp::object &dst, std::string keyName)
{
    if (obj.is_none())
    {
        return;
    }
    if (std::strcmp(obj.ptr()->ob_type->tp_name, "dict") == 0)
    {
        // Extract the dictionary from the object.
        bp::dict dictObj = bp::extract<bp::dict>(obj);
        countDictWords(dictObj, dst, keyName);
    }
    else if (std::strcmp(obj.ptr()->ob_type->tp_name, "list") == 0)
    {
        // Extract the list from the object.
        bp::list listObj = bp::extract<bp::list>(obj);
        countListWords(listObj, dst, keyName);
    }
    else if (std::strcmp(obj.ptr()->ob_type->tp_name, "str") == 0)
    {
        // // Extract the string from the object.
        std::string strObj = bp::extract<std::string>(obj);
        countStringWords(strObj, dst);
    }
    else if (std::strcmp(obj.ptr()->ob_type->tp_name, "int") == 0)
    {
        // Extract the int from the object.
        int intObj = bp::extract<int>(obj);
        std::string strObj = std::to_string(intObj);
        countStringWords(strObj, dst);
    }
    else if (std::strcmp(obj.ptr()->ob_type->tp_name, "float") == 0)
    {
        // Extract the float from the object.
        float floatObj = bp::extract<float>(obj);
        std::string strObj = std::to_string(floatObj);
        countStringWords(strObj, dst);
    }
    else if (std::strcmp(obj.ptr()->ob_type->tp_name, "bool") == 0)
    {
        // Extract the bool from the object.
        bool boolObj = bp::extract<bool>(obj);
        std::string strObj = std::to_string(boolObj);
        countStringWords(strObj, dst);
    }
    else
    {
        std::cout << "Object type not supported." << std::endl;
    }
}

/**
 * @brief Counts the words in a python dictionary.
 * @param src The python dictionary to count the words in.
 * @param dict The dictionary to store the words in.
 */
void countDictWords(bp::dict src, bp::object &dst, std::string keyName)
{
    // If the dst object is a list, count the words on the src and add the counter to the list.
    if (std::strcmp(dst.ptr()->ob_type->tp_name, "list") == 0)
    {
        bp::list dstList = bp::extract<bp::list>(dst);

        // Create a counter dictionary.
        bp::dict counterDict;
        // Send the dictionary to the counter function.
        countObjectWords(src, counterDict, keyName);
        // Add the counter dictionary to the list.
        dstList.append(counterDict);
    }
    else
    {
        // If the dst object is a dictionary, loop over the src keys and send the values to the counter function.
        if (std::strcmp(dst.ptr()->ob_type->tp_name, "dict") == 0)
        {
            bp::dict dstDict = bp::extract<bp::dict>(dst);

            // Loop over the keys and send the values to the counter function.
            bp::list keys = src.keys();
            for (int i = 0; i < bp::len(keys); i++)
            {
                // Create a counter dictionary.
                bp::dict counterDict;
                bp::list counterList;

                std::string key = bp::extract<std::string>(keys[i]);
                bp::object value = src[key];
                // If the value is a list, send it to the counter function using the counter list.
                if (std::strcmp(value.ptr()->ob_type->tp_name, "list") == 0)
                {
                    countObjectWords(value, counterList, keyName);
                    dstDict[key] = counterList;
                }
                else
                {
                    // If the value is not a list, send it to the counter function using the counter dictionary.
                    countObjectWords(value, counterDict, keyName);
                    // Add the counter dictionary to the dst dictionary.
                    dstDict[key] = counterDict;
                }
            }
        }
    }
}

/**
 * @brief Count words from a python list.
 * 
 * @param list The python list.
 * @param dict Reference to the python dictionary to store the counted words.
 */
void countListWords(bp::list src, bp::object &dst, std::string keyName)
{
    for (int i = 0; i < bp::len(src); i++)
    {
        bp::object value = src[i];
        // If the value is a string convertible, send it to the counter function.
        if (isStringConvertible(value))
        {
            bp::dict counterDict;
            countObjectWords(value, counterDict);
            // Add the counter dictionary to the dst list.
            bp::list parentDST = bp::extract<bp::list>(dst);
            parentDST.append(counterDict);
        }
        else 
        {
            countObjectWords(value, dst);
        }

    }
}

/**
 * @brief Count words from a python string.
 * 
 * @param src The python string.
 * @param dict Reference to the python dictionary to store the counted words.
 */
void countStringWords(std::string src, bp::object &dst)
{
    // DST must be a dictionary.
    if (std::strcmp(dst.ptr()->ob_type->tp_name, "dict") != 0)
    {
        std::cout << "DST must be a dictionary." << std::endl;
        return;
    }
    bp::dict dstDict = bp::extract<bp::dict>(dst);

    std::string word;
    // Loop over the string and count the words.
    for (auto c : src)
    {
        // If the character is a letter or a number, add it to the word as a lowercase.
        if (std::isalpha(c) || std::isdigit(c))
        {
            word.append(1, std::tolower(c));
        }
        // Else,
        else
        {
            // check if the word count dictionay already has the word.
            // If it does, increment the count.
            if (dstDict.has_key(word))
            {
                dstDict[word] = bp::extract<int>(dstDict[word]) + 1;
            }
            // Else, add the word to the dictionary and set the count to 1.
            // Also, the word can't be empty.
            else
            {
                if (word.length() > 0)
                {
                    dstDict[word] = 1;
                }
            }
            // Clear the word.
            word = "";
        }
    }
    // If the word is not empty, add it to the dictionary.
    if (word.length() > 0)
    {
        // Check if the word count dictionay already has the word.
        // If it does, increment the count.
        if (dstDict.has_key(word))
        {
            dstDict[word] = bp::extract<int>(dstDict[word]) + 1;
        }
        // Else, add the word to the dictionary and set the count to 1.
        else
        {
            dstDict[word] = 1;
        }
    }
}

/**
 * @brief Receives a list with dictionary representation of a json, count the appearence of each word and store them in a list of dictionaries,
 * after that, return this newly created list.
 * @param json The dictionary representation of a json.
 * @return Return the list issues.
 */
bp::list countWords(const bp::list json)
{
    // List of dictionaries to be returned.
    bp::list issues;

    std::cout << "Counting words" << std::endl;

    // Iterate over the list of dictionaries.
    for (uint i = 0; i < bp::len(json); i++)
    {
        // Get the dictionary.
        bp::dict element = bp::extract<bp::dict>(json[i]);
        // Temp: Test the count object words.
        countObjectWords(element, issues);
    }

    // Return the list of dictionaries.
    return issues;
}

// Module initialization.
BOOST_PYTHON_MODULE(ICdataUtils)
{
    using namespace boost::python;
    Py_Initialize();
    def("countWords", countWords, "Count words in a json.");
}
