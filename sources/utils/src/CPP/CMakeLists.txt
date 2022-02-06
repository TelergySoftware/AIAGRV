message("Creating build files...")

# Set the output library directory
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/shared)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

# Set all source files to be compiled to the SRC variable
set(SRC 
${CMAKE_CURRENT_LIST_DIR}/ICdataUtils.cc
${CMAKE_CURRENT_LIST_DIR}/ICdataUtils.hh)

add_library(ICdataUtils SHARED ${SRC})
set_target_properties(${CMAKE_PROJECT_NAME} PROPERTIES PREFIX "")


find_package(Python3 COMPONENTS Development REQUIRED)
find_package(Boost COMPONENTS python REQUIRED)

add_dependencies(${CMAKE_PROJECT_NAME} Python3::Python)

if(NOT Python3_FOUND)
    message(FATAL_ERROR "Python3 is required to build this project.")
else()
    message(STATUS "Including and linking required files...")
    include_directories(${Python3_INCLUDE_DIRS} ${Boost_INCLUDE_DIRS})
    link_directories(${Python3_LIBRARY_DIRS} ${Boost_LIBRARY_DIRS})
    target_link_directories(${CMAKE_PROJECT_NAME} PRIVATE ${Boost_LIBRARY_DIRS} ${Python3_LIBRARY_DIRS})
    target_link_libraries(${CMAKE_PROJECT_NAME} ${Boost_LIBRARIES} ${Python3_LIBRARIES})
endif()