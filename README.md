# MapReduce

In this assignment, for each type of thread (mapper/reducer), I defined a specific data structure:

- **'struct MapperUtils'** for the mapper threads.
- **'struct ReducerUtils'** for the reducer threads.

These structures are used as arguments in the `pthread_create()` function calls and contain both useful information for processing and synchronization variables.

## `Map()` function

The mapper threads extract tasks from a queue that contains the names of all files to be processed. Since multiple mappers access the queue simultaneously, a mutex is used to avoid race conditions.

Each mapper thread is associated with a variable of type:

`unordered_map<char, unordered_map<string, set<unsigned int>>>`

This should have represented a partial list according to the requirement, but for simplicity, it is composed of 26 partial lists organized based on the first letter of the words:

- The key of type 'char' represents the character with which the words begin.
- The value corresponding to the key represents a mapping between words and a set of file IDs where those words were found.

A set is used to store file IDs to ensure automatic sorting and eliminate duplicates.

At the end of the function, it is ensured that all mapper threads have completed processing by using a barrier. One mapper thread then triggers the execution of the reducers via a semaphore, using a mutex to prevent concurrent access to both the semaphore and the `start_reducers` variable.

## `Reduce()` function

The reducer threads extract tasks from a queue of characters, each character representing the first letter of the words that need to be processed by the respective thread.

Similar to the mappers, access to the queue is protected by a mutex.

Each reducer thread processes words starting with the character extracted from the queue, merging the partial lists into a final list. The final list, like the partial one, represents a mapping between the alphabet letters and the words that begin with a specific letter.

After processing all words starting with a particular character, they will be sorted according to the requirements and displayed in the output file.
