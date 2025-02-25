#include <stdlib.h>
#include <iostream>
#include <pthread.h>
#include <vector>
#include <string>
#include <unordered_map>
#include <fstream>
#include <queue>
#include <set>
#include <algorithm>
#include <cctype>
#include <semaphore.h>

using namespace std;

// Structure to manage mapper resources
struct MapperUtils {
  // Partial lists
  vector<unordered_map<char, unordered_map<string, set<unsigned int>>>> partial_lists;
  unordered_map<string, unsigned int> file_ids; // Map to associate file names with unique IDs
  queue<string> queue_tasks; // Queue of file names to process
  pthread_mutex_t mutex_queue_tasks; // Mutex for concurrent access to the queue
  pthread_mutex_t mutex_start_reducers; // Mutex for synchronizing reducer threads
  pthread_barrier_t barrier_start; // Barrier to synchronize mapper threads at the start
  pthread_barrier_t barrier_end; // Barrier to synchronize mapper threads at the end
  sem_t *semaphore; // Semaphore to signal reducer threads
  unsigned int num_reducer_threads; // Number of reducer threads
  unsigned int sem_activated; // Flag to ensure the semaphore is activated only once

  // Constructor to initialize mapper utilities
  MapperUtils(unsigned int num_mapper_threads, unsigned int reducer_threads, 
              sem_t *sem_ptr) {
    pthread_mutex_init(&mutex_queue_tasks, NULL);
    pthread_mutex_init(&mutex_start_reducers, NULL);
    pthread_barrier_init(&barrier_start, NULL, num_mapper_threads);
    pthread_barrier_init(&barrier_end, NULL, num_mapper_threads);
    semaphore = sem_ptr;
    partial_lists.resize(num_mapper_threads);
    num_reducer_threads = reducer_threads;
    sem_activated = 0;
  }
    
  // Destructor to clean up resources
  ~MapperUtils() {
    pthread_mutex_destroy(&mutex_queue_tasks);
    pthread_mutex_destroy(&mutex_start_reducers);
    pthread_barrier_destroy(&barrier_start);
    pthread_barrier_destroy(&barrier_end);
  }
};

// Structure to manage reducer resources
struct ReducerUtils {
  // Reference to partial mapper lists
  vector<unordered_map<char, unordered_map<string, set<unsigned int>>>>& partial_lists;
  // Final lists of words and their occurrences
  unordered_map<char, unordered_map<string, set<unsigned int>>> final_lists;
  pthread_barrier_t barrier_start; // Barrier to synchronize reducer threads
  pthread_mutex_t mutex_queue_tasks; // Mutex for concurrent access to the task queue
  queue<char> queue_tasks; // Queue of characters to process
  sem_t *semaphore; // Semaphore to synchronize reducers with mappers

  // Constructor to initialize reducer utilities
  ReducerUtils(vector<unordered_map<char, unordered_map<string, set<unsigned int>>>>& partial_lists,
               unsigned int num_reducer_threads, sem_t *sem_ptr)
               : partial_lists(partial_lists) {
    pthread_mutex_init(&mutex_queue_tasks, NULL);
    pthread_barrier_init(&barrier_start, NULL, num_reducer_threads);
    semaphore = sem_ptr;

    // Initialize tasks with letters from 'a' to 'z'
    for (char c = 'a'; c <= 'z'; c++) {
      queue_tasks.push(c);
      final_lists[c] = unordered_map<string, set<unsigned int>>();
    }
  }

  // Destructor to clean up resources
  ~ReducerUtils() {
    pthread_mutex_destroy(&mutex_queue_tasks);
    pthread_barrier_destroy(&barrier_start);
  }
};

// Struct to pass thread arguments
struct ThreadArgs {
  MapperUtils* utils; // Pointer to mapper utilities
  unsigned int thread_id; // ID of the thread
};

// Converts a string to lowercase
void ToLowerCase(string &str) {
  for (unsigned int i = 0; i < str.size(); i++) {
    str[i] = tolower(static_cast<unsigned char>(str[i]));
  }
}

// Removes unwanted non-alphabetic characters from a string
void RemoveUnwantedCharacters(string &str) {
  string res;
  for (unsigned int i = 0; i < str.size(); i++) {
    char c = str[i];
    if (isalpha(static_cast<unsigned char>(c))) {
      res += c;
    }
  }
  str = res;
}

// Mapper thread function
void *Map(void *arg) {
  ThreadArgs *thread_args = reinterpret_cast<ThreadArgs *>(arg);
  MapperUtils *mapper_utils = thread_args->utils;
  unsigned int t_id = thread_args->thread_id;

  // Wait for all mappers to start
  pthread_barrier_wait(&(mapper_utils->barrier_start));

  while (true) {  
    pthread_mutex_lock(&(mapper_utils->mutex_queue_tasks));

    if (mapper_utils->queue_tasks.empty()) {
      pthread_mutex_unlock(&(mapper_utils->mutex_queue_tasks));
      break;
    }

    string curr_filename = mapper_utils->queue_tasks.front();
    mapper_utils->queue_tasks.pop();

    pthread_mutex_unlock(&(mapper_utils->mutex_queue_tasks));

    // Process the file
    unsigned int file_id = mapper_utils->file_ids[curr_filename];
    ifstream file_in(curr_filename);
    string word;   

    while (file_in >> word) {
      ToLowerCase(word);
      RemoveUnwantedCharacters(word);
      if (!word.empty()) {
        char first_char = word[0];
        auto& ids = mapper_utils->partial_lists[t_id][first_char][word];
        if (ids.find(file_id) == ids.end()) {
          ids.insert(file_id); // Add the file ID to the set of occurrences
        }
      }
    }
  }

  // Wait for all mappers to finish
  pthread_barrier_wait(&(mapper_utils->barrier_end));

  // Ensure reducers start only once
  pthread_mutex_lock(&(mapper_utils->mutex_start_reducers));
  if (mapper_utils->sem_activated == 0) {
    for (unsigned int i = 0; i < mapper_utils->num_reducer_threads; i++)
      sem_post(mapper_utils->semaphore);

    mapper_utils->sem_activated = 1;
  }
  pthread_mutex_unlock(&(mapper_utils->mutex_start_reducers));

  return NULL;
}

// Comparator function to sort words by occurrences and alphabetically
bool compare(pair<string, set<unsigned int>>& a,
             pair<string, set<unsigned int>>& b) {
  if (a.second.size() != b.second.size())
    return a.second.size() > b.second.size();
  return a.first < b.first;
}

// Reducer thread function
void *Reduce(void *arg) {
  ReducerUtils *reducer_utils = reinterpret_cast<ReducerUtils *>(arg);

  sem_wait(reducer_utils->semaphore); // Wait for signal to start

  while (true) {  
    pthread_mutex_lock(&(reducer_utils->mutex_queue_tasks));

    if (reducer_utils->queue_tasks.empty()) {
      pthread_mutex_unlock(&(reducer_utils->mutex_queue_tasks));
      break;
    }

    char curr_char = reducer_utils->queue_tasks.front();
    reducer_utils->queue_tasks.pop();

    pthread_mutex_unlock(&(reducer_utils->mutex_queue_tasks));

    // Merge partial lists for the current character
    unsigned int partial_lists_size = reducer_utils->partial_lists.size();
    unordered_map<string, set<unsigned int>>& final_list = 
      reducer_utils->final_lists[curr_char];

    for (unsigned int i = 0; i < partial_lists_size; i++) {
      unordered_map<string, set<unsigned int>>& partial_list = 
        reducer_utils->partial_lists[i][curr_char];

      for (auto& [key, partial_set] : partial_list) {
        if (final_list.find(key) != final_list.end()) {
          final_list[key].insert(partial_set.begin(), partial_set.end());
        } else {
          final_list[key] = partial_set;
        }
      }
    }

    // Write the results to a file
    string filename(1, curr_char);
    filename += ".txt";
    ofstream file(filename);

    if (!file.is_open()) {
      cout << "Failed to create file: " << filename << endl;
      exit(1);
    }

    if (!final_list.empty()) {
      vector<pair<string, set<unsigned int>>> vec(final_list.begin(), 
                                                  final_list.end());
      sort(vec.begin(), vec.end(), compare);

      for (auto& [word, occurrences] : vec) {
        file << word << ":[";
        for (auto it = occurrences.begin(); it != occurrences.end(); ++it) {
          file << *it;
          if (next(it) != occurrences.end()) {
            file << " ";
          }
        }
        file << "]\n";
      }
    }

    file.close();
  }

  return NULL;
}

// Reads filenames and initializes the task queue
void ReadFilenames(queue<string> *queue_filenames, string input_filename, 
                   unordered_map<string, unsigned int> *file_id) {
  ifstream file_in(input_filename);

  if (!file_in.is_open()) {
    cout << "Could not open file: " << input_filename << endl;
    exit(1);
  }

  unsigned int count_files;
  string line;

  getline(file_in, line);
  count_files = stoi(line); // Read the number of files

  string curr_filename;
  for (unsigned int i = 0; i < count_files; i++) {
    getline(file_in, curr_filename);
    queue_filenames->push(curr_filename); // Add filenames to the queue
    (*file_id)[curr_filename] = i + 1; // Assign a unique ID to each file
  }

  file_in.close();
}

int main(int argc, char **argv)
{
  unsigned int mapper_threads = atoi(argv[1]); // Number of mapper threads
  unsigned int reduce_threads = atoi(argv[2]); // Number of reducer threads
  unsigned int num_threads = mapper_threads + reduce_threads;

  pthread_t threads[num_threads]; // Thread array
  sem_t semaphore; // Semaphore for synchronization
  sem_init(&semaphore, 0, 0);

  // Initialize mapper and reducer utilities
  MapperUtils *mapper_utils = new MapperUtils(mapper_threads, reduce_threads, 
                                              &semaphore);
  ReducerUtils *reducer_utils = new ReducerUtils(mapper_utils->partial_lists, 
                                                reduce_threads, &semaphore);
    
  // Read input file and populate task queue
  ReadFilenames(&mapper_utils->queue_tasks, string(argv[3]), 
                &mapper_utils->file_ids);

  int r;

  ThreadArgs *thread_args[mapper_threads];
  for (unsigned int i = 0; i < mapper_threads; i++) {
    thread_args[i] = new ThreadArgs;
    thread_args[i]->thread_id = i;
    thread_args[i]->utils = mapper_utils;
  } 
  
  // Create mapper and reducer threads
  for (unsigned int id = 0; id < num_threads; id++) {
    if (id < mapper_threads) {
      r = pthread_create(&threads[id], NULL, Map, thread_args[id]);
      if (r) {
        cout << "Error creating the thread: " << id << endl;
        exit(1);
      }
    } else {
      r = pthread_create(&threads[id], NULL, Reduce, reducer_utils);
      if (r) {
        cout << "Error creating the thread: " << id << endl;
        exit(1);
      }
    }
  }

  // Join all threads
  for (unsigned int id = 0; id < num_threads; id++) {
    r = pthread_join(threads[id], NULL);
  }

  // Clean up allocated resources
  for (unsigned int i = 0; i < mapper_threads; i++) {
    delete thread_args[i];
  }

  sem_destroy(&semaphore);
  delete mapper_utils;
  delete reducer_utils;

  return 0;
}
