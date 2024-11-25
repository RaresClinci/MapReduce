#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <pthread.h>
#include <semaphore.h>
#include <queue>
#include <string>
#include <tuple>
#include <unordered_map>
#include <cctype>
#include <map>
#include <set>
#include <algorithm>

using namespace std;

struct mapper_args {
    int id;
    queue<tuple<int, string>> *mapper_tasks;
    pthread_mutex_t *mapper_mutex;
    queue<tuple<int, vector<string>>> *reducer_tasks;
    pthread_mutex_t *reducer_mutex;
    pthread_barrier_t *mappers_done;
    pthread_barrier_t *everyone_done;

};

struct reducer_args {
    queue<tuple<int, vector<string>>> *reducer_tasks;
    pthread_mutex_t *reducer_mutex;
    pthread_barrier_t *everyone_done;
    pthread_barrier_t *mappers_done;
    unordered_map<char, unordered_map<string, set<int>>> *agregated_list;
    pthread_mutex_t **agregated_mutex;
    queue<char> *sort_tasks;
    pthread_mutex_t *sort_mutex;
    queue<char> *write_tasks;
    pthread_mutex_t *write_mutex;
    unordered_map<char, vector<tuple<string, set<int>>>> *sorted_list;
    pthread_barrier_t *agg_done;
    pthread_barrier_t *sort_done;
};

string normalize_word(string word) {
    string new_word = "";

    for(auto letter : word) {
        if(isalpha(letter)) {
            new_word.push_back(tolower(letter));
        }
    }

    return new_word;
}

void *reducer(void *args) {
    reducer_args red_args = *(reducer_args*)args;

    // waiting for mappers to finish
    pthread_barrier_wait(red_args.mappers_done);
    
    cout << "here" << endl;

    // adding the words to the agregated list
    while (true) {
         // locking the queue
        pthread_mutex_lock(red_args.reducer_mutex);

        if (red_args.reducer_tasks->empty()) {
            // no more tasks
            pthread_mutex_unlock(red_args.reducer_mutex);
            break;
        }

        // picking up a task
        tuple<int, vector<string>> task = red_args.reducer_tasks->front();
        red_args.reducer_tasks->pop();

        // unlocking the queue
        pthread_mutex_unlock(red_args.reducer_mutex);

        // extracting the info
        int id = get<0>(task);
        vector<string> words = get<1>(task);

        // adding the words to the aggreagated list
        for (auto& w : words) {
            if (w.length() > 0) {
                char first_letter = w.at(0);

                // adding id to list
                pthread_mutex_lock(&(*red_args.agregated_mutex)[(int)first_letter]);

                red_args.agregated_list->at(first_letter)[w].insert(id);

                
                pthread_mutex_unlock(&(*red_args.agregated_mutex)[(int)first_letter]);
            }
        }
    }
    
    cout << "here1" << endl;

    // waiting for aggragting to stop
    pthread_barrier_wait(red_args.agg_done);

    // sorting the words
    while (true) {
        // locking the queue
        pthread_mutex_lock(red_args.sort_mutex);

        // searching for a letter with words to be sorted
        if (red_args.sort_tasks->empty()) {
            // no more tasks
            pthread_mutex_unlock(red_args.sort_mutex);
            break;
        }

        // picking up a task
        char letter = red_args.sort_tasks->front();
        red_args.sort_tasks->pop();

        // unlocking the queue
        pthread_mutex_unlock(red_args.sort_mutex);

        // turning the map into a vector
        for (auto it = red_args.agregated_list->at(letter).begin(); it != red_args.agregated_list->at(letter).end(); it++) {
            string word = it->first;
            set<int> ids = it->second;

            red_args.sorted_list->at(letter).push_back(make_tuple(word, ids));
        }
        // sorting the vector
        auto& vec = red_args.sorted_list->at(letter);

        sort(vec.begin(), vec.end(), [](const auto& a, const auto& b) {
            if(get<1>(a).size() != get<1>(b).size()) {
                return get<1>(a).size() > get<1>(b).size();
            } else {
                return get<0>(a) < get<0>(b);
            } 
        });

    }

    
    cout << "here2" << endl;
    // waiting for everyone to finish sorting
    pthread_barrier_wait(red_args.sort_done);

    // writing
    while(true) {
        // locking the queue
        pthread_mutex_lock(red_args.write_mutex);

        // searching for a letter with words to be sorted
        if (red_args.write_tasks->empty()) {
            // no more tasks
            pthread_mutex_unlock(red_args.write_mutex);
            break;
        }

        // picking up a task
        char letter = red_args.write_tasks->front();
        red_args.write_tasks->pop();

        // unlocking the queue
        pthread_mutex_unlock(red_args.write_mutex);

        string file_name = std::string(1, letter) + ".txt";

        ofstream fout(file_name);
        vector<tuple<string, set<int>>> letter_list = red_args.sorted_list->at(letter);

        for (auto& entry : letter_list) {
            string& word = get<0>(entry);
            set<int> files = get<1>(entry);

            fout << word << ":[";
            bool first = true;
            for (auto& file : files) {
                // placing spaces between words
                if (first) {
                    first = false;
                } else {
                    fout << " ";
                }

                // writing the file index
                fout << file;
            }
            fout << "]" << endl;
        }

        fout.close();
    }

    
    cout << "here3" << endl;

    // waiting for everyone to finish
    pthread_barrier_wait(red_args.everyone_done);

    pthread_exit(NULL);
}

void *mapper(void *args) 
{
    mapper_args map_args = *(mapper_args*)args;

    while (true) {
        // locking the queue
        pthread_mutex_lock(map_args.mapper_mutex);

        if (map_args.mapper_tasks->empty()) {
            // no more tasks
            pthread_mutex_unlock(map_args.mapper_mutex);
            break;
        }

        // picking up a task
        tuple<int, string> task = map_args.mapper_tasks->front();
        map_args.mapper_tasks->pop();

        // unlocking the queue
        pthread_mutex_unlock(map_args.mapper_mutex);

        // extracting data from task
        int id = get<0>(task);
        string file_name = get<1>(task);

        // creating word count map
        unordered_map<string, int> count;

        // opening file
        ifstream fin(file_name);
        string word;
        while(fin >> word) {
            word = normalize_word(word);

            if(count.find(word) == count.end()) {
                count[word] = 1;
            } else {
                count[word]++;
            }
        }
        fin.close();

        // only adding unique words
        vector<string> unique;
        for (auto w = count.begin(); w != count.end(); w++) {
            unique.push_back(w->first);
        }

        // adding the map to the reduce queue
        pthread_mutex_lock(map_args.reducer_mutex);
        map_args.reducer_tasks->push(make_tuple(id, unique));
        pthread_mutex_unlock(map_args.reducer_mutex);
    }

    // waiting for all mappers to end, so reducers can start
    pthread_barrier_wait(map_args.mappers_done);

    // waiting for everyone to be done
    pthread_barrier_wait(map_args.everyone_done);
   
    pthread_exit(NULL);

}



void printReducerTasks(std::queue<std::tuple<int, std::vector<std::string>>>& reducer_tasks) {
    // Make a copy of the queue to avoid modifying the original queue
    std::queue<std::tuple<int, std::vector<std::string>>> tasks_copy = reducer_tasks;

    // Iterate through the queue
    while (!tasks_copy.empty()) {
        // Get the front tuple
        auto task = tasks_copy.front();
        tasks_copy.pop();

        // Get the integer and vector of strings
        int task_id = std::get<0>(task);
        std::vector<std::string> task_data = std::get<1>(task);

        // Print the integer followed by ":"
        std::cout << task_id << ": \n";

        // Print each string in the vector, indented with a tab
        for (const auto& str : task_data) {
            std::cout << "\t" << str << "\n";
        }
    }
}

void printSortedList(const std::unordered_map<char, std::vector<std::tuple<std::string, std::set<int>>>>& sorted_list) {
    // Iterate through the outer unordered_map
    for (const auto& outer_it : sorted_list) {
        char letter = outer_it.first; // Access the letter
        const auto& vec = outer_it.second; // Access the vector of tuples

        // Print the letter followed by ":"
        std::cout << letter << ":\n";

        // Iterate through the vector of tuples
        for (const auto& tuple : vec) {
            const std::string& word = std::get<0>(tuple);   // Access the word from the tuple
            const auto& numbers = std::get<1>(tuple);       // Access the set of numbers from the tuple

            // Print the word indented with one tab
            std::cout << "\t" << word << ":\n";

            // Iterate through the set of numbers
            for (const auto& num : numbers) {
                std::cout << "\t\t" << num << "\n"; // Access and print each number
            }
        }
    }
}


int main(int argc, char **argv)
{
    if(argc != 4) {
        cerr << "Incorrect argument format: ./tema1 <numar_mapperi> <numar_reduceri> <fisier_intrare>" << endl;
        return 1;
    }

    // parsing arguments
    int M = atoi(argv[1]);
    int R = atoi(argv[2]);
    char* input_file = argv[3];

    // task list for mappers
    queue<tuple<int, string>> mapper_tasks;
    pthread_mutex_t mapper_mutex;
    pthread_mutex_init(&mapper_mutex, NULL);

    // adding tasks to the list
    int i, n;
    string file;
    ifstream in(input_file);

    in >> n;
    in.ignore(); // ignoring the endline
    for (int i = 1; i <= n; i++) {
        getline(in, file);
        mapper_tasks.push(make_tuple(i, file));
    }

    in.close();

    // task list for reducer
    queue<tuple<int, vector<string>>> reducer_tasks;
    pthread_mutex_t reducer_mutex;
    pthread_mutex_init(&reducer_mutex, NULL);

    // barrier after mappers finish work
    pthread_barrier_t mappers_done;
    pthread_barrier_init(&mappers_done, nullptr, M + R);

    // barrier after everyone finishes work
    pthread_barrier_t everyone_done;
    pthread_barrier_init(&everyone_done, nullptr, M + R);

    // map of all letters and their unique words
    unordered_map<char, unordered_map<string, set<int>>> agregated_list;
    pthread_mutex_t agregated_mutex['z' + 1];
    for (char l = 'a'; l <= 'z'; l++)
        pthread_mutex_init(&agregated_mutex[(int)l], NULL);

    // creating a map for each letter
    for (char l = 'a'; l <= 'z'; l++) {
        agregated_list[l];
    }

    // sort queue
    queue<char> sort_tasks;
    pthread_mutex_t sort_mutex;
    pthread_mutex_init(&sort_mutex, NULL);

    // barrier after everyone finishes aggregating
    pthread_barrier_t agg_done;
    pthread_barrier_init(&agg_done, nullptr, R);

    // write queue
    queue<char> write_tasks;
    pthread_mutex_t write_mutex;
    pthread_mutex_init(&write_mutex, NULL);

    // adding tasks to sort and write
    for(char l = 'a'; l <= 'z'; l++) {
        sort_tasks.push(l);
        write_tasks.push(l);
    }

    // sorted list
    unordered_map<char, vector<tuple<string, set<int>>>> sorted_list;
    for (char l = 'a'; l <= 'z'; l++) {
        sorted_list[l];
    }

    // barrier after everyone finishes sorting
    pthread_barrier_t sort_done;
    pthread_barrier_init(&sort_done, nullptr, R);

    // defining threads
    pthread_t threads[M + R];

    // creating mapper threads
    int r;
    mapper_args map_args[M];
    for (i = 0; i < M; i++) {
        map_args[i].id = i;
		map_args[i].mapper_mutex = &mapper_mutex;
        map_args[i].mapper_tasks = &mapper_tasks;
        map_args[i].reducer_mutex = &reducer_mutex;
        map_args[i].reducer_tasks = &reducer_tasks;
        map_args[i].mappers_done = &mappers_done;
        map_args[i].everyone_done = &everyone_done;


		r = pthread_create(&threads[i], NULL, mapper, &map_args[i]);

		if (r) {
			printf("Eroare la crearea thread-ului %d\n", i);
			exit(-1);
		}
	}

    // creating reducer threads
    reducer_args red_args[R];
    for(i = 0; i < R; i++) {
        red_args[i].agregated_list = &agregated_list;
        red_args[i].agregated_mutex = (pthread_mutex_t**)&agregated_mutex;
        red_args[i].reducer_tasks = &reducer_tasks;
        red_args[i].reducer_mutex = &reducer_mutex;
        red_args[i].everyone_done = &everyone_done;
        red_args[i].mappers_done = &mappers_done;
        red_args[i].sort_tasks = &sort_tasks;
        red_args[i].sort_mutex = &sort_mutex;
        red_args[i].write_tasks = &write_tasks;
        red_args[i].write_mutex = &write_mutex;
        red_args[i].sorted_list = &sorted_list;
        red_args[i].agg_done = &agg_done;
        red_args[i].sort_done = &sort_done;

        r = pthread_create(&threads[M + i], NULL, reducer, &red_args[i]);

        if (r) {
			printf("Eroare la crearea thread-ului %d\n", i);
			exit(-1);
		}
    }

    // joining the mapper threads
    for (i = 0; i < M; i++) {
        void* status;
		r = pthread_join(threads[i], &status);

		if (r) {
			printf("Eroare la asteptarea thread-ului %d\n", i);
			exit(-1);
		}
	}

    // joining the reducers threads
    for (i = 0; i < R; i++) {
        void* status;
		r = pthread_join(threads[M + i], &status);

		if (r) {
			printf("Eroare la asteptarea thread-ului %d\n", i);
			exit(-1);
		}
	}

    // destroying mutexes and barriers
//     struct mapper_args {
//     int id;
//     queue<tuple<int, string>> *mapper_tasks;
//     pthread_mutex_t *mapper_mutex;
//     queue<tuple<int, vector<string>>> *reducer_tasks;
//     pthread_mutex_t *reducer_mutex;
//     pthread_barrier_t *mappers_done;
//     pthread_barrier_t *everyone_done;

// };

// struct reducer_args {
//     queue<tuple<int, vector<string>>> *reducer_tasks;
//     pthread_mutex_t *reducer_mutex;
//     pthread_barrier_t *everyone_done;
//     pthread_barrier_t *mappers_done;
//     unordered_map<char, unordered_map<string, set<int>>> *agregated_list;
//     pthread_mutex_t **agregated_mutex;
//     queue<char> *sort_tasks;
//     pthread_mutex_t *sort_mutex;
//     queue<char> *write_tasks;
//     pthread_mutex_t *write_mutex;
//     unordered_map<char, vector<tuple<string, set<int>>>> *sorted_list;
//     pthread_barrier_t *agg_done;
//     pthread_barrier_t *sort_done;
// };

    pthread_mutex_destroy(&mapper_mutex);
    pthread_mutex_destroy(&reducer_mutex);
    pthread_mutex_destroy(&reducer_mutex);
    pthread_mutex_destroy(&sort_mutex);
    pthread_mutex_destroy(&write_mutex);
    for(auto& mutex : agregated_mutex) {
        pthread_mutex_destroy(&mutex);
    }
    pthread_barrier_destroy(&mappers_done);
    pthread_barrier_destroy(&everyone_done);
    pthread_barrier_destroy(&agg_done);
    pthread_barrier_destroy(&sort_done);

    // printReducerTasks(reducer_tasks);
    // printSortedList(sorted_list);


    return 0;
}