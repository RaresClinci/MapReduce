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
    unordered_map<char, map<string, vector<int>>> *agregated_list;
    pthread_mutex_t *agregated_mutex;
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
            if(w->second == 1) {
                unique.push_back(w->first);
            }
        }

        // adding the map to the reduce queue
        pthread_mutex_lock(map_args.reducer_mutex);
        map_args.reducer_tasks->push(make_tuple(id, unique));
        pthread_mutex_unlock(map_args.reducer_mutex);
    }

    // waiting for all mappers to end, so reducers can start
    // pthread_barrier_wait(map_args.mappers_done);

    // waiting for everyone to be done
    // pthread_barrier_wait(map_args.everyone_done);
   
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
    pthread_barrier_init(&mappers_done, nullptr, M);

    // barrier after everyone finishes work
    pthread_barrier_t everyone_done;
    pthread_barrier_init(&everyone_done, nullptr, M + R);

    // map of all letters and their unique words
    unordered_map<char, map<string, vector<int>>> agregated_list;
    pthread_mutex_t agregated_mutex;
    pthread_mutex_init(&agregated_mutex, NULL);

    // creating a map for each letter
    for (char l = 'a'; l <= 'z'; l++) {
        agregated_list[l];
    }

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
        red_args[i].agregated_mutex = &agregated_mutex;
        red_args[i].reducer_tasks = &reducer_tasks;
        red_args[i].reducer_mutex = &reducer_mutex;
        red_args[i].everyone_done = &everyone_done;
        red_args[i].mappers_done = &mappers_done;

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

    printReducerTasks(reducer_tasks);


    return 0;
}