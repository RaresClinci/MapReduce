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

using namespace std;

struct mapper_args {
    int id;
    queue<tuple<int, string>> *mapper_tasks;
    pthread_mutex_t *mapper_mutex;
    queue<tuple<int, unordered_map<string, int>>> *reducer_tasks;
    pthread_mutex_t *reducer_mutex;
    sem_t *new_task;
    pthread_barrier_t *barrier;

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

        // adding the map to the reduce queue
        pthread_mutex_lock(map_args.reducer_mutex);
        map_args.reducer_tasks->push(make_tuple(id, count));
        sem_post(map_args.new_task);
        pthread_mutex_unlock(map_args.reducer_mutex);
    }

    // waiting on a barrier so the mappers can signal the reducers that they are over
    pthread_barrier_wait(map_args.barrier);

    // one of the threads(the first) will signal the reducers that they can exit the function if there are no more tasks
    if(map_args.id == 0) {
        sem_post(map_args.new_task);
    }
   
    pthread_exit(NULL);

}


void printReducerTasks(const std::queue<std::tuple<int, std::unordered_map<std::string, int>>>& reducer_tasks) {
    // Create a copy of the queue to process (because queues cannot be accessed directly in a loop)
    std::queue<std::tuple<int, std::unordered_map<std::string, int>>> tasks = reducer_tasks;

    // Iterate over each task in the queue
    while (!tasks.empty()) {
        // Get the current tuple
        auto task = tasks.front();
        tasks.pop(); // Remove the task from the queue
        
        // Extract the int and the unordered_map from the tuple
        int number = std::get<0>(task);
        const std::unordered_map<std::string, int>& map = std::get<1>(task);
        
        // Print the int followed by a colon
        std::cout << number << ": " << std::endl;

        // Iterate through the unordered_map and print each key-value pair
        for (const auto& pair : map) {
            std::cout << "\t" << pair.first << ": " << pair.second << std::endl;
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
    queue<tuple<int, unordered_map<string, int>>> reducer_tasks;
    pthread_mutex_t reducer_mutex;
    pthread_mutex_init(&reducer_mutex, NULL);

    // barrier after mappers finish work
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, M);

    // semaphore so mappers signal new tasks for reducers
    sem_t new_task;
    sem_init(&new_task, 0, 0);

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
        map_args[i].barrier = &barrier;
        map_args[i].new_task = &new_task;


		r = pthread_create(&threads[i], NULL, mapper, &map_args[i]);

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

    printReducerTasks(reducer_tasks);

    int value;
    sem_getvalue(&new_task, &value);
    cout << value << endl;


    return 0;
}