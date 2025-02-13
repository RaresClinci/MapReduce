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
    unordered_map<char, pthread_mutex_t> *agregated_mutex;
    queue<char> *sort_tasks;
    pthread_mutex_t *sort_mutex;
    unordered_map<char, vector<tuple<string, set<int>>>> *sorted_list;
    pthread_barrier_t *agg_done;
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
                pthread_mutex_lock(&(*red_args.agregated_mutex)[first_letter]);

                red_args.agregated_list->at(first_letter)[w].insert(id);

                
                pthread_mutex_unlock(&(*red_args.agregated_mutex)[first_letter]);
            }
        }
    }

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

        // writing to file
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
            count[word] = 1;
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
    unordered_map<char, pthread_mutex_t> agregated_mutex;
    for (char l = 'a'; l <= 'z'; l++) {
        agregated_mutex[l];
        pthread_mutex_init(&agregated_mutex[l], NULL);
    }

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

    // adding tasks to sort and write
    for(char l = 'a'; l <= 'z'; l++) {
        sort_tasks.push(l);
    }

    // sorted list
    unordered_map<char, vector<tuple<string, set<int>>>> sorted_list;
    for (char l = 'a'; l <= 'z'; l++) {
        sorted_list[l];
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
        red_args[i].sort_tasks = &sort_tasks;
        red_args[i].sort_mutex = &sort_mutex;
        red_args[i].sorted_list = &sorted_list;
        red_args[i].agg_done = &agg_done;

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

    pthread_mutex_destroy(&mapper_mutex);
    pthread_mutex_destroy(&reducer_mutex);
    pthread_mutex_destroy(&reducer_mutex);
    pthread_mutex_destroy(&sort_mutex);
    for(char l = 'a'; l <= 'z'; l++) {
        pthread_mutex_destroy(&agregated_mutex[l]);
    }
    pthread_barrier_destroy(&mappers_done);
    pthread_barrier_destroy(&everyone_done);
    pthread_barrier_destroy(&agg_done);

    return 0;
}