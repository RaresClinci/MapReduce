# Structura
Exista 2 tipuri de threaduri: ```Mapper``` si ```Reducer```, fiecare cu o structura prin care se transmit primitivele de sincronizare si modurile de distributie a muncii. Reducerii asteapta ca Mapperii sa isi termine treaba pentru a incepe.


## Mapper
Responsabili pentru citirea din fisiere si gasirea cuvintelor unice.

### Mod de functionare

Mapperi scot un task(un fisier) dintr-o coada, citesc din el si creaza un vector cu toate cuvintele unice. Este adaugat un tuplu <id_fisier, vector> in coada de taskuri pentru reduceri

### Primitive de sincronizare si distribuire a muncii
  - ```mapper_task``` : o coada cu numele fiecarui fisier si id-ul sau
  - ```mapper_mutex``` : un mutex care asigura ca doar un thread acceseaza coada ```mapper_task```
  - ```reducer_tasks``` : coada de taskuri pentru reduceri(mapperi adauga taskuri) - un task este un tuplu <id_fisier, vector<string>>
  - ```reducer_mutex``` : un mutex care asigura ca doar un thread acceseaza coada ```reducer_task```
  - ```mapers_done``` : o bariera care anunta reduceri ca pot incepe pentru ca mapperi au terminat
  - ```everyone_done``` : o bariera care anunta ca toate threadurile au terminat si ca pot se pot inchide

## Reducer

### Mod de functionare
Reduceri au doua roluri(care se pot realiza doar unul dupa altul):
- agregarea listelor mapperilor intr-o lista comuna, unde fiecare litera a alfabetului are o lista cu toate cuvintele unice(si fisierele din care fac parte) care incep cu acea litera(listele cu fisiere sunt set - se sorteaza singure la adaugare)
- sortarea si scrierea in fisiere a listelor de cuvinte 

### Primitive de sincronizare si distribuire a muncii
- ```reducer_tasks``` : coada de taskuri pentru reduceri - un task este un tuplu <id_fisier, vector<string>>
- ```reducer_mutex``` : un mutex care asigura ca doar un thread acceseaza coada ```reducer_task```
- ```everyone_done``` : o bariera care anunta ca toate threadurile au terminat si ca pot se pot inchide
- ```mappers_done``` : o bariera care anunta reduceri ca mapperi au terminat si pot incepe
- ```agregated_list``` : lista rezultata dupa prima etapa a reducerilor, un map intre litera din alfabet si vectorul de cuvinte
- ```agregated_mutex``` : o lista cu cate un mutex pentru fiecare litera(asigura ca doar un thread poate scrie in lista unei litere)
- ```sort_tasks``` : o coada cu toate literele alfabetului, reprezinta taskurile de sortare si scriere
- ```sort_mutex``` : mutex care asigura ca doar un thread extrage din ```sort_tasks```
- ```sorted_list``` : lista in care cuvintele sunt sortate
- ```agg_done``` : o bariera care separa procesul de agregare de cel de sortare si scriere
