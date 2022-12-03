# VINF zadanie
### Téma: F12 - Parsovanie title, alt a iných špecifických údajov entity Book

### Spustenie projektu

#### sparkParser.py

- parsovanie freebase súboru na záznamy o knihách, s ktorými potom ďalej pracujeme
- spustenie `spark-submit sparkParser.py -i <input_file_path> -o <output_file_path> -m <cluster_master> -s <spark_path_cluster>`

#### pyLucene.py

- indexovanie a vyhľadávanie nad dátami získanými z parseru, tento súbor je potrebné spúšťať pomocou dockeru
- vytvorenie a spustenie dockeru podľa Dockerfile
```
docker volume create pylucene ; 
docker image build -t pylucene .  ; 
docker run --rm --name pylucene -v pylucene:/usr/app/src pylucene
```
- po vytvorení sa môžeme prepnúť do kontajnera 
`docker ps`
- zistíme id kontajnera s názvom pylucene
CONTAINER ID   IMAGE                                                       COMMAND                  CREATED          STATUS          PORTS                              NAMES
34c704022f84   pylucene                                                    "python3 -m http.ser…"   20 minutes ago   Up 20 minutes                                      pylucene

následne sa vieme prepnúť do tohto kontajnera pomocou príkazu
`docker exec -it 34c704022f84 /bin/sh`

v kontajneri potom môžeme spustiť skripty ktoré používajú PyLucene, v našom prípade to sú skripty pre Unit testy a pre indexovanie/vyhĺadávanie

spustenie unit testov (je potrebný súbor part-00000)

`python3 unitTests.py`

spustenie vyhľadávania

`python3 pyLucene.py`

po spustení skriptu sa začne vytvárať index, po vytvorení indexu môžeme pomocou konzoly vyhľadávať knihy podľa zvolených stĺpcov

K dispozícii máme tieto stĺpce nad ktorými môžeme vyhľadávať: 
```
book.written_work.author
type.object.name
book.written_work.date_of_first_publication
common.topic.description
media_common.cataloged_instance.isbn13
book.book.editions
book.book_edition.publication_date
```
v prípade že do query stringu zadáme viacej slov, nájde nám všetky záznamy ktoré obsahujú ľubovoľne z týchto slov, napr. výraz 'harry potter' by našiel záznamy ktoré obsahujú v texte harry, alebo aj potter, pokiaľ chceme vyhľadávať slovné spojenie, je potrebné pridať hľadaný text do úvodzoviek '"Harry potter"'

